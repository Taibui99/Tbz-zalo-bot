"""
Scheduler chạy nền, kiểm tra mỗi phút để:
1. Gửi lời chào buổi sáng + thời tiết vào đúng giờ đã cài đặt
2. Báo môn học tiết tiếp theo khi 1 tiết học (trong thời khóa biểu) vừa kết thúc

Chạy như 1 asyncio task riêng trong event loop của FastAPI (không phải thread
của bot Zalo), dùng 1 Bot instance độc lập chỉ để gửi tin chủ động.
"""

import asyncio

from zalo_bot import Bot

import storage
import weather

WEEKDAY_KEYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
WEEKDAY_VI = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]


def _reset_if_new_day(data: dict, today_str: str):
    if data.get("_last_sent_date") != today_str:
        data["_last_sent_date"] = today_str
        data["_sent_today"] = []


async def _check_and_send(bot: Bot, vn_now_fn, log_fn):
    data = storage.load_data()
    now = vn_now_fn()
    today_str = now.strftime("%Y-%m-%d")
    _reset_if_new_day(data, today_str)

    owner_chat_id = data.get("owner_chat_id")
    current_hm = now.strftime("%H:%M")
    morning = data.get("morning_greeting", {})

    # Log chi tiết mỗi lần kiểm tra - để debug xem scheduler có thấy đúng cài đặt
    # đã lưu không. Nếu thấy owner_chat_id=None hoặc morning trống, nghĩa là dữ
    # liệu cài đặt đã bị mất (thường do Render tự khởi động lại server).
    log_fn(
        f"⏰ [scheduler check] giờ hiện tại={current_hm} | owner_chat_id={owner_chat_id} | "
        f"chào sáng: bật={morning.get('enabled')}, giờ cài={morning.get('time')}"
    )

    if not owner_chat_id:
        storage.save_data(data)
        return  # chưa cài đặt chat_id thì không gửi gì cả

    changed = False

    # 1) Chào buổi sáng + thời tiết
    if morning.get("enabled") and morning.get("time") == current_hm:
        key = f"morning_{today_str}"
        if key not in data["_sent_today"]:
            loc = data.get("location", {})
            summary = await asyncio.to_thread(weather.get_weather_summary, loc.get("lat"), loc.get("lon"))
            weekday_vi = WEEKDAY_VI[now.weekday()]
            text = (
                f"☀️ Chào buổi sáng! Hôm nay là {weekday_vi}, {now.strftime('%d/%m/%Y')}.\n"
                f"Thời tiết ở {loc.get('name', 'chỗ bro')} hiện tại: {summary}.\n"
                f"Chúc bro 1 ngày học tập hiệu quả! 📚"
            )
            try:
                await bot.send_message(owner_chat_id, text)
                log_fn(f"☀️ Đã gửi chào buổi sáng cho {owner_chat_id}")
            except Exception as e:
                log_fn(f"⚠️  Lỗi gửi chào buổi sáng: {e}")
            data["_sent_today"].append(key)
            changed = True

    # 2) Báo tiết học tiếp theo khi 1 tiết vừa kết thúc
    weekday_key = WEEKDAY_KEYS[now.weekday()]
    periods = data.get("schedule", {}).get(weekday_key, [])
    for i, period in enumerate(periods):
        if period.get("end") == current_hm:
            key = f"period_{today_str}_{weekday_key}_{i}"
            if key not in data["_sent_today"]:
                next_period = periods[i + 1] if i + 1 < len(periods) else None
                if next_period:
                    text = (
                        f"🔔 Hết tiết {period.get('subject', '')}!\n"
                        f"Tiết tiếp theo ({next_period.get('start')}-{next_period.get('end')}): "
                        f"**{next_period.get('subject')}**"
                    )
                else:
                    text = f"🔔 Hết tiết {period.get('subject', '')}! Đó là tiết cuối cùng hôm nay rồi 🎉"
                try:
                    await bot.send_message(owner_chat_id, text)
                    log_fn(f"🔔 Đã báo hết tiết cho {owner_chat_id}")
                except Exception as e:
                    log_fn(f"⚠️  Lỗi gửi báo hết tiết: {e}")
                data["_sent_today"].append(key)
                changed = True

    if changed:
        storage.save_data(data)


async def run_scheduler(bot_token: str, vn_now_fn, log_fn):
    """Vòng lặp chạy mãi, kiểm tra mỗi 60 giây."""
    bot = Bot(bot_token)
    log_fn("⏰ Scheduler (chào buổi sáng + thời khóa biểu) đã khởi động")
    while True:
        try:
            await _check_and_send(bot, vn_now_fn, log_fn)
        except Exception as e:
            log_fn(f"⚠️  Lỗi trong scheduler: {e}")
        await asyncio.sleep(60)
