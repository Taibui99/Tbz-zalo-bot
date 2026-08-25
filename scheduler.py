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

# Chặn hai scheduler task trong cùng process cùng chạy phần morning greeting.
# Bình thường FastAPI chỉ tạo một task, nhưng guard này bảo vệ khi startup/lifecycle
# bị kích hoạt nhiều lần trong cùng process.
_morning_lock = asyncio.Lock()
_scheduler_started = False


def _reset_if_new_day(data: dict, today_str: str):
    if data.get("_last_sent_date") != today_str:
        data["_last_sent_date"] = today_str
        data["_sent_today"] = []


def _get_morning_summary(data: dict, today_str: str, lat, lon, log_fn) -> str:
    """Lấy thời tiết tối đa một lần/ngày cho lời chào buổi sáng.

    Kết quả được lưu cùng ngày để nếu gửi Zalo lỗi sau đó, scheduler không gọi
    Open-Meteo lại ở phút kế tiếp.
    """
    cached = data.get("_morning_weather", {})
    if cached.get("date") == today_str and cached.get("summary"):
        return cached["summary"]

    log_fn(f"🌤️ [weather] gọi Open-Meteo một lần cho ngày {today_str}")
    summary = weather.get_weather_summary(lat, lon)
    data["_morning_weather"] = {
        "date": today_str,
        "summary": summary,
    }
    # Lưu ngay sau khi request hoàn tất để tránh request lặp nếu lần gửi Zalo lỗi.
    storage.save_data(data)
    return summary


async def _check_and_send(bot: Bot, vn_now_fn, log_fn):
    data = storage.load_data()
    now = vn_now_fn()
    today_str = now.strftime("%Y-%m-%d")
    _reset_if_new_day(data, today_str)

    owner_chat_id = data.get("owner_chat_id")
    current_hm = now.strftime("%H:%M")
    morning = data.get("morning_greeting", {})

    # Log nhịp tim mỗi 10 phút (tránh spam đè log quan trọng trong buffer),
    # cộng log ngay khi thấy thiếu dữ liệu để debug mất cài đặt.
    if not owner_chat_id or now.minute % 10 == 0:
        log_fn(
            f"⏰ [scheduler check] giờ hiện tại={current_hm} | owner_chat_id={owner_chat_id} | "
            f"chào sáng: bật={morning.get('enabled')}, giờ cài={morning.get('time')}"
        )

    if not owner_chat_id:
        storage.save_data(data)
        return

    changed = False

    # 1) Chào buổi sáng + thời tiết
    if morning.get("enabled") and morning.get("time") == current_hm:
        async with _morning_lock:
            # Reload sau khi lấy lock để một task khác trong cùng process không
            # dùng snapshot cũ và gọi weather lần thứ hai.
            data = storage.load_data()
            _reset_if_new_day(data, today_str)

            key = f"morning_{today_str}"
            if key not in data["_sent_today"]:
                loc = data.get("location", {})
                summary = await asyncio.to_thread(
                    _get_morning_summary,
                    data,
                    today_str,
                    loc.get("lat"),
                    loc.get("lon"),
                    log_fn,
                )
                weekday_vi = WEEKDAY_VI[now.weekday()]
                text = (
                    f"☀️ Chào buổi sáng! Hôm nay là {weekday_vi}, {now.strftime('%d/%m/%Y')}.\n"
                    f"Thời tiết ở {loc.get('name', 'chỗ bro')} hiện tại: {summary}.\n"
                    f"Chúc bro 1 ngày học tập hiệu quả! 📚"
                )
                try:
                    await bot.send_message(owner_chat_id, text)
                    log_fn(f"☀️ Đã gửi chào buổi sáng cho {owner_chat_id}")
                    data["_sent_today"].append(key)
                    storage.save_data(data)
                except Exception as e:
                    # Không gọi lại weather ở phút kế tiếp: summary đã được lưu
                    # theo ngày trong _morning_weather.
                    log_fn(f"⚠️ Lỗi gửi chào buổi sáng: {e}")

    # 2) Báo tiết học tiếp theo khi 1 tiết vừa kết thúc
    data = storage.load_data()
    _reset_if_new_day(data, today_str)
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
                    data["_sent_today"].append(key)
                    storage.save_data(data)
                except Exception as e:
                    log_fn(f"⚠️  Lỗi gửi báo hết tiết: {e}")


async def run_scheduler(bot_token: str, vn_now_fn, log_fn):
    """Vòng lặp chạy mãi, kiểm tra mỗi 60 giây."""
    global _scheduler_started
    if _scheduler_started:
        log_fn("⚠️ Scheduler đã chạy trong process này, bỏ qua lần khởi động trùng.")
        return

    _scheduler_started = True
    bot = Bot(bot_token)
    log_fn("⏰ Scheduler (chào buổi sáng + thời khóa biểu) đã khởi động")
    while True:
        try:
            await _check_and_send(bot, vn_now_fn, log_fn)
        except Exception as e:
            log_fn(f"⚠️  Lỗi trong scheduler: {e}")
        await asyncio.sleep(60)
