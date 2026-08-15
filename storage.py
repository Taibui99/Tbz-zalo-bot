"""
Lưu trữ cài đặt (thời khóa biểu, giờ chào buổi sáng, vị trí, chat_id chủ bot)
vào 1 file JSON trên đĩa. Đơn giản, không cần database riêng.

LƯU Ý: Render free tier sẽ MẤT file này mỗi khi redeploy code mới (đĩa không
persistent qua các lần deploy) - giống hệt giới hạn của lịch sử chat trong RAM.
Nếu cần lưu bền hơn, nên chuyển sang Supabase sau này.
"""

import json
import os
import threading

DATA_FILE = os.path.join(os.path.dirname(__file__), "bot_data.json")
_lock = threading.Lock()

DEFAULT_DATA = {
    "owner_chat_id": None,
    "morning_greeting": {
        "enabled": False,
        "time": "07:00",
    },
    "location": {
        "name": "Đồng Xoài, Bình Phước",
        "lat": 11.53,
        "lon": 106.90,
    },
    # schedule: { "Mon": [{"start": "07:00", "end": "07:45", "subject": "Toán"}, ...], ... }
    "schedule": {"Mon": [], "Tue": [], "Wed": [], "Thu": [], "Fri": [], "Sat": [], "Sun": []},
    # sticker_library: { "vui": "d063f44dc80821567819", "buon": "...", ... } - mood key
    # bằng tiếng Việt không dấu, để Gemini chọn qua function calling
    "sticker_library": {
        "vui": "d063f44dc80821567819",
        "haha": "bfe458bf64fa8da4d4eb",
    },
    # đánh dấu đã gửi thông báo nào hôm nay rồi, để không gửi lặp lại (reset mỗi ngày mới)
    "_last_sent_date": None,
    "_sent_today": [],  # danh sách các "key" thông báo đã gửi trong ngày hôm nay
}


def load_data() -> dict:
    with _lock:
        if not os.path.exists(DATA_FILE):
            return json.loads(json.dumps(DEFAULT_DATA))
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            # đảm bảo đủ field nếu file cũ thiếu key mới thêm sau này
            merged = json.loads(json.dumps(DEFAULT_DATA))
            merged.update(data)
            # sticker_library rỗng (file dữ liệu cũ) thì dùng sticker mặc định,
            # để bot có sticker dùng ngay từ đầu
            if not merged.get("sticker_library"):
                merged["sticker_library"] = json.loads(json.dumps(DEFAULT_DATA["sticker_library"]))
            return merged
        except (json.JSONDecodeError, OSError):
            return json.loads(json.dumps(DEFAULT_DATA))


def save_data(data: dict):
    with _lock:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
