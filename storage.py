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
    # sticker_library: { "vui": "451a23c11f84f6daaf95", "buon": "...", ... } - mood key
    # bằng tiếng Việt không dấu, để Gemini chọn qua function calling.
    # "vui" và "buon" là mã ĐÃ XÁC MINH (bro gửi sticker thật, bot nhận được),
    # các mood còn lại lấy từ pack miễn phí https://stickers.zaloapp.com/sticker
    # (nếu mood nào gửi lỗi, gửi sticker thật cho bot để lấy mã thay thế).
    "sticker_library": {
        "vui": "3eb5aad796927fcc2683",   # [XÁC MINH] sticker cười của bro
        "buon": "744605053940d01e8951",  # [XÁC MINH] sticker khóc của bro
        "haha": "bcad5b6a672f8e71d73e",  # Cà Khịa
        "yeu": "bf596d9a51dfb881e1ce",   # Yêu Quá Đi
        "ghet": "c6c516062a43c31d9a52",  # Giận Rồi Nha
        "tuc": "140c51c86d8d84d3dd9c",   # Thỏ Cáu Kỉnh
        "chao": "e0279194add1448f1dc0",  # [XÁC MINH] sticker chào của bro
        "bye": "8dc278014444ad1af455",   # Hi, Bye & Good Night
        "woa": "5776d836e4730d2d5462",   # [XÁC MINH] sticker bất ngờ của bro
        "nghi_ngo": "009cc1d1fd9414ca4d85",  # [XÁC MINH] sticker nghi ngờ của bro
        "dong_y": "2d8f5ecc62898bd7d298",    # [XÁC MINH] sticker đồng ý của bro
        "camon": "f6bbdb76e7330e6d5722", # Bên Nhau
        "sinh_nhat": "905570964cd3a58dfcc2",  # Happy Birthday 2
        "meme": "021e76da4a9fa3c1fa8e",  # Meme Boss Cat
        "chan": "862c43ee7fab96f5cfba",  # Nhạt
        "buon_ngu": "27d33d110154e80ab145",  # Mr. Lờ Đờ
    },
    # đánh dấu đã gửi thông báo nào hôm nay rồi, để không gửi lặp lại (reset mỗi ngày mới)
    "_last_sent_date": None,
    "_sent_today": [],  # danh sách các "key" thông báo đã gửi trong ngày hôm nay
}


def sticker_code(entry) -> str:
    """Trích mã sticker từ 1 entry — chấp nhận cả 2 định dạng đang tồn tại:
    - chuỗi cũ: "3eb5aad796927fcc2683"
    - dict mới: {"sticker_id": "...", "verified_code": "..."}
    Ưu tiên verified_code (mã đã xác minh gửi được) rồi mới đến sticker_id."""
    if isinstance(entry, dict):
        return str(entry.get("verified_code") or entry.get("sticker_id") or "")
    return str(entry or "")


def normalize_sticker_library(library) -> dict:
    """Chuẩn hoá sticker_library về định dạng dict cho web mới:
    {"mood": {"sticker_id": "...", "verified_code": "..."}}.
    Entry chuỗi cũ sẽ được nhân đôi mã vào cả 2 field."""
    out = {}
    for mood, entry in (library or {}).items():
        code = sticker_code(entry)
        if isinstance(entry, dict):
            sid = str(entry.get("sticker_id") or code)
        else:
            sid = code
        out[mood] = {"sticker_id": sid, "verified_code": code}
    return out


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
            # để bot có sticker dùng ngay từ đầu. Đồng thời loại bỏ mã cũ đã
            # seed trước đó (không xác minh được trên site chính thức).
            fake_codes = {"d063f44dc80821567819", "bfe458bf64fa8da4d4eb"}
            library = merged.get("sticker_library") or {}
            library = {k: v for k, v in library.items() if sticker_code(v) not in fake_codes}
            if not library:
                library = json.loads(json.dumps(DEFAULT_DATA["sticker_library"]))
            merged["sticker_library"] = library
            return merged
        except (json.JSONDecodeError, OSError):
            return json.loads(json.dumps(DEFAULT_DATA))


def save_data(data: dict):
    with _lock:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
