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
    # Chỉ giữ mã CÒN GỬI ĐƯỢC (đã probe thật qua Zalo API ngày 22/08/2026).
    # 10 mã pack cũ (haha/yeu/ghet/tuc/bye/camon/sinh_nhat/meme/chan/buon_ngu)
    # bị Zalo từ chối 425 "The sticker is invalid" - các pack đó đã bị gỡ khỏi
    # store của Zalo nên mã chết hẳn, không thể sửa bằng cách nào khác.
    # Muốn thêm mood mới: gửi sticker thật cho bot -> nó log ra mã -> dán mã vào
    # tab Stickers trên dashboard.
    "sticker_library": {
        "vui": "3eb5aad796927fcc2683",       # [CÒN SỐNG] sticker cười của bro
        "buon": "744605053940d01e8951",      # [CÒN SỐNG] sticker khóc của bro
        "chao": "e0279194add1448f1dc0",      # [CÒN SỐNG] sticker chào của bro
        "woa": "5776d836e4730d2d5462",       # [CÒN SỐNG] sticker bất ngờ của bro
        "nghi_ngo": "009cc1d1fd9414ca4d85",  # [CÒN SỐNG] sticker nghi ngờ của bro
        "dong_y": "2d8f5ecc62898bd7d298",    # [CÒN SỐNG] sticker đồng ý của bro
    },
    # đánh dấu đã gửi thông báo nào hôm nay rồi, để không gửi lặp lại (reset mỗi ngày mới)
    "_last_sent_date": None,
    "_sent_today": [],  # danh sách các "key" thông báo đã gửi trong ngày hôm nay
    # Sổ địa chỉ các chat từng tương tác với bot, phân loại ACCOUNT riêng vs NHÓM:
    # {"chat_id": {type, name, message_count, first_seen, last_seen, member_names}}
    # dashboard dùng sổ này để chọn đích gửi tin nhắn/voice/sticker test.
    "chats": {},
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
