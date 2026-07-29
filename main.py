"""
Zalo Bot + Gemini AI, chạy kèm 1 trang web dashboard xem trạng thái/log real-time.

Kiến trúc:
- Bot Zalo (long-polling) chạy trong 1 thread nền riêng.
- FastAPI (web server) chạy ở thread chính, phục vụ trang dashboard.
- 2 bên giao tiếp qua 1 bộ nhớ chung (deque) chứa log gần đây.
"""

import asyncio
import os
import threading
import time
from collections import OrderedDict, deque
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import uuid
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from google import genai
from google.genai import errors, types

import scheduler
import storage
from zalo_bot import Update
from zalo_bot.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def vn_now() -> datetime:
    """Giờ Việt Nam thật - server (Render) chạy giờ UTC nên không thể dùng
    datetime.now() suông, phải gắn rõ múi giờ, nếu không sẽ lệch 7 tiếng."""
    return datetime.now(VN_TZ)

# ============================================================
# CẤU HÌNH — lấy từ biến môi trường (set trong Render dashboard)
# ============================================================
BOT_TOKEN = os.environ.get("ZALO_BOT_TOKEN", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite")
IMAGE_GEN_MODEL = os.environ.get("IMAGE_GEN_MODEL", "gemini-2.5-flash-image")

# URL công khai của chính server này - dùng để tạo link ảnh cho send_photo (Zalo
# yêu cầu 1 URL, không nhận file trực tiếp). Set biến môi trường PUBLIC_URL trong
# Render = đúng domain Render cấp (vd https://tbz-zalo-bot.onrender.com)
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")

# Lưu ảnh AI vừa tạo trong RAM để phục vụ qua route /img/{id} - chỉ giữ tối đa
# 50 ảnh gần nhất, ảnh cũ tự bị đẩy ra (không cần dọn dẹp thủ công)
image_store: "OrderedDict[str, tuple]" = OrderedDict()
image_store_lock = threading.Lock()
MAX_STORED_IMAGES = 50


def store_image(data: bytes, mime_type: str) -> str:
    image_id = uuid.uuid4().hex
    with image_store_lock:
        image_store[image_id] = (data, mime_type)
        while len(image_store) > MAX_STORED_IMAGES:
            image_store.popitem(last=False)
    return image_id

SYSTEM_INSTRUCTION = (
    "Bạn là trợ lý AI thân thiện, trả lời bằng tiếng Việt, ngắn gọn và dễ hiểu. "
    "Mỗi tin nhắn người dùng gửi đều có kèm 1 dòng '[Bối cảnh hệ thống: Bây giờ là...]' "
    "ghi rõ thời điểm thực tế tin đó được gửi - đây không phải nội dung người dùng "
    "gõ, chỉ là thông tin nền. Hãy để ý các mốc thời gian này xuyên suốt lịch sử "
    "trò chuyện: nếu người dùng hỏi về thời gian đã trôi qua giữa các lần nhắn "
    "trước đó (vd 'lúc nãy tôi hỏi gì', 'cách đây bao lâu', 'hôm qua mình nói gì'), "
    "hãy so sánh các mốc thời gian đó để trả lời chính xác, đừng đoán mò."
)

# ============================================================
# TRẠNG THÁI DÙNG CHUNG (đọc/ghi từ cả 2 thread) — dùng deque + lock cho an toàn
# ============================================================
log_lines: deque = deque(maxlen=300)
log_lock = threading.Lock()

conversations: deque = deque(maxlen=100)
conv_lock = threading.Lock()

stats = {
    "started_at": time.time(),
    "message_count": 0,
    "text_count": 0,
    "photo_count": 0,
    "error_count": 0,
    "last_message_at": None,
    "bot_running": False,
    "bot_error": None,
}
unique_users: set = set()
response_times: deque = deque(maxlen=50)
stats_lock = threading.Lock()


def log(message: str):
    line = f"[{vn_now().strftime('%H:%M:%S')}] {message}"
    print(line, flush=True)
    with log_lock:
        log_lines.append(line)


def record_conversation(chat_id, display_name, msg_type, user_text, bot_reply, sent_at, received_at, responded_at):
    duration = (responded_at - received_at).total_seconds()
    with conv_lock:
        conversations.append({
            "display_name": display_name,
            "chat_id": chat_id,
            "type": msg_type,
            "user_text": user_text,
            "bot_reply": bot_reply,
            "sent_at": sent_at.strftime("%H:%M:%S") if sent_at else "?",
            "received_at": received_at.strftime("%H:%M:%S"),
            "responded_at": responded_at.strftime("%H:%M:%S"),
            "duration": round(duration, 1),
        })
    with stats_lock:
        unique_users.add(chat_id)
        response_times.append(duration)


# ============================================================
# GEMINI (khởi tạo trễ để tránh crash lúc import nếu thiếu biến môi trường)
# ============================================================
_gemini_client = None


def get_gemini_client():
    global _gemini_client
    if _gemini_client is None:
        _gemini_client = genai.Client(
            api_key=GEMINI_API_KEY,
            http_options=types.HttpOptions(timeout=20_000),
        )
    return _gemini_client


chat_sessions = {}


def get_chat_session(chat_id: str):
    if chat_id not in chat_sessions:
        chat_sessions[chat_id] = get_gemini_client().chats.create(
            model=GEMINI_MODEL,
            config={
                "system_instruction": SYSTEM_INSTRUCTION,
                "thinking_config": {"thinking_level": "minimal"},
            },
        )
    return chat_sessions[chat_id]


def build_time_context() -> str:
    """Gemini không tự biết thời gian thực - phải tự gắn kèm mỗi lần gọi,
    nếu không nó sẽ BỊA ra 1 ngày giờ nghe hợp lý nhưng hoàn toàn sai."""
    now = vn_now()
    weekday_vi = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"][now.weekday()]
    return f"[Bối cảnh hệ thống: Bây giờ là {now.strftime('%H:%M')} ngày {weekday_vi}, {now.strftime('%d/%m/%Y')} (giờ Việt Nam). Dùng thông tin này nếu người dùng hỏi về ngày giờ hiện tại, đừng tự đoán.]"


def call_gemini(chat_id: str, parts: list) -> str:
    try:
        session = get_chat_session(chat_id)
        # Gắn kèm ngày giờ thật vào MỖI lần gọi (không chỉ lúc tạo session), vì
        # session có thể được dùng lại nhiều giờ/nhiều ngày sau lúc tạo.
        parts_with_time = [build_time_context()] + list(parts)
        response = session.send_message(parts_with_time)
        return response.text or "Mình chưa nghĩ ra câu trả lời, bro hỏi lại kiểu khác thử nhé."
    except errors.ClientError as e:
        stats["error_count"] += 1
        if e.code == 429:
            log(f"⚠️  Gemini rate limit (429): {e}")
            return (
                "Bot đang bị giới hạn tốc độ của Gemini free tier. "
                "Bro đợi khoảng 1 phút rồi nhắn lại nhé 🙏"
            )
        log(f"⚠️  Lỗi Gemini (ClientError): {e}")
        return "Xin lỗi, mình đang gặp sự cố khi trả lời. Thử lại sau ít phút nhé."
    except Exception as e:
        stats["error_count"] += 1
        log(f"⚠️  Lỗi gọi Gemini: {e}")
        return "Xin lỗi, mình đang gặp sự cố khi trả lời. Thử lại sau ít phút nhé."


async def keep_typing(bot, chat_id: str, interval: float = 4.0):
    try:
        while True:
            await bot.send_chat_action(chat_id, "typing")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass


def ensure_owner_captured(chat_id: str):
    """Tự động ghi nhớ chat_id của người nhắn ĐẦU TIÊN làm 'chủ bot' (owner) -
    để scheduler biết gửi thông báo chào buổi sáng/thời khóa biểu cho ai.
    Có thể đổi lại qua trang Cài đặt trên dashboard nếu cần."""
    data = storage.load_data()
    if not data.get("owner_chat_id"):
        data["owner_chat_id"] = chat_id
        storage.save_data(data)
        log(f"👤 Đã tự động đặt {chat_id} làm chủ bot (owner) - dùng cho thông báo chào buổi sáng/thời khóa biểu")


async def call_gemini_with_typing(bot, chat_id: str, parts: list) -> str:
    typing_task = asyncio.create_task(keep_typing(bot, chat_id))
    try:
        reply_text = await asyncio.to_thread(call_gemini, chat_id, parts)
    finally:
        typing_task.cancel()
    return reply_text


# ============================================================
# HANDLERS ZALO BOT
# ============================================================
async def send_long_reply(update: Update, reply_text: str):
    MAX_LEN = 1900
    if len(reply_text) > MAX_LEN:
        for i in range(0, len(reply_text), MAX_LEN):
            await update.message.reply_text(reply_text[i:i + MAX_LEN])
    else:
        await update.message.reply_text(reply_text)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.effective_user.display_name if update.effective_user else "bạn"
    await update.message.reply_text(
        f"Chào {name}! Mình là bot AI, cứ nhắn gì đó (kể cả gửi ảnh) là mình trả lời nhé 🤖"
    )


async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    chat_sessions.pop(chat_id, None)
    await update.message.reply_text("Đã xoá ngữ cảnh cũ, bắt đầu cuộc trò chuyện mới nhé 🔄")


async def generate_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lệnh /anh <mô tả> - tạo ảnh bằng Gemini (Nano Banana) rồi gửi qua Zalo."""
    chat_id = update.message.chat.id
    prompt = update.message.text.replace("/anh", "", 1).strip()

    if not prompt:
        await update.message.reply_text("Dùng kiểu: /anh một chú mèo đội nón lá đang ngồi học bài")
        return

    if not PUBLIC_URL:
        await update.message.reply_text(
            "Bot chưa cấu hình PUBLIC_URL nên chưa gửi ảnh được. Cần set biến môi "
            "trường PUBLIC_URL = domain Render của bot."
        )
        return

    await context.bot.send_chat_action(chat_id, "typing")
    try:
        def _gen():
            client = get_gemini_client()
            return client.models.generate_content(
                model=IMAGE_GEN_MODEL,
                contents=prompt,
                config={"response_modalities": ["IMAGE"]},
            )

        response = await asyncio.to_thread(_gen)
        image_bytes = None
        mime_type = "image/png"
        for part in response.candidates[0].content.parts:
            if getattr(part, "inline_data", None):
                image_bytes = part.inline_data.data
                mime_type = part.inline_data.mime_type or mime_type
                break

        if not image_bytes:
            await update.message.reply_text("Gemini không trả về ảnh nào, thử mô tả khác xem sao.")
            return

        image_id = store_image(image_bytes, mime_type)
        photo_url = f"{PUBLIC_URL}/img/{image_id}"
        await context.bot.send_photo(chat_id, prompt[:200], photo_url)
        log(f"🎨 Đã tạo & gửi ảnh AI cho {chat_id}: {prompt[:50]!r}")
    except Exception as e:
        stats["error_count"] += 1
        log(f"⚠️  Lỗi tạo ảnh: {e}")
        await update.message.reply_text("Xin lỗi, mình gặp lỗi lúc tạo ảnh. Thử lại sau nhé.")


async def handle_sticker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Nhận sticker - log lại mã ID để bro thu thập, dùng gắn vào STICKER_LIBRARY."""
    chat_id = update.message.chat.id
    sticker_id = update.message.sticker
    log(f"🎟️  Nhận sticker từ {chat_id} - mã ID: {sticker_id}")
    await update.message.reply_text(f"Đã nhận sticker, mã ID của nó là:\n{sticker_id}")


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    text = update.message.text
    display_name = update.effective_user.display_name if update.effective_user else str(chat_id)
    sent_at = update.message.date
    received_at = vn_now()
    ensure_owner_captured(chat_id)
    stats["message_count"] += 1
    stats["text_count"] += 1
    stats["last_message_at"] = time.time()
    log(f"📩 Nhận tin nhắn từ {display_name} ({chat_id}): {text!r}")

    reply_text = await call_gemini_with_typing(update.get_bot(), chat_id, [text])
    await send_long_reply(update, reply_text)
    responded_at = vn_now()
    record_conversation(chat_id, display_name, "text", text, reply_text, sent_at, received_at, responded_at)
    log(f"✅ Đã trả lời {display_name} (mất {(responded_at - received_at).total_seconds():.1f}s)")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    photo_url = update.message.photo_url
    caption = (update.message.text or "").strip()
    display_name = update.effective_user.display_name if update.effective_user else str(chat_id)
    sent_at = update.message.date
    received_at = vn_now()
    ensure_owner_captured(chat_id)
    stats["message_count"] += 1
    stats["photo_count"] += 1
    stats["last_message_at"] = time.time()
    log(f"🖼️  Nhận ảnh từ {display_name} ({chat_id})")

    try:
        img_resp = requests.get(photo_url, timeout=20)
        img_resp.raise_for_status()
        image_bytes = img_resp.content
        content_type = img_resp.headers.get("Content-Type", "image/jpeg")
        if not content_type.startswith("image/"):
            content_type = "image/jpeg"
    except requests.exceptions.RequestException as e:
        stats["error_count"] += 1
        log(f"⚠️  Lỗi tải ảnh: {e}")
        await update.message.reply_text("Mình không tải được ảnh bro gửi, thử gửi lại nhé.")
        return

    image_part = types.Part.from_bytes(data=image_bytes, mime_type=content_type)
    prompt = caption if caption else "Mô tả và phân tích nội dung trong ảnh này giúp mình."

    reply_text = await call_gemini_with_typing(update.get_bot(), chat_id, [image_part, prompt])
    await send_long_reply(update, reply_text)
    responded_at = vn_now()
    record_conversation(
        chat_id, display_name, "photo", caption or "[gửi 1 ảnh]", reply_text, sent_at, received_at, responded_at
    )
    log(f"✅ Đã trả lời ảnh cho {display_name} (mất {(responded_at - received_at).total_seconds():.1f}s)")


# ============================================================
# CHẠY BOT TRONG THREAD NỀN RIÊNG
# ============================================================
def run_bot_in_background():
    if not BOT_TOKEN or not GEMINI_API_KEY:
        log("⚠️  Thiếu ZALO_BOT_TOKEN hoặc GEMINI_API_KEY trong biến môi trường - bot KHÔNG chạy.")
        stats["bot_error"] = "Thiếu biến môi trường ZALO_BOT_TOKEN / GEMINI_API_KEY"
        return

    try:
        app_zalo = ApplicationBuilder().token(BOT_TOKEN).build()
        app_zalo.add_handler(CommandHandler("start", start))
        app_zalo.add_handler(CommandHandler("reset", reset))
        app_zalo.add_handler(CommandHandler("anh", generate_image))
        app_zalo.add_handler(MessageHandler(filters.PHOTO, handle_photo))
        app_zalo.add_handler(MessageHandler(filters.STICKER, handle_sticker))
        app_zalo.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
        app_zalo.bot.delete_webhook()

        stats["bot_running"] = True
        log("🤖 Bot đã khởi động, đang long-polling...")
        app_zalo.run_polling()
    except Exception as e:
        stats["bot_running"] = False
        stats["bot_error"] = str(e)
        log(f"❌ Bot dừng do lỗi: {e}")


# ============================================================
# WEB DASHBOARD (FastAPI)
# ============================================================
app = FastAPI()


@app.on_event("startup")
async def on_startup():
    thread = threading.Thread(target=run_bot_in_background, daemon=True)
    thread.start()

    if BOT_TOKEN:
        asyncio.create_task(scheduler.run_scheduler(BOT_TOKEN, vn_now, log))
    else:
        log("⚠️  Chưa có ZALO_BOT_TOKEN - scheduler (chào buổi sáng/thời khóa biểu) không chạy.")


@app.get("/img/{image_id}")
def serve_image(image_id: str):
    with image_store_lock:
        entry = image_store.get(image_id)
    if not entry:
        return Response(content="Không tìm thấy ảnh (có thể đã hết hạn)", status_code=404)
    data, mime_type = entry
    return Response(content=data, media_type=mime_type)


@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"status": "ok"}


@app.get("/api/status")
def api_status():
    uptime_seconds = int(time.time() - stats["started_at"])
    with stats_lock:
        unique_user_count = len(unique_users)
        avg_response = round(sum(response_times) / len(response_times), 1) if response_times else 0
    return {
        "bot_running": stats["bot_running"],
        "bot_error": stats["bot_error"],
        "message_count": stats["message_count"],
        "text_count": stats["text_count"],
        "photo_count": stats["photo_count"],
        "error_count": stats["error_count"],
        "unique_users": unique_user_count,
        "avg_response_seconds": avg_response,
        "last_message_at": stats["last_message_at"],
        "uptime_seconds": uptime_seconds,
    }


@app.get("/api/settings")
def api_get_settings():
    data = storage.load_data()
    # không cần trả về các field nội bộ (_last_sent_date, _sent_today) cho frontend
    return {
        "owner_chat_id": data.get("owner_chat_id"),
        "morning_greeting": data.get("morning_greeting"),
        "location": data.get("location"),
        "schedule": data.get("schedule"),
    }


@app.put("/api/settings")
async def api_put_settings(request: dict):
    data = storage.load_data()
    if "owner_chat_id" in request:
        data["owner_chat_id"] = request["owner_chat_id"] or None
    if "morning_greeting" in request:
        data["morning_greeting"] = request["morning_greeting"]
    if "location" in request:
        data["location"] = request["location"]
    if "schedule" in request:
        data["schedule"] = request["schedule"]
    storage.save_data(data)
    log("⚙️  Đã cập nhật cài đặt (chào buổi sáng / thời khóa biểu / vị trí)")
    return {"success": True}


@app.get("/api/conversations")
def api_conversations():
    with conv_lock:
        return {"conversations": list(reversed(conversations))}


@app.get("/api/logs/stream")
async def stream_logs():
    async def event_generator():
        last_sent_index = 0
        while True:
            with log_lock:
                current = list(log_lines)
            if len(current) > last_sent_index:
                for line in current[last_sent_index:]:
                    yield f"data: {line}\n\n"
                last_sent_index = len(current)
            await asyncio.sleep(1)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """
<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<title>Zalo Bot Dashboard</title>
<style>
  body { font-family: -apple-system, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }
  h1 { font-size: 20px; }
  .cards { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 20px; }
  .card { background: #1e293b; border-radius: 12px; padding: 14px 18px; min-width: 120px; }
  .card .label { font-size: 12px; color: #94a3b8; }
  .card .value { font-size: 20px; font-weight: 700; margin-top: 4px; }
  .ok { color: #4ade80; }
  .err { color: #f87171; }
  .tabs { display: flex; gap: 8px; margin-bottom: 12px; }
  .tab-btn { background: #1e293b; border: none; color: #94a3b8; padding: 8px 16px;
             border-radius: 8px; cursor: pointer; font-size: 14px; }
  .tab-btn.active { background: #3b82f6; color: white; }
  .panel { display: none; }
  .panel.active { display: block; }
  #logs { background: #000; border-radius: 12px; padding: 16px; height: 55vh; overflow-y: auto;
          font-family: monospace; font-size: 13px; white-space: pre-wrap; }
  #conversations { height: 55vh; overflow-y: auto; }
  .conv-item { background: #1e293b; border-radius: 12px; padding: 14px 16px; margin-bottom: 10px; }
  .conv-meta { font-size: 11px; color: #64748b; margin-bottom: 6px; }
  .conv-user { color: #93c5fd; margin-bottom: 6px; }
  .conv-bot { color: #d1d5db; white-space: pre-wrap; }
  .badge { display: inline-block; background: #334155; border-radius: 6px; padding: 1px 6px;
           font-size: 10px; margin-left: 6px; }
  .settings-box { background: #1e293b; border-radius: 12px; padding: 20px; margin-bottom: 16px; }
  .settings-box h3 { margin-top: 0; font-size: 15px; }
  .field-row { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; flex-wrap: wrap; }
  .field-row label { font-size: 13px; color: #94a3b8; min-width: 110px; }
  .field-row input[type=text], .field-row input[type=time], .field-row input[type=number] {
    background: #0f172a; border: 1px solid #334155; color: #e2e8f0; border-radius: 6px;
    padding: 6px 10px; font-size: 13px;
  }
  .day-block { margin-bottom: 14px; }
  .day-block h4 { font-size: 13px; color: #93c5fd; margin: 0 0 6px 0; }
  .period-row { display: flex; gap: 8px; align-items: center; margin-bottom: 6px; }
  .period-row input { width: 90px; }
  .period-row input.subject { width: 140px; }
  .btn { background: #3b82f6; color: white; border: none; border-radius: 6px; padding: 6px 14px;
         font-size: 13px; cursor: pointer; }
  .btn.secondary { background: #334155; }
  .btn.danger { background: #ef4444; padding: 4px 8px; }
  .save-bar { position: sticky; bottom: 0; background: #0f172a; padding: 12px 0; }
</style>
</head>
<body>
  <h1>🤖 Zalo Bot Dashboard</h1>
  <div class="cards">
    <div class="card"><div class="label">Trạng thái</div><div class="value" id="status">...</div></div>
    <div class="card"><div class="label">Uptime</div><div class="value" id="uptime">...</div></div>
    <div class="card"><div class="label">Tổng tin nhắn</div><div class="value" id="count">...</div></div>
    <div class="card"><div class="label">Người dùng</div><div class="value" id="users">...</div></div>
    <div class="card"><div class="label">Text / Ảnh</div><div class="value" id="breakdown">...</div></div>
    <div class="card"><div class="label">Phản hồi TB</div><div class="value" id="avgtime">...</div></div>
    <div class="card"><div class="label">Lỗi</div><div class="value" id="errors">...</div></div>
  </div>

  <div class="tabs">
    <button class="tab-btn active" id="tab-conv-btn" onclick="showTab('conv')">💬 Hội thoại</button>
    <button class="tab-btn" id="tab-log-btn" onclick="showTab('log')">📜 Log hệ thống</button>
    <button class="tab-btn" id="tab-settings-btn" onclick="showTab('settings')">⚙️ Cài đặt</button>
  </div>

  <div class="panel active" id="panel-conv">
    <div id="conversations"></div>
  </div>
  <div class="panel" id="panel-log">
    <div id="logs"></div>
  </div>
  <div class="panel" id="panel-settings">
    <div class="settings-box">
      <h3>👤 Chủ bot</h3>
      <p style="font-size:12px;color:#64748b;margin-top:-4px">
        Tự động ghi nhận từ người đầu tiên nhắn tin cho bot. Đây là người sẽ nhận
        thông báo chào buổi sáng / thời khóa biểu.
      </p>
      <div class="field-row">
        <label>Chat ID</label>
        <input type="text" id="owner-chat-id" placeholder="Chưa có ai nhắn tin" style="width:280px">
      </div>
    </div>

    <div class="settings-box">
      <h3>☀️ Chào buổi sáng</h3>
      <div class="field-row">
        <label><input type="checkbox" id="morning-enabled"> Bật</label>
        <label>Giờ gửi</label>
        <input type="time" id="morning-time" value="07:00">
      </div>
    </div>

    <div class="settings-box">
      <h3>📍 Vị trí (để lấy thời tiết)</h3>
      <div class="field-row">
        <label>Tên nơi ở</label>
        <input type="text" id="loc-name" style="width:200px">
      </div>
      <div class="field-row">
        <label>Vĩ độ (lat)</label>
        <input type="number" step="0.01" id="loc-lat">
        <label>Kinh độ (lon)</label>
        <input type="number" step="0.01" id="loc-lon">
      </div>
      <p style="font-size:12px;color:#64748b">
        Tra toạ độ nơi bro ở tại
        <a href="https://www.latlong.net" target="_blank" style="color:#60a5fa">latlong.net</a>
      </p>
    </div>

    <div class="settings-box">
      <h3>📅 Thời khóa biểu</h3>
      <div id="schedule-editor"></div>
    </div>

    <div class="save-bar">
      <button class="btn" onclick="saveSettings()">💾 Lưu cài đặt</button>
      <span id="save-status" style="margin-left:10px;font-size:13px;color:#4ade80"></span>
    </div>
  </div>

<script>
const DAY_LABELS = {Mon:'Thứ Hai', Tue:'Thứ Ba', Wed:'Thứ Tư', Thu:'Thứ Năm', Fri:'Thứ Sáu', Sat:'Thứ Bảy', Sun:'Chủ Nhật'};
let currentSchedule = {Mon:[],Tue:[],Wed:[],Thu:[],Fri:[],Sat:[],Sun:[]};

function showTab(name) {
  document.getElementById('panel-conv').className = 'panel' + (name === 'conv' ? ' active' : '');
  document.getElementById('panel-log').className = 'panel' + (name === 'log' ? ' active' : '');
  document.getElementById('panel-settings').className = 'panel' + (name === 'settings' ? ' active' : '');
  document.getElementById('tab-conv-btn').className = 'tab-btn' + (name === 'conv' ? ' active' : '');
  document.getElementById('tab-log-btn').className = 'tab-btn' + (name === 'log' ? ' active' : '');
  document.getElementById('tab-settings-btn').className = 'tab-btn' + (name === 'settings' ? ' active' : '');
}

async function refreshStatus() {
  const res = await fetch('/api/status');
  const data = await res.json();
  const statusEl = document.getElementById('status');
  statusEl.textContent = data.bot_running ? 'Đang chạy' : 'Lỗi / chưa chạy';
  statusEl.className = 'value ' + (data.bot_running ? 'ok' : 'err');
  document.getElementById('count').textContent = data.message_count;
  document.getElementById('users').textContent = data.unique_users;
  document.getElementById('breakdown').textContent = `${data.text_count} / ${data.photo_count}`;
  document.getElementById('avgtime').textContent = data.avg_response_seconds + 's';
  const errEl = document.getElementById('errors');
  errEl.textContent = data.error_count;
  errEl.className = 'value ' + (data.error_count > 0 ? 'err' : '');
  const h = Math.floor(data.uptime_seconds / 3600);
  const m = Math.floor((data.uptime_seconds % 3600) / 60);
  document.getElementById('uptime').textContent = `${h}h ${m}m`;
}

async function refreshConversations() {
  const res = await fetch('/api/conversations');
  const data = await res.json();
  const el = document.getElementById('conversations');
  if (data.conversations.length === 0) {
    el.innerHTML = '<div style="color:#64748b">Chưa có hội thoại nào.</div>';
    return;
  }
  el.innerHTML = data.conversations.map(c => `
    <div class="conv-item">
      <div class="conv-meta">
        <strong>${escapeHtml(c.display_name)}</strong> (${c.chat_id})
        <span class="badge">${c.type === 'photo' ? '🖼️ ảnh' : '💬 text'}</span>
      </div>
      <div class="conv-meta">
        Gửi lúc ${c.sent_at} · Bot nhận lúc ${c.received_at} · Bot trả lời lúc ${c.responded_at}
        <span class="badge">${c.duration}s</span>
      </div>
      <div class="conv-user">👤 ${escapeHtml(c.user_text)}</div>
      <div class="conv-bot">🤖 ${escapeHtml(c.bot_reply)}</div>
    </div>
  `).join('');
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

async function loadSettings() {
  const res = await fetch('/api/settings');
  const data = await res.json();
  document.getElementById('owner-chat-id').value = data.owner_chat_id || '';
  document.getElementById('morning-enabled').checked = !!(data.morning_greeting && data.morning_greeting.enabled);
  document.getElementById('morning-time').value = (data.morning_greeting && data.morning_greeting.time) || '07:00';
  document.getElementById('loc-name').value = (data.location && data.location.name) || '';
  document.getElementById('loc-lat').value = (data.location && data.location.lat) || '';
  document.getElementById('loc-lon').value = (data.location && data.location.lon) || '';
  currentSchedule = data.schedule || currentSchedule;
  renderScheduleEditor();
}

function renderScheduleEditor() {
  const container = document.getElementById('schedule-editor');
  container.innerHTML = Object.keys(DAY_LABELS).map(day => `
    <div class="day-block">
      <h4>${DAY_LABELS[day]}</h4>
      <div id="day-${day}">
        ${(currentSchedule[day] || []).map((p, i) => periodRowHtml(day, i, p)).join('')}
      </div>
      <button class="btn secondary" onclick="addPeriod('${day}')" style="font-size:12px;padding:4px 10px">+ Thêm tiết</button>
    </div>
  `).join('');
}

function periodRowHtml(day, i, p) {
  return `
    <div class="period-row">
      <input type="time" value="${p.start || ''}" onchange="updatePeriod('${day}',${i},'start',this.value)">
      <span>-</span>
      <input type="time" value="${p.end || ''}" onchange="updatePeriod('${day}',${i},'end',this.value)">
      <input type="text" class="subject" placeholder="Môn học" value="${p.subject || ''}" onchange="updatePeriod('${day}',${i},'subject',this.value)">
      <button class="btn danger" onclick="removePeriod('${day}',${i})">✕</button>
    </div>
  `;
}

function addPeriod(day) {
  currentSchedule[day] = currentSchedule[day] || [];
  currentSchedule[day].push({start: '', end: '', subject: ''});
  renderScheduleEditor();
}

function removePeriod(day, i) {
  currentSchedule[day].splice(i, 1);
  renderScheduleEditor();
}

function updatePeriod(day, i, field, value) {
  currentSchedule[day][i][field] = value;
}

async function saveSettings() {
  const payload = {
    owner_chat_id: document.getElementById('owner-chat-id').value.trim(),
    morning_greeting: {
      enabled: document.getElementById('morning-enabled').checked,
      time: document.getElementById('morning-time').value,
    },
    location: {
      name: document.getElementById('loc-name').value.trim(),
      lat: parseFloat(document.getElementById('loc-lat').value),
      lon: parseFloat(document.getElementById('loc-lon').value),
    },
    schedule: currentSchedule,
  };
  const res = await fetch('/api/settings', {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  });
  const statusEl = document.getElementById('save-status');
  if (res.ok) {
    statusEl.textContent = '✓ Đã lưu!';
    setTimeout(() => statusEl.textContent = '', 2000);
  } else {
    statusEl.textContent = '✗ Lỗi khi lưu';
    statusEl.style.color = '#f87171';
  }
}

refreshStatus();
refreshConversations();
loadSettings();
setInterval(refreshStatus, 5000);
setInterval(refreshConversations, 5000);

const logsEl = document.getElementById('logs');
const evtSource = new EventSource('/api/logs/stream');
evtSource.onmessage = (e) => {
  logsEl.textContent += e.data + "\\n";
  logsEl.scrollTop = logsEl.scrollHeight;
};
</script>
</body>
</html>
"""
