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
from collections import deque
from datetime import datetime

import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from google import genai
from google.genai import errors, types

from zalo_bot import Update
from zalo_bot.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

# ============================================================
# CẤU HÌNH — lấy từ biến môi trường (set trong Render dashboard)
# ============================================================
BOT_TOKEN = os.environ.get("ZALO_BOT_TOKEN", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite")

SYSTEM_INSTRUCTION = (
    "Bạn là trợ lý AI thân thiện, trả lời bằng tiếng Việt, ngắn gọn và dễ hiểu."
)

# ============================================================
# TRẠNG THÁI DÙNG CHUNG (đọc/ghi từ cả 2 thread) — dùng deque + lock cho an toàn
# ============================================================
log_lines: deque[str] = deque(maxlen=300)
log_lock = threading.Lock()

conversations: deque[dict] = deque(maxlen=100)
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
response_times: deque[float] = deque(maxlen=50)
stats_lock = threading.Lock()


def log(message: str):
    """Ghi 1 dòng log - vừa in ra console (Render Logs) vừa lưu để hiện lên dashboard."""
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    print(line, flush=True)
    with log_lock:
        log_lines.append(line)


def record_conversation(
    chat_id: str,
    display_name: str,
    msg_type: str,
    user_text: str,
    bot_reply: str,
    sent_at: "datetime | None",
    received_at: datetime,
    responded_at: datetime,
):
    """Lưu 1 cặp hỏi-đáp đầy đủ mốc thời gian để hiện lên tab Hội thoại trên dashboard."""
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
# GEMINI (khởi tạo trễ - chỉ tạo khi thực sự có đủ API key, tránh crash lúc import
# nếu biến môi trường chưa được set, vd lúc Render mới build xong)
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


chat_sessions: dict[str, "genai.chats.Chat"] = {}


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


def call_gemini(chat_id: str, parts: list) -> str:
    try:
        session = get_chat_session(chat_id)
        response = session.send_message(parts)
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
    """Gửi hiệu ứng 'đang gõ...' lặp lại mỗi vài giây trong lúc chờ Gemini trả lời
    (hiệu ứng chỉ giữ được ~5s mỗi lần nên cần gửi lại liên tục)."""
    try:
        while True:
            await bot.send_chat_action(chat_id, "typing")
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass


async def call_gemini_with_typing(bot, chat_id: str, parts: list) -> str:
    """Chạy call_gemini() (hàm đồng bộ/blocking) trong 1 thread riêng, song song
    với việc gửi hiệu ứng 'đang gõ...' liên tục - để vòng lặp async không bị đứng."""
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


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    text = update.message.text
    display_name = update.effective_user.display_name if update.effective_user else str(chat_id)
    sent_at = update.message.date
    received_at = datetime.now()
    stats["message_count"] += 1
    stats["text_count"] += 1
    stats["last_message_at"] = time.time()
    log(f"📩 Nhận tin nhắn từ {display_name} ({chat_id}): {text!r}")

    reply_text = await call_gemini_with_typing(update.get_bot(), chat_id, [text])
    await send_long_reply(update, reply_text)
    responded_at = datetime.now()
    record_conversation(chat_id, display_name, "text", text, reply_text, sent_at, received_at, responded_at)
    log(f"✅ Đã trả lời {display_name} (mất {(responded_at - received_at).total_seconds():.1f}s)")


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat.id
    photo_url = update.message.photo_url
    caption = (update.message.text or "").strip()
    display_name = update.effective_user.display_name if update.effective_user else str(chat_id)
    sent_at = update.message.date
    received_at = datetime.now()
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
    responded_at = datetime.now()
    record_conversation(
        chat_id, display_name, "photo", caption or "[gửi 1 ảnh]", reply_text, sent_at, received_at, responded_at
    )
    log(f"✅ Đã trả lời ảnh cho {display_name} (mất {(responded_at - received_at).total_seconds():.1f}s)")


# ============================================================
# CHẠY BOT TRONG THREAD NỀN RIÊNG (tách khỏi event loop của FastAPI)
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
        app_zalo.add_handler(MessageHandler(filters.PHOTO, handle_photo))
        app_zalo.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
        app_zalo.bot.delete_webhook()

        stats["bot_running"] = True
        log("🤖 Bot đã khởi động, đang long-polling...")
        app_zalo.run_polling()  # blocking - chạy mãi trong thread này
    except Exception as e:
        stats["bot_running"] = False
        stats["bot_error"] = str(e)
        log(f"❌ Bot dừng do lỗi: {e}")


# ============================================================
# WEB DASHBOARD (FastAPI)
# ============================================================
app = FastAPI()


@app.on_event("startup")
def on_startup():
    thread = threading.Thread(target=run_bot_in_background, daemon=True)
    thread.start()


@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    """Endpoint để dịch vụ ping (UptimeRobot, v.v.) giữ cho Render free tier không ngủ.
    Hỗ trợ cả GET và HEAD vì nhiều dịch vụ uptime-monitor mặc định gửi HEAD."""
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


@app.get("/api/conversations")
def api_conversations():
    with conv_lock:
        return {"conversations": list(reversed(conversations))}


@app.get("/api/logs/stream")
async def stream_logs():
    """Server-Sent Events - đẩy log mới xuống trình duyệt theo thời gian thực."""

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
  </div>

  <div class="panel active" id="panel-conv">
    <div id="conversations"></div>
  </div>
  <div class="panel" id="panel-log">
    <div id="logs"></div>
  </div>

<script>
function showTab(name) {
  document.getElementById('panel-conv').className = 'panel' + (name === 'conv' ? ' active' : '');
  document.getElementById('panel-log').className = 'panel' + (name === 'log' ? ' active' : '');
  document.getElementById('tab-conv-btn').className = 'tab-btn' + (name === 'conv' ? ' active' : '');
  document.getElementById('tab-log-btn').className = 'tab-btn' + (name === 'log' ? ' active' : '');
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

refreshStatus();
refreshConversations();
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
