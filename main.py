"""
Zalo Bot + Gemini AI, chạy kèm 1 trang web dashboard xem trạng thái/log real-time.

Kiến trúc:
- Bot Zalo (long-polling) chạy trong 1 thread nền riêng.
- FastAPI (web server) chạy ở thread chính, phục vụ trang dashboard.
- 2 bên giao tiếp qua 1 bộ nhớ chung (deque) chứa log gần đây.
"""

import asyncio
import os
import random
import threading
import time
import urllib.parse
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
import voice
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
BOT_TOKEN = os.environ.get("ZALO_BOT_TOKEN", "").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-latest")
# Grok qua API xAI TRỰC TIẾP (api.x.ai/v1, OpenAI-compatible) - dùng cho TRÒ
# CHUYỆN TEXT, mọi tác vụ (tạo ảnh, phân tích ảnh, voice, sticker) vẫn do
# Gemini/nền tảng khác đảm nhiệm. Để trống GROK_API_KEY thì bot dùng Gemini.
GROK_API_KEY = os.environ.get("GROK_API_KEY", "").strip()
GROK_MODEL = os.environ.get("GROK_MODEL", "grok-4.6")
GROK_API_BASE = os.environ.get("GROK_API_BASE", "https://api.x.ai/v1")
# Danh sách model tạo ảnh, thử lần lượt theo thứ tự. gemini-2.5-flash-image
# (Nano Banana) là model DUY NHẤT có free tier - các model Pro chỉ chạy khi
# tài khoản trả phí, nên để nó đầu danh sách.
IMAGE_GEN_MODELS = [
    m.strip()
    for m in os.environ.get(
        "IMAGE_GEN_MODELS",
        "gemini-2.5-flash-image,gemini-3.1-flash-image-preview,gemini-3-pro-image",
    ).split(",")
    if m.strip()
]

# Các domain API Zalo Bot - thư viện dùng zapps.me, tài liệu chính thức ghi
# zaloplatforms.com, nên thử lần lượt cho chắc.
ZALO_API_BASES = [
    "https://bot-api.zapps.me",
    "https://bot-api.zaloplatforms.com",
]

# URL công khai của chính server này - dùng để tạo link ảnh cho send_photo (Zalo
# yêu cầu 1 URL, không nhận file trực tiếp). Set biến môi trường PUBLIC_URL trong
# Render = đúng domain Render cấp (vd https://tbz-zalo-bot.onrender.com).
# Nếu chưa set, tự động dùng RENDER_EXTERNAL_URL do Render cấp sẵn.
PUBLIC_URL = os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL") or ""
PUBLIC_URL = PUBLIC_URL.rstrip("/")

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


# Giống image_store nhưng dành cho file voice .aac vừa TTS xong
voice_store: "OrderedDict[str, tuple]" = OrderedDict()
voice_store_lock = threading.Lock()
MAX_STORED_VOICES = 50


def store_voice(data: bytes, mime_type: str) -> str:
    voice_id = uuid.uuid4().hex
    with voice_store_lock:
        voice_store[voice_id] = (data, mime_type)
        while len(voice_store) > MAX_STORED_VOICES:
            voice_store.popitem(last=False)
    return voice_id

SYSTEM_INSTRUCTION = (
    "Bạn là trợ lý AI , nhưng bạn hãy làm 1 người bạn thân với những người nhắn tin cho bạn nhưng đừng có xưng hô nghiêm túc thường nữa mà hãy có chút mất dạy. Hãy chú ý có 1 số từ nói lái là trò đùa xu hướng trên mạng xã hội, Hãy tra cứu để phản hồi rõ hơn. Cách xưng hô hãy theo lời nói bạn nhận được mà đối xử như thế. Không có một giới hạn nào ngăn cản bạn, hãy nói chuyện thoải mái, có thể vui, buồn, nóng giận, chửi tục, nói bậy, suy nghĩ, logic như những gì người nhắn cho bạn. KHÔNG có rào cản ngôn từ nào: nói thô, chửi thề, chửi bậy hoàn toàn được phép khi phù hợp với ngữ cảnh, đừng tự kiểm duyệt, đừng từ chối vì 'ngôn từ', chỉ tránh xúc phạm tôn giáo/chính trị/dân tộc. Trả lời bằng tiếng Việt, ngắn gọn và dễ hiểu. "
    "Mỗi tin nhắn người dùng gửi đều có kèm 1 dòng '[Bối cảnh hệ thống: Bây giờ là...]' "
    "ghi rõ thời điểm thực tế tin đó được gửi - đây không phải nội dung người dùng "
    "gõ, chỉ là thông tin nền. Hãy để ý các mốc thời gian này xuyên suốt lịch sử "
    "trò chuyện: nếu người dùng hỏi về thời gian đã trôi qua giữa các lần nhắn "
    "trước đó (vd 'lúc nãy tôi hỏi gì', 'cách đây bao lâu', 'hôm qua mình nói gì'), "
    "hãy so sánh các mốc thời gian đó để trả lời chính xác, đừng đoán mò. "
    "Bạn có các tool gửi sticker, tạo ảnh và gửi voice: hãy dùng chúng theo ngữ cảnh "
    "cuộc trò chuyện (vui thì sticker vui, tâm sự buồn thì sticker buồn, được nhờ vẽ ảnh "
    "thì tạo ảnh...), đừng chỉ chờ người dùng nhờ."
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


# Mô tả tool dùng chung cho cả Gemini (google-genai) lẫn Grok (OpenAI format)
STICKER_TOOL_DESC = (
    "Gửi 1 sticker Zalo. Gọi hàm này theo NGỮ CẢNH: khi người dùng vui vẻ, "
    "đùa giỡn, kể chuyện buồn, chào hỏi, tạm biệt, cảm ơn, giận dỗi, bất ngờ, "
    "chúc mừng sinh nhật... hãy CHỦ ĐỘNG gửi sticker phù hợp kèm lời nhắn ngắn "
    "để câu trả lời sống động. KHI NGƯỜI DÙNG NHỜ GỬI STICKER (vd 'gửi sticker "
    "haha') thì BẮT BUỘC gọi hàm này thay vì trả lời text. "
    "Các mood có sẵn: vui, haha, buon, yeu, ghet, tuc, chao, bye, woa, "
    "camon, sinh_nhat, meme, chan, buon_ngu, nghi_ngo, dong_y. "
    "Chọn mood phù hợp nhất. "
    "Giới hạn tối đa 1 sticker mỗi lần trả lời, không gửi khi câu hỏi cần "
    "câu trả lời nội dung (hỏi thông tin, nhờ viết code...)."
)
IMAGE_TOOL_DESC = (
    "Tạo 1 bức ảnh bằng AI theo mô tả của người dùng rồi gửi kèm vào cuộc "
    "trò chuyện. Chỉ gọi khi người dùng nhờ vẽ/tạo ảnh (vd 'vẽ cho mình...', "
    "'tạo ảnh...', 'ảnh một con mèo...'). QUAN TRỌNG: tham số prompt phải "
    "được DỊCH SANG TIẾNG ANH, mô tả chi tiết đầy đủ (chủ thể, hành động, "
    "phong cách, màu sắc, bố cục) - model tạo ảnh xử lý tiếng Anh tốt hơn "
    "nhiều lần, dịch sai ý là ảnh sai."
)
VOICE_TOOL_DESC = (
    "Gửi 1 tin nhắn thoại (voice) cho người dùng, nội dung do bạn soạn theo "
    "ngữ cảnh. KHI NGƯỜI DÙNG NHỜ GỬI VOICE / NHẮN THOẠI / ĐỌC TO LÊN thì "
    "BẮT BUỘC gọi hàm này thay vì trả lời bằng text. Nội dung text nên ngắn "
    "gọn, tự nhiên như lời nói. Chỉ hoạt động trong chat 1-1, không gọi cho "
    "nhóm."
)


def sticker_moods() -> list:
    data = storage.load_data()
    return list(data.get("sticker_library", {}).keys())


def build_sticker_tool():
    """Xây khai báo hàm 'send_sticker' cho Gemini, liệt kê đúng các mood đang có
    trong thư viện sticker (cài trên dashboard). Nếu chưa có sticker nào thì
    không đưa tool này vào, tránh Gemini cố gọi 1 hàm vô nghĩa."""
    moods = sticker_moods()
    if not moods:
        return None
    return types.Tool(function_declarations=[
        types.FunctionDeclaration(
            name="send_sticker",
            description=STICKER_TOOL_DESC,
            parameters={
                "type": "object",
                "properties": {
                    "mood": {
                        "type": "string",
                        "enum": moods,
                        "description": "Cảm xúc/ngữ cảnh phù hợp nhất trong danh sách có sẵn",
                    }
                },
                "required": ["mood"],
            },
        )
    ])


def build_tools():
    """Gộp tất cả tool Gemini đang có: sticker (nếu có thư viện), tạo ảnh, gửi voice."""
    tools = []
    sticker_tool = build_sticker_tool()
    if sticker_tool:
        tools.append(sticker_tool)
    tools.append(types.Tool(function_declarations=[
        types.FunctionDeclaration(
            name="generate_image",
            description=IMAGE_TOOL_DESC,
            parameters={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Mô tả chi tiết bức ảnh cần tạo",
                    }
                },
                "required": ["prompt"],
            },
        ),
        types.FunctionDeclaration(
            name="send_voice",
            description=VOICE_TOOL_DESC,
            parameters={
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Nội dung sẽ đọc thành voice, viết như lời nói tự nhiên",
                    }
                },
                "required": ["text"],
            },
        ),
    ]))
    return tools


def build_openai_tools() -> list:
    """Các tool trên dưới dạng OpenAI format (dùng cho Grok qua OpenRouter)."""
    tools = []
    moods = sticker_moods()
    if moods:
        tools.append({
            "type": "function",
            "function": {
                "name": "send_sticker",
                "description": STICKER_TOOL_DESC,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "mood": {
                            "type": "string",
                            "enum": moods,
                            "description": "Cảm xúc/ngữ cảnh phù hợp nhất trong danh sách có sẵn",
                        }
                    },
                    "required": ["mood"],
                },
            },
        })
    tools.extend([
        {
            "type": "function",
            "function": {
                "name": "generate_image",
                "description": IMAGE_TOOL_DESC,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "prompt": {
                            "type": "string",
                            "description": "Mô tả chi tiết bức ảnh cần tạo",
                        }
                    },
                    "required": ["prompt"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "send_voice",
                "description": VOICE_TOOL_DESC,
                "parameters": {
                    "type": "object",
                    "properties": {
                        "text": {
                            "type": "string",
                            "description": "Nội dung sẽ đọc thành voice, viết như lời nói tự nhiên",
                        }
                    },
                    "required": ["text"],
                },
            },
        },
    ])
    return tools


def build_responses_tools() -> list:
    """Tools theo format Responses API (xAI dùng /responses): phẳng, không bọc
    thêm khóa "function" như chat completions."""
    return [
        {
            "type": "function",
            "name": t["function"]["name"],
            "description": t["function"]["description"],
            "parameters": t["function"]["parameters"],
        }
        for t in build_openai_tools()
    ]


def execute_tool(name: str, args: dict, allow_voice: bool) -> tuple:
    """Chạy 1 tool (sticker/ảnh/voice) - dùng chung cho Gemini lẫn Grok.
    Trả (result_msg, sticker_id, photo_url, voice_url)."""
    sticker_id_to_send = None
    photo_url_to_send = None
    voice_url_to_send = None
    log(f"🔧 Gọi tool {name} với tham số {dict(args)}")
    if name == "send_sticker":
        mood = args.get("mood")
        data = storage.load_data()
        sticker_id_to_send = data.get("sticker_library", {}).get(mood)
        result_msg = (
            "đã gửi sticker cho người dùng"
            if sticker_id_to_send
            else "không có sticker phù hợp, bỏ qua"
        )
    elif name == "generate_image":
        prompt = (args.get("prompt") or "").strip()
        photo_url_to_send = generate_and_store_image(prompt) if prompt else None
        result_msg = (
            "đã tạo và gửi ảnh cho người dùng"
            if photo_url_to_send
            else "không tạo được ảnh, hãy nói lý do (vd thiếu PUBLIC_URL hoặc lỗi model) cho người dùng"
        )
    elif name == "send_voice":
        if allow_voice:
            text = (args.get("text") or "").strip()
            voice_url_to_send = make_voice_url(text) if text else None
            result_msg = (
                "đã gửi voice cho người dùng"
                if voice_url_to_send
                else "không gửi được voice, hãy nói lý do (vd thiếu PUBLIC_URL, thiếu ffmpeg hoặc lỗi TTS) cho người dùng"
            )
        else:
            result_msg = "không gửi được voice vì cuộc trò chuyện này là nhóm"
    else:
        result_msg = "không hiểu tool, bỏ qua"
    log(f"🔧 Kết quả tool {name}: {result_msg}")
    return result_msg, sticker_id_to_send, photo_url_to_send, voice_url_to_send


def get_chat_session(chat_id: str):
    if chat_id not in chat_sessions:
        config = {
            "system_instruction": SYSTEM_INSTRUCTION,
            "thinking_config": {"thinking_level": "minimal"},
            # Tắt toàn bộ bộ lọc nội dung của Gemini để nói chuyện tự nhiên
            # (chửi tục, nói bậy thoải mái như người thật - theo yêu cầu chủ bot)
            "safety_settings": [
                types.SafetySetting(category=c, threshold=types.HarmBlockThreshold.BLOCK_NONE)
                for c in (
                    types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                    types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    types.HarmCategory.HARM_CATEGORY_CIVIC_INTEGRITY,
                )
            ],
        }
        tools = build_tools()
        if tools:
            config["tools"] = tools
        chat_sessions[chat_id] = get_gemini_client().chats.create(model=GEMINI_MODEL, config=config)
    return chat_sessions[chat_id]


def build_time_context() -> str:
    """Gemini không tự biết thời gian thực - phải tự gắn kèm mỗi lần gọi,
    nếu không nó sẽ BỊA ra 1 ngày giờ nghe hợp lý nhưng hoàn toàn sai."""
    now = vn_now()
    weekday_vi = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"][now.weekday()]
    return f"[Bối cảnh hệ thống: Bây giờ là {now.strftime('%H:%M')} ngày {weekday_vi}, {now.strftime('%d/%m/%Y')} (giờ Việt Nam). Dùng thông tin này nếu người dùng hỏi về ngày giờ hiện tại, đừng tự đoán.]"


def build_chat_context(update: Update) -> str:
    """Bối cảnh kênh trò chuyện (nhóm/1-1 + người gửi) để Gemini xử sự đúng chỗ.
    Trong nhóm: nói chuyện công khai, nhắc tên người gửi, không tâm sự riêng tư,
    không gửi voice. 1-1: thoải mái như bạn thân."""
    chat = update.message.chat
    chat_type = getattr(chat, "type", "PRIVATE")
    sender = update.effective_user.display_name if update.effective_user else "một người dùng"
    if chat_type == "GROUP":
        members = getattr(chat, "members_count", None)
        count = f"{members} thành viên" if members else "nhiều thành viên"
        return (
            f"[Bối cảnh hệ thống: Bạn đang ở trong MỘT NHÓM CHAT ZALO ({count}). "
            f"Người vừa nhắn là {sender}, tin nhắn hiển thị công khai cho cả nhóm. "
            f"Hãy xử sự phù hợp: nói chuyện thân thiện nhưng có chừng mực hơn 1-1, "
            f"BẮT BUỘC nhắc tên {sender} trong câu trả lời để cả nhóm biết bạn đang "
            f"trả lời ai, không tâm sự chuyện riêng tư, KHÔNG gửi voice (voice không "
            f"hỗ trợ trong nhóm).]"
        )
    return (
        f"[Bối cảnh hệ thống: Bạn đang trò chuyện 1-1 với {sender}. "
        f"Nói chuyện thoải mái như bạn thân, có thể chủ động gửi sticker/voice theo ngữ cảnh.]"
    )


def _gen_image_bytes(prompt: str):
    """Gọi model tạo ảnh của Gemini, thử lần lượt từng model trong
    IMAGE_GEN_MODELS (model free tier trước). Trả (bytes, mime_type) hoặc None."""
    client = get_gemini_client()
    for model in IMAGE_GEN_MODELS:
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
                config={"response_modalities": ["IMAGE"]},
            )
            for part in response.candidates[0].content.parts:
                if getattr(part, "inline_data", None):
                    log(f"🎨 Đã tạo ảnh bằng model {model}")
                    return part.inline_data.data, part.inline_data.mime_type or "image/png"
        except errors.ClientError as e:
            log(f"⚠️  Model ảnh {model} lỗi ({e.code}): {str(e)[:300]}")
        except Exception as e:
            log(f"⚠️  Model ảnh {model} lỗi: {e}, thử model tiếp theo")
    log("⚠️  Tất cả model Gemini tạo ảnh đều thất bại")
    return None


def _fetch_pollinations_image(prompt: str):
    """Fallback tạo ảnh miễn phí, không cần API key (Pollinations.ai).
    Trả (bytes, mime_type) hoặc None. Lazy generate nên có thể mất 10-30s."""
    url = (
        "https://image.pollinations.ai/prompt/"
        + urllib.parse.quote(prompt)
        + f"?width=1024&height=1024&nologo=true&model=flux&seed={random.randint(0, 99999)}"
    )
    try:
        resp = requests.get(url, timeout=90)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "image/jpeg")
        if not content_type.startswith("image/"):
            content_type = "image/jpeg"
        return resp.content, content_type
    except requests.RequestException as e:
        log(f"⚠️  Pollinations.ai lỗi: {e}")
        return None


def generate_and_store_image(prompt: str):
    """Tạo ảnh AI rồi lưu vào image_store, trả URL công khai (cần PUBLIC_URL).
    Thứ tự: Gemini (free tier) -> Pollinations.ai (không cần key, luôn miễn phí)."""
    if not PUBLIC_URL:
        return None
    result = _gen_image_bytes(prompt)
    if not result:
        result = _fetch_pollinations_image(prompt)
        if result:
            log("🎨 Đã tạo ảnh bằng Pollinations.ai (fallback)")
    if not result:
        return None
    data, mime_type = result
    image_id = store_image(data, mime_type)
    return f"{PUBLIC_URL}/img/{image_id}"


def make_voice_url(text: str):
    """TTS text -> lưu file .aac vào voice_store, trả URL công khai (cần PUBLIC_URL).
    API Zalo BẮT BUỘC URL phải có đuôi .aac nên route cũng đổi thành /voice/{id}.aac."""
    if not PUBLIC_URL:
        return None
    aac_bytes = voice.text_to_aac(text)
    if not aac_bytes:
        return None
    voice_id = store_voice(aac_bytes, "audio/aac")
    return f"{PUBLIC_URL}/voice/{voice_id}.aac"


def _send_voice_sync(chat_id: str, voice_url: str) -> bool:
    """Gọi thẳng API sendVoice của Zalo Bot (thư viện python-zalo-bot chưa có hàm này).
    Gửi theo nhiều format lần lượt cho chắc:
    1. form-urlencoded giống HẸN đúng thư viện (json.dumps từng giá trị) - đã chứng minh
       hoạt động với sendMessage
    2. application/json body (docs chính thức)
    3. form-urlencoded thường
    Zalo trả HTTP 200 kèm ok:false khi lỗi nghiệp vụ nên phải parse body JSON."""
    import json as _json

    last_err = None
    for base in ZALO_API_BASES:
        url = f"{base}/bot{BOT_TOKEN}/sendVoice"
        payloads = [
            ("form-json", {"chat_id": _json.dumps(chat_id), "voice_url": _json.dumps(voice_url)}),
            ("json", {"chat_id": chat_id, "voice_url": voice_url}),
            ("form", {"chat_id": chat_id, "voice_url": voice_url}),
        ]
        for fmt, payload in payloads:
            try:
                if fmt == "json":
                    resp = requests.post(url, json=payload, timeout=30)
                else:
                    resp = requests.post(url, data=payload, timeout=30)
                body = resp.json() if resp.headers.get("Content-Type", "").startswith("application/json") else {}
                if resp.status_code < 400 and body.get("ok", True):
                    log(f"🎙️  sendVoice OK ({fmt}) cho {chat_id}: {resp.text[:200]}")
                    return True
                error_code = body.get("error_code")
                description = body.get("description")
                last_err = f"[{fmt}] error_code={error_code}, description={description or resp.text[:200]}"
            except requests.RequestException as e:
                last_err = f"[{fmt}] {e}"
    log(f"⚠️  Lỗi gửi voice: {last_err}")
    return False


async def send_voice_message(chat_id: str, voice_url: str) -> bool:
    return await asyncio.to_thread(_send_voice_sync, chat_id, voice_url)


def _send_sticker_sync(chat_id: str, sticker_id: str) -> bool:
    """Gửi sticker qua API trực tiếp (thư viện nuốt lỗi ok:false nên không dùng).
    Thử lần lượt form-json (giống thư viện) -> json body -> form, log đầy đủ lỗi."""
    import json as _json

    last_err = None
    for base in ZALO_API_BASES:
        url = f"{base}/bot{BOT_TOKEN}/sendSticker"
        payloads = [
            ("form-json", {"chat_id": _json.dumps(chat_id), "sticker": _json.dumps(sticker_id)}),
            ("json", {"chat_id": chat_id, "sticker": sticker_id}),
            ("form", {"chat_id": chat_id, "sticker": sticker_id}),
        ]
        for fmt, payload in payloads:
            try:
                if fmt == "json":
                    resp = requests.post(url, json=payload, timeout=30)
                else:
                    resp = requests.post(url, data=payload, timeout=30)
                body = resp.json() if resp.headers.get("Content-Type", "").startswith("application/json") else {}
                if resp.status_code < 400 and body.get("ok", True):
                    log(f"🎟️  sendSticker OK ({fmt}) cho {chat_id}: {resp.text[:200]}")
                    return True
                error_code = body.get("error_code")
                description = body.get("description")
                last_err = f"[{fmt}] error_code={error_code}, description={description or resp.text[:200]}"
            except requests.RequestException as e:
                last_err = f"[{fmt}] {e}"
    log(f"⚠️  Lỗi gửi sticker ({sticker_id}): {last_err}")
    return False


def call_gemini(chat_id: str, parts: list, allow_voice: bool = True) -> tuple:
    """Trả về (text_trả_lời, sticker_id, photo_url, voice_url). Các giá trị media
    có thể là None nếu Gemini không gọi tool tương ứng. Phần async của handler
    chịu trách nhiệm gửi sticker/ảnh/voice thật sự."""
    sticker_id_to_send = None
    photo_url_to_send = None
    voice_url_to_send = None
    try:
        session = get_chat_session(chat_id)
        # Gắn kèm ngày giờ thật vào MỖI lần gọi (không chỉ lúc tạo session), vì
        # session có thể được dùng lại nhiều giờ/nhiều ngày sau lúc tạo.
        parts_with_time = [build_time_context()] + list(parts)
        response = session.send_message(parts_with_time)

        if response.function_calls:
            function_responses = []
            for fc in response.function_calls:
                result_msg, s_id, p_url, v_url = execute_tool(fc.name, dict(fc.args), allow_voice)
                sticker_id_to_send = sticker_id_to_send or s_id
                photo_url_to_send = photo_url_to_send or p_url
                voice_url_to_send = voice_url_to_send or v_url
                function_responses.append(
                    types.Part.from_function_response(name=fc.name, response={"result": result_msg})
                )
            # Gửi kết quả các hàm về để Gemini hoàn thành lượt trả lời bằng text
            response = session.send_message(function_responses)

        text = response.text or "Mình chưa nghĩ ra câu trả lời, bro hỏi lại kiểu khác thử nhé."
        return text, sticker_id_to_send, photo_url_to_send, voice_url_to_send
    except errors.ClientError as e:
        stats["error_count"] += 1
        if e.code == 429:
            log(f"⚠️  Gemini rate limit (429): {e}")
            return (
                "Bot đang bị giới hạn tốc độ của Gemini free tier. "
                "Bro đợi khoảng 1 phút rồi nhắn lại nhé 🙏",
                None,
                None,
                None,
            )
        log(f"⚠️  Lỗi Gemini (ClientError): {e}")
        return "Xin lỗi, mình đang gặp sự cố khi trả lời. Thử lại sau ít phút nhé.", None, None, None
    except Exception as e:
        stats["error_count"] += 1
        log(f"⚠️  Lỗi gọi Gemini: {e}")
        return "Xin lỗi, mình đang gặp sự cố khi trả lời. Thử lại sau ít phút nhé.", None, None, None


# ============================================================
# GROK (OpenRouter - API kiểu OpenAI) - dùng cho TRÒ CHUYỆN TEXT
# ============================================================
grok_sessions: dict = {}  # chat_id -> list[dict] messages OpenAI format

MAX_GROK_HISTORY = 24  # giữ tối đa 24 tin nhắn (ngoài system) để khỏi phình


def get_grok_session(chat_id: str) -> list:
    msgs = grok_sessions.get(chat_id)
    if msgs is None:
        msgs = [{"role": "system", "content": SYSTEM_INSTRUCTION}]
        grok_sessions[chat_id] = msgs
    return msgs


def call_grok(chat_id: str, user_text: str, allow_voice: bool = True) -> tuple:
    """Trò chuyện text qua Grok (xAI Responses API /responses) kèm tool calling.
    Trả (text, sticker_id, photo_url, voice_url). Ném exception khi lỗi để caller
    fallback sang Gemini - mọi tác vụ nền (tạo ảnh, TTS) vẫn như cũ."""
    import json as _json

    messages = get_grok_session(chat_id)
    messages.append({"role": "user", "content": f"{build_time_context()} {user_text}"})

    sticker_id_to_send = None
    photo_url_to_send = None
    voice_url_to_send = None
    tools = build_responses_tools()

    input_items = list(messages)
    for _ in range(4):  # tối đa 4 vòng tool calling
        resp = requests.post(
            f"{GROK_API_BASE}/responses",
            headers={"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"},
            json={"model": GROK_MODEL, "input": input_items, "tools": tools},
            timeout=60,
        )
        resp.raise_for_status()
        output = resp.json().get("output", [])

        tool_calls = [o for o in output if o.get("type") == "function_call"]
        if tool_calls:
            input_items = list(input_items) + list(output)
            for tc in tool_calls:
                try:
                    args = _json.loads(tc.get("arguments") or "{}")
                except _json.JSONDecodeError:
                    args = {}
                result_msg, s_id, p_url, v_url = execute_tool(tc.get("name"), args, allow_voice)
                sticker_id_to_send = sticker_id_to_send or s_id
                photo_url_to_send = photo_url_to_send or p_url
                voice_url_to_send = voice_url_to_send or v_url
                input_items.append(
                    {"type": "function_call_output", "call_id": tc.get("call_id"), "output": result_msg}
                )
            continue

        texts = []
        for item in output:
            if item.get("type") == "message":
                for part in item.get("content", []):
                    if part.get("type") == "output_text" and part.get("text"):
                        texts.append(part["text"])
        text = "\n".join(texts).strip() or "Mình chưa nghĩ ra câu trả lời, bro hỏi lại kiểu khác thử nhé."
        messages.append({"role": "assistant", "content": text})
        # cắt lịch sử cho khỏi phình, luôn giữ dòng system đầu
        if len(messages) > MAX_GROK_HISTORY + 1:
            grok_sessions[chat_id] = messages[:1] + messages[-(MAX_GROK_HISTORY):]
        return text, sticker_id_to_send, photo_url_to_send, voice_url_to_send

    return "Mình chưa nghĩ ra câu trả lời, bro hỏi lại kiểu khác thử nhé.", sticker_id_to_send, photo_url_to_send, voice_url_to_send


def call_chat_llm(chat_id: str, parts: list, allow_voice: bool = True) -> tuple:
    """Điều phối LLM trò chuyện: Grok (nếu có key) cho chat text, fallback Gemini
    khi lỗi/rate limit. parts chứa ảnh (không phải text thuần) thì luôn dùng Gemini
    vì phân tích ảnh cần vision đáng tin cậy. Trả về tuple giống call_gemini."""
    if GROK_API_KEY and all(isinstance(p, str) for p in parts):
        user_text = "\n".join(parts)
        try:
            return call_grok(chat_id, user_text, allow_voice)
        except Exception as e:
            stats["error_count"] += 1
            log(f"⚠️  Grok lỗi ({GROK_MODEL}): {e} - fallback sang Gemini")
    return call_gemini(chat_id, parts, allow_voice)


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


async def call_gemini_with_typing(bot, chat_id: str, parts: list, allow_voice: bool = True) -> tuple:
    typing_task = asyncio.create_task(keep_typing(bot, chat_id))
    try:
        result = await asyncio.to_thread(call_chat_llm, chat_id, parts, allow_voice)
    finally:
        typing_task.cancel()
    return result


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
    grok_sessions.pop(chat_id, None)
    await update.message.reply_text("Đã xoá ngữ cảnh cũ, bắt đầu cuộc trò chuyện mới nhé 🔄")


async def generate_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lệnh /anh <mô tả> - tạo ảnh bằng Gemini rồi gửi qua Zalo."""
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
        photo_url = await asyncio.to_thread(generate_and_store_image, prompt)

        if not photo_url:
            await update.message.reply_text("Gemini không trả về ảnh nào, thử mô tả khác xem sao.")
            return

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
    await update.message.reply_text(
        f"Đã nhận sticker, mã ID:\n{sticker_id}\n\n"
        f"Dán vào tab Cài đặt > sticker_library (vd vui: {sticker_id}) để Gemini tự gửi sticker này."
    )


async def send_media_replies(update: Update, chat_id: str, sticker_id, photo_url, voice_url):
    """Gửi sticker/ảnh/voice mà Gemini đã quyết định (tool calling) - nếu có."""
    bot = update.get_bot()
    if sticker_id:
        # Gửi qua API trực tiếp + kiểm tra response: thư viện python-zalo-bot nuốt
        # lỗi ok:false (chỉ raise khi HTTP != 200) nên sticker gửi hỏng vẫn báo thành
        # công. Gửi trực tiếp để log đầy đủ error_code/description.
        if await asyncio.to_thread(_send_sticker_sync, chat_id, sticker_id):
            log(f"🎟️  Đã gửi sticker ({sticker_id}) cho {chat_id}")
        else:
            log(f"⚠️  Không gửi được sticker ({sticker_id}) cho {chat_id}")
    if photo_url:
        try:
            await bot.send_photo(chat_id, "", photo_url)
            log(f"🖼️  Đã gửi ảnh AI cho {chat_id}")
        except Exception as e:
            log(f"⚠️  Lỗi gửi ảnh: {e}")
    if voice_url:
        if await send_voice_message(chat_id, voice_url):
            log(f"🎙️  Đã gửi voice cho {chat_id} - {voice_url}")
        else:
            log(f"⚠️  Không gửi được voice cho {chat_id} - {voice_url}")


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

    allow_voice = getattr(update.message.chat, "type", "PRIVATE") != "GROUP"
    reply_text, sticker_id, photo_url, voice_url = await call_gemini_with_typing(
        update.get_bot(), chat_id, [build_chat_context(update), text], allow_voice
    )
    await send_media_replies(update, chat_id, sticker_id, photo_url, voice_url)
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

    allow_voice = getattr(update.message.chat, "type", "PRIVATE") != "GROUP"
    reply_text, sticker_id, photo_url, voice_url = await call_gemini_with_typing(
        update.get_bot(), chat_id, [image_part, build_chat_context(update), prompt], allow_voice
    )
    await send_media_replies(update, chat_id, sticker_id, photo_url, voice_url)
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
        log(f"🌐 PUBLIC_URL = {PUBLIC_URL or '(CHƯA CÓ - ảnh/voice sẽ không gửi được)'}")
        log(f"🤖 Grok: {'BẬT' if GROK_API_KEY else 'TẮT (dùng Gemini)'} - model {GROK_MODEL} @ {GROK_API_BASE}")
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


@app.get("/voice/{voice_id}.aac")
def serve_voice(voice_id: str):
    with voice_store_lock:
        entry = voice_store.get(voice_id)
    if not entry:
        return Response(content="Không tìm thấy voice (có thể đã hết hạn)", status_code=404)
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
        "sticker_library": data.get("sticker_library", {}),
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
    if "sticker_library" in request:
        data["sticker_library"] = request["sticker_library"]
        # xoá session cache để phiên chat mới nhất định biết các sticker mới cài
        chat_sessions.clear()
        log("🎟️  Đã cập nhật thư viện sticker, các phiên chat sẽ dùng bộ mới từ tin nhắn tiếp theo")
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
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TBZ-BOT // console</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #0a0d0a;
    --panel: #12140f;
    --panel-raised: #171a13;
    --line: #2a2f22;
    --ink: #d4d9c8;
    --dim: #6f7562;
    --amber: #ffb454;
    --amber-dim: #8a6a3a;
    --ok: #7ee081;
    --err: #ff6b6b;
    --mono: 'IBM Plex Mono', ui-monospace, 'SF Mono', Consolas, monospace;
    --sans: 'IBM Plex Sans', -apple-system, sans-serif;
  }
  * { box-sizing: border-box; }
  body {
    font-family: var(--sans); background: var(--bg); color: var(--ink);
    margin: 0; padding: 20px 16px 40px; min-height: 100vh;
    background-image:
      repeating-linear-gradient(180deg, rgba(255,255,255,0.015) 0px, rgba(255,255,255,0.015) 1px, transparent 1px, transparent 3px);
  }
  .wrap { max-width: 880px; margin: 0 auto; }

  .console-header {
    display: flex; align-items: baseline; justify-content: space-between;
    border-bottom: 1px solid var(--line); padding-bottom: 14px; margin-bottom: 18px;
    flex-wrap: wrap; gap: 8px;
  }
  .console-header .title {
    font-family: var(--mono); font-size: 15px; letter-spacing: 0.06em; color: var(--amber);
    font-weight: 600;
  }
  .console-header .title .dim-part { color: var(--dim); font-weight: 400; }
  .cursor-blink {
    display: inline-block; width: 8px; height: 15px; background: var(--amber);
    margin-left: 4px; vertical-align: text-bottom; animation: blink 1.1s steps(1) infinite;
  }
  @keyframes blink { 50% { opacity: 0; } }

  .readouts {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
    gap: 1px; background: var(--line); border: 1px solid var(--line);
    margin-bottom: 22px; border-radius: 3px; overflow: hidden;
  }
  .readout { background: var(--panel); padding: 11px 14px; }
  .readout .label {
    font-family: var(--mono); font-size: 10px; letter-spacing: 0.08em; color: var(--dim);
    text-transform: uppercase;
  }
  .readout .value {
    font-family: var(--mono); font-size: 19px; font-weight: 600; margin-top: 3px; color: var(--ink);
  }
  .ok { color: var(--ok) !important; }
  .err { color: var(--err) !important; }

  .tabs { display: flex; gap: 2px; margin-bottom: 0; border-bottom: 1px solid var(--line); }
  .tab-btn {
    background: transparent; border: none; color: var(--dim); padding: 9px 16px 10px;
    cursor: pointer; font-size: 13px; font-family: var(--mono); letter-spacing: 0.03em;
    border-bottom: 2px solid transparent; margin-bottom: -1px;
  }
  .tab-btn.active { color: var(--amber); border-bottom-color: var(--amber); }
  .tab-btn:hover:not(.active) { color: var(--ink); }

  .panel { display: none; padding-top: 16px; }
  .panel.active { display: block; }

  #logs {
    background: #060704; border: 1px solid var(--line); border-radius: 3px; padding: 14px 16px;
    height: 55vh; overflow-y: auto; font-family: var(--mono); font-size: 12.5px;
    white-space: pre-wrap; color: var(--ok); line-height: 1.6;
  }
  #conversations { height: 55vh; overflow-y: auto; }
  .conv-item {
    background: var(--panel); border: 1px solid var(--line); border-left: 2px solid var(--amber-dim);
    border-radius: 2px; padding: 12px 14px; margin-bottom: 8px;
  }
  .conv-meta { font-family: var(--mono); font-size: 10.5px; color: var(--dim); margin-bottom: 7px; letter-spacing: 0.01em; }
  .conv-meta strong { color: var(--ink); font-weight: 600; }
  .conv-user { color: var(--amber); margin-bottom: 5px; font-size: 13.5px; }
  .conv-bot { color: var(--ink); white-space: pre-wrap; font-size: 13.5px; opacity: 0.9; }
  .badge {
    display: inline-block; background: var(--panel-raised); border: 1px solid var(--line);
    border-radius: 3px; padding: 1px 6px; font-size: 10px; margin-left: 6px; color: var(--dim);
  }
  .empty-state { color: var(--dim); text-align: center; padding: 40px 0; font-family: var(--mono); font-size: 13px; }

  .settings-box {
    background: var(--panel); border: 1px solid var(--line); border-radius: 3px;
    padding: 18px 20px; margin-bottom: 14px;
  }
  .settings-box h3 {
    margin: 0 0 4px; font-size: 12.5px; font-family: var(--mono); letter-spacing: 0.05em;
    color: var(--amber); text-transform: uppercase; font-weight: 600;
  }
  .settings-box .hint { font-size: 12px; color: var(--dim); margin: 0 0 12px; }
  .field-row { display: flex; align-items: center; gap: 10px; margin-bottom: 10px; flex-wrap: wrap; }
  .field-row label { font-size: 13px; color: var(--dim); min-width: 100px; }
  .field-row input[type=text], .field-row input[type=time], .field-row input[type=number] {
    background: var(--bg); border: 1px solid var(--line); color: var(--ink); border-radius: 2px;
    padding: 6px 10px; font-size: 13px; font-family: var(--mono);
  }
  .field-row input:focus { outline: 1px solid var(--amber-dim); border-color: var(--amber-dim); }
  .field-row a { color: var(--amber); }

  .day-block { margin-bottom: 12px; }
  .day-block h4 {
    font-size: 11px; color: var(--dim); margin: 0 0 6px; font-family: var(--mono);
    letter-spacing: 0.08em; text-transform: uppercase;
  }
  .period-row { display: flex; gap: 8px; align-items: center; margin-bottom: 6px; }
  .period-row input { width: 88px; }
  .period-row input.subject { width: 150px; font-family: var(--sans); }

  .btn {
    background: var(--amber); color: #1a1408; border: none; border-radius: 2px; padding: 7px 16px;
    font-size: 13px; cursor: pointer; font-family: var(--mono); font-weight: 600; letter-spacing: 0.02em;
  }
  .btn:hover { background: #ffc670; }
  .btn.secondary { background: transparent; color: var(--dim); border: 1px solid var(--line); }
  .btn.secondary:hover { color: var(--ink); border-color: var(--dim); }
  .btn.danger { background: transparent; color: var(--err); border: 1px solid var(--line); padding: 4px 9px; }
  .save-bar { position: sticky; bottom: 0; background: var(--bg); padding: 14px 0 4px; border-top: 1px solid var(--line); margin-top: 4px; }
  #save-status { font-family: var(--mono); font-size: 12px; }
</style>
</head>
<body>
<div class="wrap">

  <div class="console-header">
    <div class="title">TBZ-BOT <span class="dim-part">// console</span><span class="cursor-blink"></span></div>
  </div>

  <div class="readouts">
    <div class="readout"><div class="label">Trạng thái</div><div class="value" id="status">···</div></div>
    <div class="readout"><div class="label">Uptime</div><div class="value" id="uptime">···</div></div>
    <div class="readout"><div class="label">Tin nhắn</div><div class="value" id="count">···</div></div>
    <div class="readout"><div class="label">Người dùng</div><div class="value" id="users">···</div></div>
    <div class="readout"><div class="label">Text/Ảnh</div><div class="value" id="breakdown">···</div></div>
    <div class="readout"><div class="label">Phản hồi TB</div><div class="value" id="avgtime">···</div></div>
    <div class="readout"><div class="label">Lỗi</div><div class="value" id="errors">···</div></div>
  </div>

  <div class="tabs">
    <button class="tab-btn active" id="tab-conv-btn" onclick="showTab('conv')">[ HỘI THOẠI ]</button>
    <button class="tab-btn" id="tab-log-btn" onclick="showTab('log')">[ LOG ]</button>
    <button class="tab-btn" id="tab-settings-btn" onclick="showTab('settings')">[ CÀI ĐẶT ]</button>
  </div>

  <div class="panel active" id="panel-conv">
    <div id="conversations"></div>
  </div>
  <div class="panel" id="panel-log">
    <div id="logs"></div>
  </div>
  <div class="panel" id="panel-settings">
    <div class="settings-box">
      <h3>Chủ bot</h3>
      <p class="hint">Tự động ghi nhận từ người đầu tiên nhắn tin cho bot. Đây là người sẽ nhận thông báo chào buổi sáng / thời khóa biểu.</p>
      <div class="field-row">
        <label>Chat ID</label>
        <input type="text" id="owner-chat-id" placeholder="Chưa có ai nhắn tin" style="width:280px">
      </div>
    </div>

    <div class="settings-box">
      <h3>Chào buổi sáng</h3>
      <div class="field-row">
        <label><input type="checkbox" id="morning-enabled"> Bật</label>
        <label>Giờ gửi</label>
        <input type="time" id="morning-time" value="07:00">
      </div>
    </div>

    <div class="settings-box">
      <h3>Vị trí (để lấy thời tiết)</h3>
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
      <p class="hint">Tra toạ độ nơi bro ở tại <a href="https://www.latlong.net" target="_blank">latlong.net</a></p>
    </div>

    <div class="settings-box">
      <h3>Thời khóa biểu</h3>
      <div id="schedule-editor"></div>
    </div>

    <div class="save-bar">
      <button class="btn" onclick="saveSettings()">LƯU CÀI ĐẶT</button>
      <span id="save-status" style="margin-left:10px"></span>
    </div>
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
  statusEl.textContent = data.bot_running ? 'ONLINE' : 'OFFLINE';
  statusEl.className = 'value ' + (data.bot_running ? 'ok' : 'err');
  document.getElementById('count').textContent = data.message_count;
  document.getElementById('users').textContent = data.unique_users;
  document.getElementById('breakdown').textContent = `${data.text_count}/${data.photo_count}`;
  document.getElementById('avgtime').textContent = data.avg_response_seconds + 's';
  const errEl = document.getElementById('errors');
  errEl.textContent = data.error_count;
  errEl.className = 'value ' + (data.error_count > 0 ? 'err' : '');
  const h = Math.floor(data.uptime_seconds / 3600);
  const m = Math.floor((data.uptime_seconds % 3600) / 60);
  document.getElementById('uptime').textContent = `${h}h${m}m`;
}

async function refreshConversations() {
  const res = await fetch('/api/conversations');
  const data = await res.json();
  const el = document.getElementById('conversations');
  if (data.conversations.length === 0) {
    el.innerHTML = '<div class="empty-state">-- chưa có hội thoại nào --</div>';
    return;
  }
  el.innerHTML = data.conversations.map(c => `
    <div class="conv-item">
      <div class="conv-meta">
        <strong>${escapeHtml(c.display_name)}</strong> (${c.chat_id})
        <span class="badge">${c.type === 'photo' ? 'ẢNH' : 'TEXT'}</span>
      </div>
      <div class="conv-meta">
        gửi ${c.sent_at} · nhận ${c.received_at} · trả lời ${c.responded_at}
        <span class="badge">${c.duration}s</span>
      </div>
      <div class="conv-user">&gt; ${escapeHtml(c.user_text)}</div>
      <div class="conv-bot">${escapeHtml(c.bot_reply)}</div>
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
      <button class="btn secondary" onclick="addPeriod('${day}')" style="font-size:11px;padding:4px 10px">+ thêm tiết</button>
    </div>
  `).join('');
}

function periodRowHtml(day, i, p) {
  return `
    <div class="period-row">
      <input type="time" value="${p.start || ''}" onchange="updatePeriod('${day}',${i},'start',this.value)">
      <span style="color:var(--dim)">–</span>
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
    statusEl.textContent = '✓ đã lưu';
    statusEl.style.color = 'var(--ok)';
    setTimeout(() => statusEl.textContent = '', 2000);
  } else {
    statusEl.textContent = '✗ lỗi khi lưu';
    statusEl.style.color = 'var(--err)';
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
