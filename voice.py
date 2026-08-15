"""
Gửi tin nhắn thoại (voice) qua API Zalo Bot + TTS miễn phí.

API sendVoice của Zalo Bot chỉ nhận file định dạng .aac nên flow là:
TTS (mp3) -> ffmpeg convert sang .aac -> trả về bytes để main.py lưu tạm.

TTS dùng 2 nguồn để chắc chắn luôn có tiếng Việt:
1. edge-tts (giọng neural tự nhiên hơn, có thể lỗi ở một số vùng mạng)
2. gTTS (Google Translate TTS, hoạt động ổn định hơn) - fallback

Nếu server không có ffmpeg, text_to_aac trả None và bot tự xử lý lỗi mềm.
"""

import asyncio
import io
import os
import shutil
import subprocess
import tempfile

import edge_tts
from gtts import gTTS

DEFAULT_VOICE = os.environ.get("ZALO_VOICE", "vi-VN-HoaiMyNeural")
DEFAULT_RATE = os.environ.get("ZALO_VOICE_RATE", "+10%")

MAX_TEXT_LENGTH = 2000

_ffmpeg_path = None


def _find_ffmpeg():
    """Ưu tiên ffmpeg hệ thống (Render cài qua build command), fallback sang
    imageio-ffmpeg (bundle sẵn binary trong pip, không cần apt)."""
    global _ffmpeg_path
    if _ffmpeg_path:
        return _ffmpeg_path
    system = shutil.which("ffmpeg")
    if system:
        _ffmpeg_path = system
        return _ffmpeg_path
    try:
        import imageio_ffmpeg
        _ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
        return _ffmpeg_path
    except Exception:
        return None


async def _edge_tts_mp3(text: str, voice: str, rate: str, mp3_path: str) -> bool:
    communicate = edge_tts.Communicate(text, voice=voice, rate=rate)
    with open(mp3_path, "wb") as f:
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                f.write(chunk["data"])
    return os.path.getsize(mp3_path) > 0


def _gtts_mp3(text: str, mp3_path: str) -> bool:
    tts = gTTS(text=text, lang="vi", slow=False)
    buf = io.BytesIO()
    tts.write_to_fp(buf)
    with open(mp3_path, "wb") as f:
        f.write(buf.getvalue())
    return os.path.getsize(mp3_path) > 0


def _tts_to_mp3(text: str, voice: str, rate: str, mp3_path: str) -> bool:
    try:
        asyncio.run(_edge_tts_mp3(text, voice, rate, mp3_path))
        return True
    except Exception:
        pass
    try:
        return _gtts_mp3(text, mp3_path)
    except Exception:
        return False


def text_to_aac(text: str, voice: str = DEFAULT_VOICE, rate: str = DEFAULT_RATE):
    """Chuyển text thành bytes .aac. Trả None nếu thiếu ffmpeg hoặc có lỗi."""
    tmpdir = tempfile.mkdtemp(prefix="tbz_voice_")
    mp3_path = os.path.join(tmpdir, "voice.mp3")
    aac_path = os.path.join(tmpdir, "voice.aac")
    try:
        if not _tts_to_mp3(text[:MAX_TEXT_LENGTH], voice, rate, mp3_path):
            return None
        ffmpeg = _find_ffmpeg()
        if not ffmpeg:
            return None
        # ép sample rate 44100 Hz (chuẩn, tương thích tối đa với trình phát của Zalo),
        # mono 1 kênh, AAC LC @ 64kbps
        subprocess.run(
            [ffmpeg, "-y", "-i", mp3_path, "-ar", "44100", "-ac", "1", "-c:a", "aac", "-b:a", "64k", aac_path],
            capture_output=True,
            timeout=120,
            check=True,
        )
        with open(aac_path, "rb") as f:
            return f.read()
    except Exception:
        return None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)