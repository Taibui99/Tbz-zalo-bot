"""Lấy thời tiết hiện tại từ Open-Meteo - miễn phí, không cần API key."""

import time

import requests

# Mã thời tiết (weathercode) của Open-Meteo -> mô tả tiếng Việt
WEATHER_CODES = {
    0: "trời quang, nắng đẹp",
    1: "trời quang, ít mây",
    2: "trời có mây rải rác",
    3: "trời nhiều mây, âm u",
    45: "sương mù",
    48: "sương mù đóng băng",
    51: "mưa phùn nhẹ",
    53: "mưa phùn",
    55: "mưa phùn nặng hạt",
    61: "mưa nhỏ",
    63: "mưa vừa",
    65: "mưa to",
    80: "mưa rào nhẹ",
    81: "mưa rào",
    82: "mưa rào lớn",
    95: "có dông",
    96: "dông kèm mưa đá",
}

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
MAX_RETRIES = 2


def _retry_delay(response: requests.Response | None, attempt: int) -> float:
    """Ưu tiên Retry-After của API, nếu không có thì exponential backoff."""
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), 1.0), 15.0)
            except ValueError:
                pass
    return min(2.0 ** attempt, 8.0)


def get_weather_summary(lat: float, lon: float) -> str:
    """Trả về thời tiết hiện tại.

    Hàm này được scheduler gọi một lần lúc giờ chào buổi sáng, không polling
    liên tục. Khi Open-Meteo trả 429, retry có backoff thay vì đẩy raw exception
    vào tin nhắn Zalo.
    """
    if lat is None or lon is None:
        return "thời tiết hiện chưa khả dụng vì chưa cấu hình vị trí"

    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": True,
    }
    headers = {
        "User-Agent": "TBZ-Zalo-Bot/1.0 (weather summary)",
    }

    for attempt in range(MAX_RETRIES + 1):
        response = None
        try:
            response = requests.get(
                WEATHER_URL,
                params=params,
                headers=headers,
                timeout=10,
            )

            if response.status_code == 429:
                if attempt < MAX_RETRIES:
                    time.sleep(_retry_delay(response, attempt))
                    continue
                return "thời tiết hiện chưa lấy được do API đang giới hạn lượt truy cập"

            response.raise_for_status()
            current = response.json()["current_weather"]
            code = current.get("weathercode", -1)
            desc = WEATHER_CODES.get(code, "thời tiết bình thường")
            temp = current.get("temperature")
            wind = current.get("windspeed")
            return f"{desc}, {temp}°C, gió {wind} km/h"

        except requests.RequestException:
            if attempt < MAX_RETRIES:
                time.sleep(_retry_delay(response, attempt))
                continue
            return "thời tiết hiện chưa lấy được, bro thử lại sau nhé"
        except (KeyError, TypeError, ValueError):
            return "thời tiết hiện chưa lấy được do dữ liệu API không hợp lệ"
        except Exception:
            return "thời tiết hiện chưa lấy được do lỗi tạm thời"

    return "thời tiết hiện chưa lấy được, bro thử lại sau nhé"
