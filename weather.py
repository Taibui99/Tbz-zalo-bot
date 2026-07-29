"""Lấy thời tiết hiện tại từ Open-Meteo - miễn phí, không cần API key."""

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


def get_weather_summary(lat: float, lon: float) -> str:
    """Trả về 1 câu mô tả thời tiết hiện tại, ví dụ:
    'trời có mây rải rác, 28°C, gió 12 km/h'"""
    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={"latitude": lat, "longitude": lon, "current_weather": True},
            timeout=10,
        )
        resp.raise_for_status()
        current = resp.json()["current_weather"]
        code = current.get("weathercode", -1)
        desc = WEATHER_CODES.get(code, "thời tiết bình thường")
        temp = current.get("temperature")
        wind = current.get("windspeed")
        return f"{desc}, {temp}°C, gió {wind} km/h"
    except Exception as e:
        return f"(không lấy được dữ liệu thời tiết: {e})"
