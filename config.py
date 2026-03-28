from dotenv import load_dotenv
import os

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "bot.db")

# Notifications sent at 11:00–21:00, each asking about the PREVIOUS hour
# e.g. at 11:00 → "что делал с 10:00 до 11:00?"
NOTIFY_HOURS_START = 11
NOTIFY_HOURS_END = 21

TIMEZONES = {
    "🇬🇪 Тбилиси (UTC+4)": "Asia/Tbilisi",
    "🇷🇺 Москва (UTC+3)": "Europe/Moscow",
    "🇺🇦 Киев (UTC+3)": "Europe/Kyiv",
    "🇧🇾 Минск (UTC+3)": "Europe/Minsk",
    "🇦🇲 Ереван (UTC+4)": "Asia/Yerevan",
    "🇦🇿 Баку (UTC+4)": "Asia/Baku",
    "🇩🇪 Берлин (UTC+1)": "Europe/Berlin",
    "🇬🇧 Лондон (UTC+0)": "Europe/London",
    "🇺🇸 Нью-Йорк (UTC-5)": "America/New_York",
    "🇺🇸 Лос-Анджелес (UTC-8)": "America/Los_Angeles",
}

ADMIN_IDS = [43711483]

CONTEXT_COLORS = [
    "🟥", "🟧", "🟨", "🟩", "🟦", "🟪",
    "🟫", "⬛", "🔴", "🔵", "🟤", "⚪",
]
