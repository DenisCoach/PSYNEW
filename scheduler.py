import logging
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot

from database import get_all_users, mark_notification_sent, get_notification_hours
from keyboards import notification_keyboard

logger = logging.getLogger(__name__)


async def send_hourly_notifications(bot: Bot):
    """
    Runs every minute. For each user checks if it is XX:00 in their timezone
    and the hour is in their personal notification schedule.
    """
    users = await get_all_users()

    for user_id, timezone, _ in users:
        try:
            tz = pytz.timezone(timezone)
            now = datetime.now(tz)

            if now.minute != 0:
                continue

            hour = now.hour
            # hour_slot = the period just completed (notify at 12:00 → ask about 11:00–12:00)
            # At 00:00 → ask about 23:00–00:00
            hour_slot = (hour - 1) % 24

            # Check user's personal schedule
            user_hours = await get_notification_hours(user_id)
            if hour_slot not in user_hours:
                continue

            date_str = now.date().isoformat()
            sent = await mark_notification_sent(user_id, date_str, hour_slot)
            if not sent:
                continue  # already sent

            await bot.send_message(
                user_id,
                f"⏰ <b>{hour:02d}:00</b>\n"
                f"Чем ты занимался с {hour_slot:02d}:00 до {hour:02d}:00?",
                parse_mode="HTML",
                reply_markup=notification_keyboard(date_str, hour_slot),
            )
            logger.info("Sent notification to %s for %s %02d:00", user_id, date_str, hour_slot)

        except Exception as e:
            logger.error("Failed to notify user %s: %s", user_id, e)


def setup_scheduler(scheduler: AsyncIOScheduler, bot: Bot):
    scheduler.add_job(
        send_hourly_notifications,
        trigger="interval",
        minutes=1,
        kwargs={"bot": bot},
        id="hourly_notifications",
        replace_existing=True,
    )
