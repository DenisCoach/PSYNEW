import logging
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot

from config import NOTIFY_HOURS_START, NOTIFY_HOURS_END
from database import get_all_users, mark_notification_sent
from keyboards import notification_keyboard

logger = logging.getLogger(__name__)


async def send_hourly_notifications(bot: Bot):
    """
    Runs every minute. For each user checks if it is XX:00 in their timezone
    and the hour falls within the notification window. Sends once per hour
    thanks to deduplication in notifications_sent table.
    """
    users = await get_all_users()

    for user_id, timezone in users:
        try:
            tz = pytz.timezone(timezone)
            now = datetime.now(tz)

            if now.minute != 0:
                continue

            hour = now.hour
            if not (NOTIFY_HOURS_START <= hour <= NOTIFY_HOURS_END):
                continue

            # hour_slot = the hour period JUST completed (10 means 10:00–11:00)
            hour_slot = hour - 1
            date_str = now.date().isoformat()

            sent = await mark_notification_sent(user_id, date_str, hour_slot)
            if not sent:
                continue  # already sent this notification

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
