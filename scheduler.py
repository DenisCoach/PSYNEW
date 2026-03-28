import logging
from datetime import datetime

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot

from database import (
    get_all_users, mark_notification_sent,
    get_notification_hours, get_recorded_hours_today, get_day_summary,
)
from keyboards import notification_keyboard

logger = logging.getLogger(__name__)

DAILY_SUMMARY_HOUR = 21   # Send day summary at 21:00 local time
MISSED_THRESHOLD   = 3    # Warn if this many consecutive scheduled hours are missed


def _fmt_dur(minutes: int) -> str:
    if minutes < 60:
        return f"{minutes} мин"
    h, m = divmod(minutes, 60)
    return f"{h}ч" if m == 0 else f"{h}ч {m}мин"


async def send_hourly_notifications(bot: Bot):
    users = await get_all_users()

    for user_id, timezone, _ in users:
        try:
            tz       = pytz.timezone(timezone)
            now      = datetime.now(tz)

            if now.minute != 0:
                continue

            hour      = now.hour
            hour_slot = (hour - 1) % 24
            date_str  = now.date().isoformat()

            # ── Daily summary at DAILY_SUMMARY_HOUR ───────────────────────────
            if hour == DAILY_SUMMARY_HOUR:
                sent = await mark_notification_sent(user_id, date_str, 99)
                if sent:
                    summary = await get_day_summary(user_id, date_str)
                    if summary:
                        total_m = sum(r[2] for r in summary)
                        lines   = [f"📊 <b>Итог дня — {date_str}</b>\n"]
                        for ctx_name, color, mins in summary:
                            pct  = int(mins / total_m * 100)
                            bar  = "█" * (pct // 10)
                            lines.append(f"{color} {ctx_name}  {_fmt_dur(mins)}  {bar} {pct}%")
                        lines.append(f"\n<b>Всего:</b> {_fmt_dur(total_m)}")
                        await bot.send_message(
                            user_id, "\n".join(lines), parse_mode="HTML"
                        )
                    else:
                        await bot.send_message(
                            user_id,
                            f"📊 <b>Итог дня — {date_str}</b>\n\n😔 Ничего не записано.",
                            parse_mode="HTML",
                        )

            # ── Regular hourly notification ───────────────────────────────────
            user_hours = await get_notification_hours(user_id)
            if hour_slot not in user_hours:
                continue

            sent = await mark_notification_sent(user_id, date_str, hour_slot)
            if not sent:
                continue

            # ── Missed hours check ────────────────────────────────────────────
            recorded      = await get_recorded_hours_today(user_id, date_str)
            past_scheduled = sorted([h for h in user_hours if h < hour_slot])
            missed = [h for h in past_scheduled if h not in recorded]

            # Count consecutive missed hours ending at hour_slot - 1
            consecutive = 0
            for h in reversed(past_scheduled):
                if h in recorded:
                    break
                consecutive += 1

            missed_text = ""
            if consecutive >= MISSED_THRESHOLD:
                missed_text = (
                    f"\n\n⚠️ Ты не записывал уже <b>{consecutive} ч</b> подряд."
                )

            await bot.send_message(
                user_id,
                f"⏰ <b>{hour:02d}:00</b>\n"
                f"Чем ты занимался с {hour_slot:02d}:00 до {hour:02d}:00?"
                f"{missed_text}",
                parse_mode="HTML",
                reply_markup=notification_keyboard(date_str, hour_slot),
            )
            logger.info("Notified %s for %s %02d:00", user_id, date_str, hour_slot)

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
