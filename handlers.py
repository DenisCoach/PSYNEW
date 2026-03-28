import re
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import pytz
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext

from config import TIMEZONES, NOTIFY_HOURS_START, NOTIFY_HOURS_END
from database import (
    register_user, user_exists, update_timezone, get_user,
    get_user_contexts, get_or_create_context, add_activity,
    get_activities_for_period,
)
from keyboards import (
    timezone_keyboard, notification_keyboard, contexts_keyboard,
    after_activity_keyboard, stats_keyboard,
)
from states import Registration, ActivityFSM

logger = logging.getLogger(__name__)
router = Router()


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_duration(text: str) -> Optional[int]:
    """Parse user input to minutes. Returns None if unrecognized."""
    text = text.strip().lower()

    # "1ч 30м" / "1ч30мин"
    m = re.match(r"(\d+)\s*ч\w*\s*(\d+)\s*м", text)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))

    # "1ч" alone
    m = re.match(r"(\d+)\s*ч", text)
    if m:
        return int(m.group(1)) * 60

    # "1:30"
    m = re.match(r"^(\d+):(\d{2})$", text)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))

    # "30м" / "30мин" / "30 мин"
    m = re.match(r"^(\d+)\s*м", text)
    if m:
        return int(m.group(1))

    # plain number → minutes
    m = re.match(r"^(\d+)$", text)
    if m:
        val = int(m.group(1))
        return val if val > 0 else None

    return None


def fmt_dur(minutes: int) -> str:
    if minutes < 60:
        return f"{minutes} мин"
    h, m = divmod(minutes, 60)
    return f"{h}ч" if m == 0 else f"{h}ч {m}мин"


def _tz_label(tz: str) -> str:
    return next((k for k, v in TIMEZONES.items() if v == tz), tz)


# ── Registration ──────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    if await user_exists(message.from_user.id):
        await message.answer(
            "👋 Ты уже зарегистрирован!\n\n"
            "/stats — статистика\n"
            "/add — добавить дело вручную\n"
            "/timezone — сменить часовой пояс"
        )
        return
    await state.set_state(Registration.choosing_timezone)
    await message.answer(
        "👋 Привет! Я буду помогать отслеживать куда уходит твоё время.\n\n"
        f"Каждый час с {NOTIFY_HOURS_START - 1}:00 до {NOTIFY_HOURS_END}:00 "
        "буду спрашивать чем ты занимался.\n\n"
        "📍 Выбери свой часовой пояс:",
        reply_markup=timezone_keyboard(),
    )


@router.message(Command("timezone"))
async def cmd_change_timezone(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await state.set_state(Registration.choosing_timezone)
    await message.answer("🌍 Выбери новый часовой пояс:", reply_markup=timezone_keyboard())


@router.callback_query(F.data.startswith("tz:"), Registration.choosing_timezone)
async def cb_timezone(callback: CallbackQuery, state: FSMContext):
    tz = callback.data.split(":", 1)[1]
    user = callback.from_user
    is_new = await register_user(user.id, user.username or user.first_name or "", tz)
    if not is_new:
        await update_timezone(user.id, tz)

    await state.clear()
    label = _tz_label(tz)

    if is_new:
        text = (
            f"✅ Готово! Часовой пояс: {label}\n\n"
            f"Каждый час с {NOTIFY_HOURS_START - 1}:00 до {NOTIFY_HOURS_END}:00 "
            "я буду спрашивать чем ты занимался.\n\n"
            "/stats — статистика\n"
            "/add — добавить дело вручную\n"
            "/help — помощь"
        )
    else:
        text = f"✅ Часовой пояс обновлён: {label}"

    await callback.message.edit_text(text)
    await callback.answer()


# ── Notification callbacks ────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("act_add:"))
async def cb_act_add(callback: CallbackQuery, state: FSMContext):
    _, date_str, hour_str = callback.data.split(":")
    hour = int(hour_str)
    await state.update_data(date_str=date_str, hour=hour)
    await state.set_state(ActivityFSM.waiting_description)
    await callback.message.answer(
        f"📝 Что ты делал с {hour:02d}:00 до {hour + 1:02d}:00?\n\nОпиши занятие:"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("act_skip:"))
async def cb_act_skip(callback: CallbackQuery):
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer("Пропущено")


@router.callback_query(F.data.startswith("act_more:"))
async def cb_act_more(callback: CallbackQuery, state: FSMContext):
    _, date_str, hour_str = callback.data.split(":")
    hour = int(hour_str)
    await state.update_data(date_str=date_str, hour=hour)
    await state.set_state(ActivityFSM.waiting_description)
    await callback.message.answer(
        f"📝 Ещё одно дело за {hour:02d}:00–{hour + 1:02d}:00?\n\nОпиши:"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("act_done:"))
async def cb_act_done(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer("✅ Всё записано!")


# ── /add — manual entry ───────────────────────────────────────────────────────

@router.message(Command("add"))
async def cmd_add(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    user = await get_user(message.from_user.id)
    tz = pytz.timezone(user[2])
    now = datetime.now(tz)
    hour = now.hour
    date_str = now.date().isoformat()

    await state.update_data(date_str=date_str, hour=hour)
    await state.set_state(ActivityFSM.waiting_description)
    await message.answer(
        f"📝 Добавляем дело за {hour:02d}:00–{hour + 1:02d}:00 ({date_str})\n\n"
        "Опиши что ты делал:"
    )


# ── FSM steps ─────────────────────────────────────────────────────────────────

@router.message(ActivityFSM.waiting_description)
async def fsm_description(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    await state.set_state(ActivityFSM.waiting_duration)
    await message.answer(
        "⏱ Сколько времени это заняло?\n\n"
        "Примеры: <code>30</code>  <code>45 мин</code>  <code>1ч</code>  <code>1ч 30мин</code>",
        parse_mode="HTML",
    )


@router.message(ActivityFSM.waiting_duration)
async def fsm_duration(message: Message, state: FSMContext):
    minutes = parse_duration(message.text)
    if not minutes or minutes > 600:
        await message.answer(
            "❌ Не понял. Введи длительность, например:\n"
            "<code>30</code>  <code>1ч 30мин</code>  <code>45 мин</code>",
            parse_mode="HTML",
        )
        return

    await state.update_data(duration=minutes)
    await state.set_state(ActivityFSM.choosing_context)
    data = await state.get_data()
    contexts = await get_user_contexts(message.from_user.id)

    await message.answer(
        "🏷 Выбери контекст:",
        reply_markup=contexts_keyboard(contexts, data["date_str"], data["hour"]),
    )


@router.callback_query(F.data.startswith("ctx:"), ActivityFSM.choosing_context)
async def fsm_chose_context(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    ctx_id, date_str, hour = int(parts[1]), parts[2], int(parts[3])
    data = await state.get_data()

    contexts = await get_user_contexts(callback.from_user.id)
    ctx = next((c for c in contexts if c[0] == ctx_id), None)
    if not ctx:
        await callback.answer("Контекст не найден", show_alert=True)
        return

    await add_activity(
        user_id=callback.from_user.id,
        context_id=ctx_id,
        description=data["description"],
        duration_minutes=data["duration"],
        activity_date=date_str,
        hour_slot=hour,
    )
    await state.clear()

    await callback.message.edit_text(
        f"✅ Записано!\n\n"
        f"{ctx[2]} {ctx[1]}  ·  {data['description']}  ·  {fmt_dur(data['duration'])}",
        reply_markup=after_activity_keyboard(date_str, hour),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ctx_new:"), ActivityFSM.choosing_context)
async def fsm_new_context_start(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    date_str, hour = parts[1], int(parts[2])
    await state.update_data(date_str=date_str, hour=hour)
    await state.set_state(ActivityFSM.entering_new_context)
    await callback.message.answer("✏️ Введи название нового контекста:")
    await callback.answer()


@router.message(ActivityFSM.entering_new_context)
async def fsm_new_context_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 30:
        await message.answer("❌ Слишком длинное название. Максимум 30 символов.")
        return

    data = await state.get_data()
    ctx_id, color = await get_or_create_context(message.from_user.id, name)

    await add_activity(
        user_id=message.from_user.id,
        context_id=ctx_id,
        description=data["description"],
        duration_minutes=data["duration"],
        activity_date=data["date_str"],
        hour_slot=data["hour"],
    )
    await state.clear()

    await message.answer(
        f"✅ Записано! Создан контекст {color} {name}\n\n"
        f"{color} {name}  ·  {data['description']}  ·  {fmt_dur(data['duration'])}",
        reply_markup=after_activity_keyboard(data["date_str"], data["hour"]),
    )


# ── /cancel ───────────────────────────────────────────────────────────────────

@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Отменено.")


# ── Statistics ────────────────────────────────────────────────────────────────

@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await message.answer("📊 Выбери период:", reply_markup=stats_keyboard())


@router.callback_query(F.data.startswith("stats:"))
async def cb_stats(callback: CallbackQuery):
    period = callback.data.split(":")[1]
    user = await get_user(callback.from_user.id)
    tz = pytz.timezone(user[2])
    today = datetime.now(tz).date()

    if period == "day":
        start, end = today, today
        title = today.strftime("📅 %d %B %Y")
    elif period == "week":
        start = today - timedelta(days=today.weekday())
        end = today
        title = f"📆 Неделя ({start.strftime('%d.%m')}–{end.strftime('%d.%m.%Y')})"
    else:
        start = today.replace(day=1)
        end = today
        title = today.strftime("📊 %B %Y")

    activities = await get_activities_for_period(
        callback.from_user.id, start.isoformat(), end.isoformat()
    )

    if not activities:
        text = f"{title}\n\n😔 Нет данных за этот период."
    else:
        text = _format_stats(activities, period, title)
        if len(text) > 4000:
            text = text[:3900] + "\n\n…(показано частично)"

    try:
        await callback.message.edit_text(
            text, reply_markup=stats_keyboard(), parse_mode="HTML"
        )
    except Exception:
        pass
    await callback.answer()


def _format_stats(
    activities: List[Tuple], period: str, title: str
) -> str:
    lines = [f"<b>{title}</b>", ""]

    if period == "day":
        hours: dict = {}
        for _, hour, ctx, color, desc, dur in activities:
            hours.setdefault(hour, []).append((ctx, color, desc, dur))

        for hour in sorted(hours):
            lines.append(f"🕐 <b>{hour:02d}:00–{hour + 1:02d}:00</b>")
            for ctx, color, desc, dur in hours[hour]:
                lines.append(f"  {color} {ctx}  ·  {desc}  ·  {fmt_dur(dur)}")
            lines.append("")
    else:
        days: dict = {}
        for act_date, hour, ctx, color, desc, dur in activities:
            days.setdefault(act_date, {}).setdefault(hour, []).append(
                (ctx, color, desc, dur)
            )

        for day in sorted(days):
            lines.append(f"<b>📅 {day}</b>")
            for hour in sorted(days[day]):
                lines.append(f"  {hour:02d}:00–{hour + 1:02d}:00")
                for ctx, color, desc, dur in days[day][hour]:
                    lines.append(f"    {color} {ctx}  ·  {desc}  ·  {fmt_dur(dur)}")
            lines.append("")

    # Summary
    lines.append("─" * 22)
    lines.append("<b>Итого по контекстам:</b>")
    totals: dict = {}
    for _, hour, ctx, color, desc, dur in activities:
        if ctx not in totals:
            totals[ctx] = {"color": color, "m": 0}
        totals[ctx]["m"] += dur

    total_m = sum(v["m"] for v in totals.values())
    for ctx, data in sorted(totals.items(), key=lambda x: -x[1]["m"]):
        pct = int(data["m"] / total_m * 100) if total_m else 0
        bar = "█" * (pct // 5)
        lines.append(
            f"{data['color']} {ctx:<14}  {fmt_dur(data['m']):>8}  {bar} {pct}%"
        )

    lines.append("")
    lines.append(f"<b>Всего:</b> {fmt_dur(total_m)}")
    return "\n".join(lines)


# ── Help ──────────────────────────────────────────────────────────────────────

@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "<b>Команды:</b>\n\n"
        "/start — регистрация\n"
        "/add — добавить дело вручную\n"
        "/stats — просмотр статистики\n"
        "/timezone — сменить часовой пояс\n"
        "/cancel — отменить текущее действие\n"
        "/help — эта справка",
        parse_mode="HTML",
    )
