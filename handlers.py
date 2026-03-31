import re
import os
import asyncio
import logging
import functools
import subprocess
import tempfile
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional, List, Tuple

import pytz
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, BufferedInputFile, InlineKeyboardMarkup
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext

from config import TIMEZONES, NOTIFY_HOURS_START, NOTIFY_HOURS_END, ADMIN_IDS
from visualizer import generate_grid, generate_dynamics
from database import (
    register_user, user_exists, update_timezone, get_user,
    get_user_contexts, get_or_create_context, add_activity,
    get_activities_for_period, get_all_users_stats, get_user_full_stats,
    get_notification_hours, toggle_notification_hour,
    get_recent_activities, get_activity_by_id, delete_activity,
    update_activity_description, update_activity_duration, update_activity_context,
    get_context_by_id, rename_context, update_context_color,
    count_context_activities, delete_context,
    get_export_activities, set_goal, delete_goal, get_goals_with_progress,
    update_activity_tags, save_day_note, get_day_note, delete_day_note,
    get_week_comparison,
    get_top_activities, get_streak, get_hour_patterns,
    get_templates, get_template_by_id, add_template, delete_template, increment_template_use,
    get_recent_unique_for_quick,
    ensure_default_habits, get_habits, get_habit_by_id, add_habit, delete_habit,
    get_habit_logs_today, log_habit, delete_habit_log,
    create_snapshot, get_snapshots, restore_snapshot, delete_snapshot, reset_user_data,
    get_places, add_place, delete_place, set_activity_place, get_activity_place,
    get_people, add_person, delete_person, set_activity_people, get_activity_people,
)
from keyboards import (
    timezone_keyboard, notification_keyboard, notification_quick_keyboard,
    notification_added_keyboard, contexts_keyboard,
    after_activity_keyboard, stats_keyboard, schedule_keyboard,
    activities_list_keyboard, edit_menu_keyboard, delete_confirm_keyboard,
    contexts_list_keyboard, context_menu_keyboard, color_picker_keyboard,
    ctx_delete_confirm_keyboard, goals_contexts_keyboard, export_keyboard,
    tags_keyboard, templates_keyboard, main_menu_keyboard,
    hour_picker_keyboard, hour_picker_day_keyboard,
    habits_keyboard, habit_action_keyboard, habits_manage_keyboard,
    duration_keyboard, settings_keyboard, snapshots_keyboard,
    snapshot_actions_keyboard, snap_restore_confirm_keyboard,
    reset_confirm_keyboard,
    place_picker_keyboard, people_picker_keyboard,
    places_list_keyboard, people_list_keyboard,
    space_menu_keyboard,
    PREDEFINED_TAGS,
)
import csv
import io
from states import Registration, ActivityFSM, EditFSM, ContextFSM, GoalFSM, NoteFSM, NotifFSM, HabitFSM, SnapshotFSM, PlaceFSM, PersonFSM, PeoplePickFSM

try:
    import speech_recognition as sr
    _HAS_SR = True
except ImportError:
    _HAS_SR = False

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


async def _transcribe_voice(voice_bytes: bytes) -> Optional[str]:
    """Download voice bytes (ogg/opus) → convert via ffmpeg → Google STT → text."""
    if not _HAS_SR:
        return None

    def _blocking():
        ogg_path = wav_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
                f.write(voice_bytes)
                ogg_path = f.name
            wav_path = ogg_path[:-4] + ".wav"
            res = subprocess.run(
                ["ffmpeg", "-i", ogg_path, "-ar", "16000", "-ac", "1", "-f", "wav", wav_path, "-y"],
                capture_output=True, timeout=30,
            )
            if res.returncode != 0:
                return None
            recognizer = sr.Recognizer()
            with sr.AudioFile(wav_path) as source:
                audio = recognizer.record(source)
            return recognizer.recognize_google(audio, language="ru-RU")
        except Exception as exc:
            logger.warning("Voice transcription failed: %s", exc)
            return None
        finally:
            for p in [ogg_path, wav_path]:
                if p:
                    try:
                        os.unlink(p)
                    except OSError:
                        pass

    return await asyncio.to_thread(_blocking)


# ── Registration ──────────────────────────────────────────────────────────────

WELCOME_TEXT = """👋 Привет! Я помогаю отслеживать куда уходит твоё время.

Каждый час я буду спрашивать чем ты занимался — ты указываешь дело, контекст и длительность. Со временем появляется полная картина твоей недели и месяца.

━━━━━━━━━━━━━━━━━━━━━━
📋 <b>КАК ЭТО РАБОТАЕТ</b>

Каждый час бот присылает вопрос:
<i>«Чем ты занимался с 14:00 до 15:00?»</i>

Ты нажимаешь ➕ и вводишь:
1. Описание — что делал
2. Длительность — сколько времени (напр. <code>45 мин</code> или <code>1ч 30мин</code>)
3. Контекст — категория дела (Работа, Спорт, Учёба и т.д.)

За один час можно добавить несколько дел из разных контекстов.

━━━━━━━━━━━━━━━━━━━━━━
🗂 <b>КОНТЕКСТЫ</b>

Контексты — это категории твоих дел. Они накапливаются сами и предлагаются на выбор. Каждому контексту присваивается свой цвет.

/contexts — переименовать, сменить цвет или удалить контекст

━━━━━━━━━━━━━━━━━━━━━━
📊 <b>СТАТИСТИКА</b>

/stats — семь режимов просмотра:
• <b>День</b> — все записи за сегодня по часам (🔥 серия дней)
• <b>Неделя / Месяц</b> — сводка с % по контекстам
• <b>Сетка недели / Сетка месяца</b> — картинка-грид, где каждый час закрашен цветом контекста (справа — часы за день)
• <b>Сравнение недель</b> — эта неделя vs прошлая по каждому контексту
• <b>Динамика</b> — линейный график: как менялись часы по контекстам неделя за неделей

/top — топ-10 самых частых дел
/patterns — паттерны продуктивности по времени суток

━━━━━━━━━━━━━━━━━━━━━━
🎯 <b>ЦЕЛИ</b>

/goals — задай сколько часов в неделю хочешь тратить на каждый контекст и следи за прогрессом:
<code>█████░░░░░  5ч / 10ч  (50%)</code>

━━━━━━━━━━━━━━━━━━━━━━
⚡ <b>БЫСТРЫЕ ДЕЛА</b>

/quick — добавить дело одним нажатием по сохранённому шаблону.
После сохранения любого дела нажми <b>💾 Шаблон</b> — и оно попадёт в быстрый список.

🎤 Можешь отправить <b>голосовое сообщение</b> — бот распознает и предложит заполнить запись.

━━━━━━━━━━━━━━━━━━━━━━
⏰ <b>РАСПИСАНИЕ УВЕДОМЛЕНИЙ</b>

/schedule — выбери в какие именно часы получать напоминания. Можно включить любые из 24 часов.

По умолчанию: с 10:00 до 21:00 каждый час.

Если ты не записывал ничего 3 часа подряд — бот предупредит об этом.

Каждый день в 21:00 — автоматический итог дня.

━━━━━━━━━━━━━━━━━━━━━━
🏷 <b>ТЕГИ</b>

После каждого сохранённого дела можно добавить теги: #важное #срочное #рутина #фокус #встреча #отдых

━━━━━━━━━━━━━━━━━━━━━━
📝 <b>ЗАМЕТКИ</b>

/note — написать заметку за сегодня (настроение, мысли, итоги дня).
Заметка отображается внизу дневной статистики.
/note_del — удалить заметку за сегодня.

━━━━━━━━━━━━━━━━━━━━━━
✏️ <b>РЕДАКТИРОВАНИЕ</b>

/edit — выбери любую из последних 10 записей и измени описание, время или контекст, либо удали её.

━━━━━━━━━━━━━━━━━━━━━━
📤 <b>ЭКСПОРТ</b>

/export — выгрузи все данные в CSV файл (за неделю, месяц или всё время).

━━━━━━━━━━━━━━━━━━━━━━
<b>Все команды:</b>
/add · /quick · /edit · /note · /stats · /top · /patterns · /goals · /contexts · /schedule · /export · /timezone · /cancel"""


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    if await user_exists(message.from_user.id):
        await message.answer(
            WELCOME_TEXT + "\n\n📍 Твой часовой пояс и расписание уже настроены.",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )
        return
    await state.set_state(Registration.choosing_timezone)
    await message.answer(
        WELCOME_TEXT,
        parse_mode="HTML",
    )
    await message.answer(
        "📍 Для начала выбери свой часовой пояс:",
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
            "Используй кнопки внизу экрана для навигации 👇"
        )
    else:
        text = f"✅ Часовой пояс обновлён: {label}"

    await callback.message.edit_text(text)
    await callback.message.answer("👇 Меню всегда доступно:", reply_markup=main_menu_keyboard())
    await callback.answer()


# ── Notification callbacks ────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("act_add:"))
async def cb_act_add(callback: CallbackQuery, state: FSMContext):
    _, date_str, hour_str = callback.data.split(":")
    hour = int(hour_str)

    recent = await get_recent_unique_for_quick(callback.from_user.id)

    if recent:
        # Edit the notification inline — show quick options
        await state.set_state(NotifFSM.quick_adding)
        await state.update_data(date_str=date_str, hour=hour, added=[])
        await callback.message.edit_text(
            f"⏰ <b>{hour:02d}:00–{hour + 1:02d}:00</b>  |  {date_str}\n\n"
            "Выбери из недавних или введи своё:",
            parse_mode="HTML",
            reply_markup=notification_quick_keyboard(recent, date_str, hour),
        )
    else:
        # No history yet — go straight to FSM
        await state.update_data(date_str=date_str, hour=hour)
        await state.set_state(ActivityFSM.waiting_description)
        await callback.message.answer(
            f"📝 Что ты делал с {hour:02d}:00 до {hour + 1:02d}:00?\n\nОпиши занятие:"
        )
    await callback.answer()


@router.callback_query(F.data.startswith("qk:"))
async def cb_quick_notif_add(callback: CallbackQuery, state: FSMContext):
    """One-tap add from notification quick menu."""
    parts    = callback.data.split(":")
    act_id   = int(parts[1])
    date_str = parts[2]
    hour     = int(parts[3])

    # Fetch original activity to copy description/duration/context
    orig = await get_activity_by_id(act_id, callback.from_user.id)
    if not orig:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    # orig: (id, activity_date, hour_slot, ctx_name, color, description, duration)
    _, _, _, ctx_name, color, desc, dur = orig

    # Find context_id
    contexts = await get_user_contexts(callback.from_user.id)
    ctx = next((c for c in contexts if c[1] == ctx_name), None)
    if not ctx:
        await callback.answer("Контекст не найден", show_alert=True)
        return

    await add_activity(
        user_id=callback.from_user.id,
        context_id=ctx[0],
        description=desc,
        duration_minutes=dur,
        activity_date=date_str,
        hour_slot=hour,
    )

    # Update state with added items
    data  = await state.get_data()
    added = data.get("added", [])
    added.append(f"{color} {desc} · {fmt_dur(dur)}")
    await state.update_data(added=added)

    lines = [f"⏰ <b>{hour:02d}:00–{hour + 1:02d}:00</b>  |  {date_str}\n"]
    lines.append("✅ <b>Добавлено:</b>")
    for item in added:
        lines.append(f"• {item}")

    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=notification_added_keyboard(date_str, hour, added),
    )
    await callback.answer("✅ Записано!")


@router.callback_query(F.data.startswith("qk_custom:"))
async def cb_qk_custom(callback: CallbackQuery, state: FSMContext):
    """User wants to enter a custom activity from the quick-add menu."""
    parts    = callback.data.split(":")
    date_str = parts[1]
    hour     = int(parts[2])
    await state.update_data(date_str=date_str, hour=hour)
    await state.set_state(ActivityFSM.waiting_description)
    await callback.message.edit_text(
        f"📝 <b>{hour:02d}:00–{hour + 1:02d}:00</b>  |  {date_str}\n\n"
        "Опиши занятие — просто напиши текст:",
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("qk_more:"))
async def cb_qk_more(callback: CallbackQuery, state: FSMContext):
    """Show quick options again to add another activity."""
    _, date_str, hour_str = callback.data.split(":")
    hour   = int(hour_str)
    recent = await get_recent_unique_for_quick(callback.from_user.id)
    data   = await state.get_data()
    added  = data.get("added", [])

    lines = [f"⏰ <b>{hour:02d}:00–{hour + 1:02d}:00</b>  |  {date_str}"]
    if added:
        lines.append("\n✅ Уже добавлено: " + "  /  ".join(added))
    lines.append("\nЧто ещё добавить?")

    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=notification_quick_keyboard(recent, date_str, hour),
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
    data  = await state.get_data()
    added = data.get("added", [])
    await state.clear()
    if added:
        lines = [f"⏰ <b>{callback.data.split(':')[2]}–{int(callback.data.split(':')[2][:2]) + 1:02d}:00</b>  ✅ Записано:"]
        for item in added:
            lines.append(f"• {item}")
        try:
            await callback.message.edit_text("\n".join(lines), parse_mode="HTML")
        except Exception:
            await callback.message.edit_reply_markup(reply_markup=None)
    else:
        await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer("✅ Всё записано!")


# ── /add — manual entry ───────────────────────────────────────────────────────

async def _show_hour_picker(target: Message, user_id: int, edit: bool = False):
    user = await get_user(user_id)
    tz   = pytz.timezone(user[2])
    now  = datetime.now(tz)
    today_str     = now.date().isoformat()
    yesterday_str = (now.date() - timedelta(days=1)).isoformat()
    kb = hour_picker_keyboard(today_str, yesterday_str, now.hour)
    text = "🕐 <b>За какой час добавить дело?</b>\n\nВыбери день и час:"
    if edit:
        await target.edit_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=kb)


@router.message(Command("add"))
async def cmd_add(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await state.clear()
    await _show_hour_picker(message, message.from_user.id)


@router.callback_query(F.data.startswith("addday:"))
async def cb_addday(callback: CallbackQuery, state: FSMContext):
    """Switch selected day in the hour picker."""
    _, date_str, hour_str = callback.data.split(":")
    user = await get_user(callback.from_user.id)
    tz   = pytz.timezone(user[2])
    now  = datetime.now(tz)
    today_str     = now.date().isoformat()
    yesterday_str = (now.date() - timedelta(days=1)).isoformat()
    kb = hour_picker_day_keyboard(date_str, today_str, yesterday_str, int(hour_str))
    await callback.message.edit_reply_markup(reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("addhour:"))
async def cb_addhour(callback: CallbackQuery, state: FSMContext):
    """Hour selected — proceed to description input."""
    _, date_str, hour_str = callback.data.split(":")
    hour = int(hour_str)
    await state.update_data(date_str=date_str, hour=hour)
    await state.set_state(ActivityFSM.waiting_description)

    recent = await get_recent_unique_for_quick(callback.from_user.id)
    if recent:
        await state.update_data(added=[])
        await state.set_state(NotifFSM.quick_adding)
        await callback.message.edit_text(
            f"⏰ <b>{hour:02d}:00–{hour + 1:02d}:00</b>  |  {date_str}\n\n"
            "Выбери из недавних или введи своё:",
            parse_mode="HTML",
            reply_markup=notification_quick_keyboard(recent, date_str, hour),
        )
    else:
        await state.set_state(ActivityFSM.waiting_description)
        await callback.message.edit_text(
            f"📝 <b>{hour:02d}:00–{hour + 1:02d}:00</b>  |  {date_str}\n\n"
            "Опиши занятие — просто напиши текст:",
            parse_mode="HTML",
        )
    await callback.answer()


# ── FSM steps ─────────────────────────────────────────────────────────────────

DURATION_PROMPT = (
    "⏱ Сколько времени это заняло?\n\n"
    "Выбери или напиши своё: <code>45 мин</code>  <code>1ч 30мин</code>"
)

@router.message(ActivityFSM.waiting_description)
async def fsm_description(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    await state.set_state(ActivityFSM.waiting_duration)
    await message.answer(DURATION_PROMPT, parse_mode="HTML", reply_markup=duration_keyboard())


@router.callback_query(F.data.startswith("dur:"), ActivityFSM.waiting_duration)
async def cb_duration_pick(callback: CallbackQuery, state: FSMContext):
    minutes = int(callback.data.split(":")[1])
    if minutes == 0:
        # "✏️ Своё" — ask for text input
        await callback.message.edit_text(
            "⏱ Введи своё время:\n<code>45 мин</code>  <code>1ч 30мин</code>  <code>90</code>",
            parse_mode="HTML",
        )
        await callback.answer()
        return

    await state.update_data(duration=minutes)
    await state.set_state(ActivityFSM.choosing_context)
    data = await state.get_data()
    contexts = await get_user_contexts(callback.from_user.id)
    await callback.message.edit_text(
        f"⏱ {fmt_dur(minutes)}\n\n🏷 Выбери контекст:",
        reply_markup=contexts_keyboard(contexts, data["date_str"], data["hour"]),
    )
    await callback.answer()


@router.message(ActivityFSM.waiting_duration)
async def fsm_duration(message: Message, state: FSMContext):
    minutes = parse_duration(message.text)
    if not minutes or minutes > 600:
        await message.answer(
            "❌ Не понял. Например: <code>30</code>  <code>1ч 30мин</code>  <code>45 мин</code>",
            parse_mode="HTML",
            reply_markup=duration_keyboard(),
        )
        return

    await state.update_data(duration=minutes)
    await state.set_state(ActivityFSM.choosing_context)
    data = await state.get_data()
    contexts = await get_user_contexts(message.from_user.id)
    await message.answer(
        f"⏱ {fmt_dur(minutes)}\n\n🏷 Выбери контекст:",
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

    act_id = await add_activity(
        user_id=callback.from_user.id,
        context_id=ctx_id,
        description=data["description"],
        duration_minutes=data["duration"],
        activity_date=date_str,
        hour_slot=hour,
    )
    await state.update_data(last_act_id=act_id, selected_tags=[], date_str=date_str, hour=hour)
    await state.set_state(ActivityFSM.choosing_tags)

    await callback.message.edit_text(
        f"✅ Записано!\n\n"
        f"{ctx[2]} {ctx[1]}  ·  {data['description']}  ·  {fmt_dur(data['duration'])}\n\n"
        f"🏷 Добавь теги (необязательно):",
        reply_markup=tags_keyboard([], act_id),
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

    act_id = await add_activity(
        user_id=message.from_user.id,
        context_id=ctx_id,
        description=data["description"],
        duration_minutes=data["duration"],
        activity_date=data["date_str"],
        hour_slot=data["hour"],
    )
    await state.update_data(last_act_id=act_id, selected_tags=[])
    await state.set_state(ActivityFSM.choosing_tags)

    await message.answer(
        f"✅ Записано! Создан контекст {color} {name}\n\n"
        f"{color} {name}  ·  {data['description']}  ·  {fmt_dur(data['duration'])}\n\n"
        f"🏷 Добавь теги (необязательно):",
        reply_markup=tags_keyboard([], act_id),
    )


# ── Tags ──────────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("tg:"), ActivityFSM.choosing_tags)
async def cb_tag_toggle(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    tag, act_id = parts[1], int(parts[2])
    data = await state.get_data()
    selected = data.get("selected_tags", [])
    selected = [t for t in selected if t != tag] if tag in selected else selected + [tag]
    await state.update_data(selected_tags=selected)
    await callback.message.edit_reply_markup(reply_markup=tags_keyboard(selected, act_id))
    await callback.answer()


@router.callback_query(F.data.startswith("tg_save:"), ActivityFSM.choosing_tags)
async def cb_tag_save(callback: CallbackQuery, state: FSMContext):
    act_id = int(callback.data.split(":")[1])
    data   = await state.get_data()
    tags   = data.get("selected_tags", [])
    if tags:
        await update_activity_tags(act_id, callback.from_user.id, tags)
    await state.clear()
    tag_text = "  ".join(f"#{t}" for t in tags) if tags else "без тегов"
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        f"🏷 {tag_text}\n\n📍 Где это было? Кто был рядом? (опционально)",
        reply_markup=after_activity_keyboard(data["date_str"], data["hour"], act_id),
    )
    await callback.answer()


# ── Day notes ──────────────────────────────────────────────────────────────────

@router.message(Command("note"))
async def cmd_note(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    user    = await get_user(message.from_user.id)
    tz      = pytz.timezone(user[2])
    today   = datetime.now(tz).date().isoformat()
    existing = await get_day_note(message.from_user.id, today)

    if existing:
        await message.answer(
            f"📝 <b>Заметка за {today}:</b>\n\n{existing}\n\n"
            "Отправь новый текст чтобы изменить, или /note_del чтобы удалить.",
            parse_mode="HTML",
        )
    else:
        await message.answer(f"📝 Напиши заметку за {today}:")

    await state.set_state(NoteFSM.waiting_text)
    await state.update_data(note_date=today)


@router.message(NoteFSM.waiting_text)
async def fsm_note_text(message: Message, state: FSMContext):
    data = await state.get_data()
    await save_day_note(message.from_user.id, data["note_date"], message.text.strip())
    await state.clear()
    await message.answer(f"✅ Заметка за {data['note_date']} сохранена.")


@router.message(Command("note_del"))
async def cmd_note_del(message: Message, state: FSMContext):
    await state.clear()
    user  = await get_user(message.from_user.id)
    tz    = pytz.timezone(user[2])
    today = datetime.now(tz).date().isoformat()
    await delete_day_note(message.from_user.id, today)
    await message.answer("✅ Заметка удалена.")


# ── Edit / Delete ─────────────────────────────────────────────────────────────

async def _activity_text(act: Tuple) -> str:
    act_id, act_date, hour, ctx, color, desc, dur = act
    return (
        f"{color} <b>{ctx}</b>\n"
        f"📅 {act_date}  🕐 {hour:02d}:00–{hour+1:02d}:00\n"
        f"📝 {desc}\n"
        f"⏱ {fmt_dur(dur)}"
    )


@router.message(Command("edit"))
async def cmd_edit(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await state.clear()
    activities = await get_recent_activities(message.from_user.id, limit=10)
    if not activities:
        await message.answer("Нет записей для редактирования.")
        return
    await message.answer(
        "✏️ <b>Выбери запись:</b>",
        parse_mode="HTML",
        reply_markup=activities_list_keyboard(activities),
    )


@router.callback_query(F.data.startswith("ea:"))
async def cb_edit_select(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    act_id = int(callback.data.split(":")[1])
    act = await get_activity_by_id(act_id, callback.from_user.id)
    if not act:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    text = await _activity_text(act)
    await callback.message.edit_text(
        text, parse_mode="HTML",
        reply_markup=edit_menu_keyboard(act_id),
    )
    await callback.answer()


@router.callback_query(F.data == "edit_back")
async def cb_edit_back(callback: CallbackQuery):
    activities = await get_recent_activities(callback.from_user.id, limit=10)
    await callback.message.edit_text(
        "✏️ <b>Выбери запись:</b>",
        parse_mode="HTML",
        reply_markup=activities_list_keyboard(activities),
    )
    await callback.answer()


# — Edit description —

@router.callback_query(F.data.startswith("ed:"))
async def cb_edit_desc_start(callback: CallbackQuery, state: FSMContext):
    act_id = int(callback.data.split(":")[1])
    await state.set_state(EditFSM.waiting_new_description)
    await state.update_data(act_id=act_id)
    await callback.message.answer("✏️ Введи новое описание:")
    await callback.answer()


@router.message(EditFSM.waiting_new_description)
async def fsm_edit_desc(message: Message, state: FSMContext):
    data = await state.get_data()
    await update_activity_description(data["act_id"], message.from_user.id, message.text.strip())
    await state.clear()
    act = await get_activity_by_id(data["act_id"], message.from_user.id)
    text = await _activity_text(act)
    await message.answer(
        f"✅ Описание обновлено!\n\n{text}",
        parse_mode="HTML",
        reply_markup=edit_menu_keyboard(data["act_id"]),
    )


# — Edit duration —

@router.callback_query(F.data.startswith("et:"))
async def cb_edit_dur_start(callback: CallbackQuery, state: FSMContext):
    act_id = int(callback.data.split(":")[1])
    await state.set_state(EditFSM.waiting_new_duration)
    await state.update_data(act_id=act_id)
    await callback.message.answer(
        "⏱ Новая длительность:",
        reply_markup=duration_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("dur:"), EditFSM.waiting_new_duration)
async def cb_edit_dur_pick(callback: CallbackQuery, state: FSMContext):
    minutes = int(callback.data.split(":")[1])
    if minutes == 0:
        await callback.message.edit_text(
            "⏱ Введи своё время:\n<code>30</code>  <code>1ч</code>  <code>1ч 30мин</code>",
            parse_mode="HTML",
        )
        await callback.answer()
        return
    data = await state.get_data()
    await update_activity_duration(data["act_id"], callback.from_user.id, minutes)
    await state.clear()
    act  = await get_activity_by_id(data["act_id"], callback.from_user.id)
    text = await _activity_text(act)
    await callback.message.edit_text(
        f"✅ Время обновлено!\n\n{text}",
        parse_mode="HTML",
        reply_markup=edit_menu_keyboard(data["act_id"]),
    )
    await callback.answer()


@router.message(EditFSM.waiting_new_duration)
async def fsm_edit_dur(message: Message, state: FSMContext):
    minutes = parse_duration(message.text)
    if not minutes or minutes > 600:
        await message.answer("❌ Не понял. Например: <code>45</code> или <code>1ч 30мин</code>", parse_mode="HTML")
        return
    data = await state.get_data()
    await update_activity_duration(data["act_id"], message.from_user.id, minutes)
    await state.clear()
    act = await get_activity_by_id(data["act_id"], message.from_user.id)
    text = await _activity_text(act)
    await message.answer(
        f"✅ Время обновлено!\n\n{text}",
        parse_mode="HTML",
        reply_markup=edit_menu_keyboard(data["act_id"]),
    )


# — Edit context —

@router.callback_query(F.data.startswith("ec:"))
async def cb_edit_ctx_start(callback: CallbackQuery, state: FSMContext):
    act_id = int(callback.data.split(":")[1])
    await state.set_state(EditFSM.choosing_new_context)
    await state.update_data(act_id=act_id)
    contexts = await get_user_contexts(callback.from_user.id)
    await callback.message.answer(
        "🔄 Выбери новый контекст:",
        reply_markup=contexts_keyboard(contexts, "edit", act_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ctx:"), EditFSM.choosing_new_context)
async def fsm_edit_ctx(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    ctx_id = int(parts[1])
    data = await state.get_data()
    await update_activity_context(data["act_id"], callback.from_user.id, ctx_id)
    await state.clear()
    act = await get_activity_by_id(data["act_id"], callback.from_user.id)
    text = await _activity_text(act)
    await callback.message.edit_text(
        f"✅ Контекст обновлён!\n\n{text}",
        parse_mode="HTML",
        reply_markup=edit_menu_keyboard(data["act_id"]),
    )
    await callback.answer()


# — Delete —

@router.callback_query(F.data.startswith("edel:"))
async def cb_delete_confirm(callback: CallbackQuery):
    act_id = int(callback.data.split(":")[1])
    act = await get_activity_by_id(act_id, callback.from_user.id)
    if not act:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    text = await _activity_text(act)
    await callback.message.edit_text(
        f"🗑 <b>Удалить эту запись?</b>\n\n{text}",
        parse_mode="HTML",
        reply_markup=delete_confirm_keyboard(act_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("edel_ok:"))
async def cb_delete_ok(callback: CallbackQuery):
    act_id = int(callback.data.split(":")[1])
    await delete_activity(act_id, callback.from_user.id)
    activities = await get_recent_activities(callback.from_user.id, limit=10)
    if activities:
        await callback.message.edit_text(
            "✅ Удалено.\n\n✏️ <b>Выбери запись:</b>",
            parse_mode="HTML",
            reply_markup=activities_list_keyboard(activities),
        )
    else:
        await callback.message.edit_text("✅ Удалено. Записей больше нет.")
    await callback.answer()


# ── Context management ────────────────────────────────────────────────────────

async def _show_contexts(user_id: int, target, edit: bool = False):
    contexts = await get_user_contexts(user_id)
    text = "🏷 <b>Твои контексты:</b>" if contexts else "🏷 <b>Контекстов пока нет.</b>\n\nДобавь первый:"
    kb   = contexts_list_keyboard(contexts)
    if edit:
        await target.edit_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=kb)


@router.message(Command("contexts"))
async def cmd_contexts(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await state.clear()
    await _show_contexts(message.from_user.id, message)


@router.callback_query(F.data.startswith("cm:"))
async def cb_ctx_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    ctx_id = int(callback.data.split(":")[1])
    ctx = await get_context_by_id(ctx_id, callback.from_user.id)
    if not ctx:
        await callback.answer("Контекст не найден", show_alert=True)
        return
    count = await count_context_activities(ctx_id, callback.from_user.id)
    await callback.message.edit_text(
        f"{ctx[2]} <b>{ctx[1]}</b>\n📝 Записей: {count}",
        parse_mode="HTML",
        reply_markup=context_menu_keyboard(ctx_id),
    )
    await callback.answer()


@router.callback_query(F.data == "cm_back")
async def cb_ctx_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await _show_contexts(callback.from_user.id, callback.message, edit=True)
    await callback.answer()


# — Rename —

@router.callback_query(F.data.startswith("cm_ren:"))
async def cb_ctx_rename_start(callback: CallbackQuery, state: FSMContext):
    ctx_id = int(callback.data.split(":")[1])
    ctx    = await get_context_by_id(ctx_id, callback.from_user.id)
    await state.set_state(ContextFSM.waiting_new_name)
    await state.update_data(ctx_id=ctx_id)
    await callback.message.edit_text(
        f"✏️ Переименовать {ctx[2]} <b>{ctx[1]}</b>\n\nВведи новое название:",
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(ContextFSM.waiting_new_name)
async def fsm_ctx_rename(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 30:
        await message.answer("❌ Максимум 30 символов.")
        return
    data = await state.get_data()
    await rename_context(data["ctx_id"], message.from_user.id, name)
    await state.clear()
    ctx = await get_context_by_id(data["ctx_id"], message.from_user.id)
    count = await count_context_activities(data["ctx_id"], message.from_user.id)
    await message.answer(
        f"✅ Переименовано: {ctx[2]} <b>{ctx[1]}</b>\n📝 Записей: {count}",
        parse_mode="HTML",
        reply_markup=context_menu_keyboard(data["ctx_id"]),
    )


# — Color —

@router.callback_query(F.data.startswith("cm_col:"))
async def cb_ctx_color(callback: CallbackQuery):
    ctx_id = int(callback.data.split(":")[1])
    await callback.message.edit_text(
        "🎨 Выбери цвет:",
        reply_markup=color_picker_keyboard(ctx_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("cm_setcol:"))
async def cb_ctx_set_color(callback: CallbackQuery):
    parts = callback.data.split(":")
    ctx_id = int(parts[1])
    color  = parts[2]
    await update_context_color(ctx_id, callback.from_user.id, color)
    ctx = await get_context_by_id(ctx_id, callback.from_user.id)
    await callback.message.edit_text(
        f"✅ Цвет обновлён: {ctx[2]} <b>{ctx[1]}</b>",
        parse_mode="HTML",
        reply_markup=context_menu_keyboard(ctx_id),
    )
    await callback.answer()


# — Delete —

@router.callback_query(F.data.startswith("cm_del:"))
async def cb_ctx_delete_confirm(callback: CallbackQuery):
    ctx_id = int(callback.data.split(":")[1])
    ctx    = await get_context_by_id(ctx_id, callback.from_user.id)
    count  = await count_context_activities(ctx_id, callback.from_user.id)
    await callback.message.edit_text(
        f"🗑 Удалить {ctx[2]} <b>{ctx[1]}</b>?\n\n"
        f"⚠️ Вместе с ним удалится <b>{count} записей</b>.",
        parse_mode="HTML",
        reply_markup=ctx_delete_confirm_keyboard(ctx_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("cm_del_ok:"))
async def cb_ctx_delete_ok(callback: CallbackQuery):
    ctx_id = int(callback.data.split(":")[1])
    await delete_context(ctx_id, callback.from_user.id)
    await _show_contexts(callback.from_user.id, callback.message, edit=True)
    await callback.answer("✅ Контекст удалён")


# — Add new context —

@router.callback_query(F.data == "cm_add")
async def cb_ctx_add(callback: CallbackQuery, state: FSMContext):
    await state.set_state(ContextFSM.waiting_create_name)
    await callback.message.edit_text(
        "➕ <b>Новый контекст</b>\n\n"
        "Введи название, например: <i>Спорт, Работа, Учёба</i>",
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(ContextFSM.waiting_create_name)
async def fsm_ctx_create(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 30:
        await message.answer("❌ Максимум 30 символов.")
        return
    ctx_id, color = await get_or_create_context(message.from_user.id, name)
    await state.clear()
    await message.answer(
        f"✅ Контекст {color} <b>{name}</b> создан!",
        parse_mode="HTML",
        reply_markup=context_menu_keyboard(ctx_id),
    )


# ── Export ────────────────────────────────────────────────────────────────────

@router.message(Command("export"))
async def cmd_export(message: Message):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await message.answer("📤 Выбери период для экспорта:", reply_markup=export_keyboard())


@router.callback_query(F.data.startswith("exp:"))
async def cb_export(callback: CallbackQuery):
    period = callback.data.split(":")[1]
    user   = await get_user(callback.from_user.id)
    tz     = pytz.timezone(user[2])
    today  = datetime.now(tz).date()

    if period == "week":
        start = today - timedelta(days=today.weekday())
        end   = today
        fname = f"export_week_{start}.csv"
    elif period == "month":
        start = today.replace(day=1)
        end   = today
        fname = f"export_{today.strftime('%Y-%m')}.csv"
    else:
        start = today.replace(year=2020, month=1, day=1)
        end   = today
        fname = f"export_all_{today}.csv"

    activities = await get_export_activities(
        callback.from_user.id, start.isoformat(), end.isoformat()
    )

    if not activities:
        await callback.answer("Нет данных за этот период", show_alert=True)
        return

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Дата", "Час", "Контекст", "Описание", "Минут"])
    for act_date, hour, ctx, desc, dur in activities:
        writer.writerow([act_date, f"{hour:02d}:00", ctx, desc, dur])

    csv_bytes = BufferedInputFile(buf.getvalue().encode("utf-8-sig"), filename=fname)
    await callback.message.answer_document(
        document=csv_bytes,
        caption=f"📤 Экспорт: {fname}\n{len(activities)} записей",
    )
    await callback.answer()


# ── Goals ──────────────────────────────────────────────────────────────────────

@router.message(Command("goals"))
async def cmd_goals(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await state.clear()
    await _show_goals(message.from_user.id, message)


async def _show_goals(user_id: int, target, edit: bool = False):
    tz      = pytz.timezone((await get_user(user_id))[2])
    today   = datetime.now(tz).date()
    w_start = today - timedelta(days=today.weekday())
    w_end   = today

    progress = await get_goals_with_progress(user_id, w_start.isoformat(), w_end.isoformat())
    contexts = await get_user_contexts(user_id)
    goals_dict = {row[3]: row[2] for row in progress}

    lines = [f"🎯 <b>Цели на неделю</b>  ({w_start.strftime('%d.%m')}–{w_end.strftime('%d.%m')})\n"]

    if progress:
        for ctx_name, color, target_h, ctx_id, actual_m in progress:
            actual_h = actual_m / 60
            pct      = min(int(actual_h / target_h * 100), 100) if target_h else 0
            bar      = "█" * (pct // 10) + "░" * (10 - pct // 10)
            lines.append(
                f"{color} <b>{ctx_name}</b>\n"
                f"  {bar} {fmt_dur(actual_m)} / {target_h:.0f}ч  ({pct}%)\n"
            )
    else:
        lines.append("Целей пока нет.\n")

    lines.append("Нажми на контекст чтобы установить или изменить цель:")

    text = "\n".join(lines)
    kb   = goals_contexts_keyboard(contexts, goals_dict)

    if edit:
        await target.edit_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("gl:"))
async def cb_goal_select(callback: CallbackQuery, state: FSMContext):
    ctx_id = int(callback.data.split(":")[1])
    ctx    = await get_context_by_id(ctx_id, callback.from_user.id)
    if not ctx:
        await callback.answer("Контекст не найден", show_alert=True)
        return
    await state.set_state(GoalFSM.waiting_hours)
    await state.update_data(ctx_id=ctx_id)
    await callback.message.answer(
        f"🎯 Цель для {ctx[2]} <b>{ctx[1]}</b>\n\n"
        "Сколько часов в неделю хочешь тратить?\n"
        "Введи число, например <code>10</code> или <code>2.5</code>\n"
        "Введи <code>0</code> чтобы удалить цель.",
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "goals_back")
async def cb_goals_back(callback: CallbackQuery):
    await _show_goals(callback.from_user.id, callback.message, edit=True)
    await callback.answer()


@router.message(GoalFSM.waiting_hours)
async def fsm_goal_hours(message: Message, state: FSMContext):
    try:
        hours = float(message.text.strip().replace(",", "."))
    except ValueError:
        await message.answer("❌ Введи число, например <code>10</code> или <code>2.5</code>", parse_mode="HTML")
        return

    if hours < 0 or hours > 168:
        await message.answer("❌ Некорректное значение.")
        return

    data = await state.get_data()
    await state.clear()

    if hours == 0:
        await delete_goal(message.from_user.id, data["ctx_id"])
        await message.answer("✅ Цель удалена.")
    else:
        await set_goal(message.from_user.id, data["ctx_id"], hours)
        ctx = await get_context_by_id(data["ctx_id"], message.from_user.id)
        await message.answer(f"✅ Цель установлена: {ctx[2]} <b>{ctx[1]}</b> — {hours:.0f}ч/нед", parse_mode="HTML")

    await _show_goals(message.from_user.id, message)


# ── /cancel ───────────────────────────────────────────────────────────────────

@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Отменено.")


# ── Habits ───────────────────────────────────────────────────────────────────

def _parse_time(text: str) -> Optional[str]:
    """Parse '730', '7:30', '07:30' → '07:30'. Returns None if invalid."""
    text = text.strip().replace(".", ":").replace("-", ":")
    if ":" in text:
        parts = text.split(":")
    elif len(text) <= 2:
        parts = [text, "00"]
    elif len(text) == 3:
        parts = [text[0], text[1:]]
    elif len(text) == 4:
        parts = [text[:2], text[2:]]
    else:
        return None
    try:
        h, m = int(parts[0]), int(parts[1])
        if 0 <= h <= 23 and 0 <= m <= 59:
            return f"{h:02d}:{m:02d}"
    except (ValueError, IndexError):
        pass
    return None


async def _habits_screen(user_id: int, today_str: str, target, edit: bool = False):
    await ensure_default_habits(user_id)
    habits   = await get_habits(user_id)
    raw_logs = await get_habit_logs_today(user_id, today_str)
    # Keep only last log per habit
    logs = {}
    for habit_id, ts, te, tv in raw_logs:
        logs[habit_id] = (ts, te, tv)

    text = f"🗓 <b>Привычки — {today_str}</b>\n\nНажми на привычку чтобы записать:"
    kb   = habits_keyboard(habits, logs)
    if edit:
        await target.edit_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=kb)


@router.message(Command("habits"))
async def cmd_habits(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await state.clear()
    user     = await get_user(message.from_user.id)
    today    = datetime.now(pytz.timezone(user[2])).date().isoformat()
    await _habits_screen(message.from_user.id, today, message)


@router.callback_query(F.data == "hb_back")
async def cb_hb_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user  = await get_user(callback.from_user.id)
    today = datetime.now(pytz.timezone(user[2])).date().isoformat()
    await _habits_screen(callback.from_user.id, today, callback.message, edit=True)
    await callback.answer()


@router.callback_query(F.data.startswith("hb:"))
async def cb_hb_select(callback: CallbackQuery, state: FSMContext):
    habit_id = int(callback.data.split(":")[1])
    habit    = await get_habit_by_id(habit_id, callback.from_user.id)
    if not habit:
        await callback.answer("Привычка не найдена", show_alert=True)
        return

    hid, name, habit_type, emoji = habit
    user     = await get_user(callback.from_user.id)
    today    = datetime.now(pytz.timezone(user[2])).date().isoformat()
    raw_logs = await get_habit_logs_today(callback.from_user.id, today)
    logged   = any(r[0] == habit_id for r in raw_logs)

    await state.update_data(habit_id=habit_id, habit_type=habit_type, today=today)

    if habit_type == "travel":
        await state.set_state(HabitFSM.waiting_travel_from)
        await callback.message.edit_text(
            f"{emoji} <b>{name}</b>\n\n🚩 Откуда едешь?",
            parse_mode="HTML",
            reply_markup=habit_action_keyboard(habit_id, logged) if logged else None,
        )
        if logged:
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.edit_text(
                f"{emoji} <b>{name}</b>\n\n"
                "Уже записана поездка сегодня. Хочешь добавить ещё одну?\n\n"
                "🚩 Откуда едешь?",
                parse_mode="HTML",
            )
    else:
        await state.set_state(HabitFSM.waiting_time)
        hint = {
            "wake":  "⏰ Во сколько встал? Например: <code>7:30</code>",
            "sleep": "⏰ Во сколько лёг спать? Например: <code>23:00</code>",
            "meal":  "⏰ Во сколько поел? Например: <code>13:00</code>",
            "custom": "⏰ Во сколько? Например: <code>10:00</code>",
        }.get(habit_type, "⏰ Введи время:")

        await callback.message.edit_text(
            f"{emoji} <b>{name}</b>\n\n{hint}",
            parse_mode="HTML",
        )
    await callback.answer()


@router.message(HabitFSM.waiting_time)
async def fsm_habit_time(message: Message, state: FSMContext):
    t = _parse_time(message.text)
    if not t:
        await message.answer("❌ Не понял. Введи время, например: <code>7:30</code> или <code>730</code>", parse_mode="HTML")
        return
    data = await state.get_data()
    await log_habit(message.from_user.id, data["habit_id"], data["today"], time_start=t)
    await state.clear()
    habit = await get_habit_by_id(data["habit_id"], message.from_user.id)
    await message.answer(f"✅ {habit[3]} <b>{habit[1]}</b> — {t}", parse_mode="HTML")
    await _habits_screen(message.from_user.id, data["today"], message)


# — Travel FSM —

@router.message(HabitFSM.waiting_travel_from)
async def fsm_travel_from(message: Message, state: FSMContext):
    await state.update_data(travel_from=message.text.strip())
    await state.set_state(HabitFSM.waiting_travel_to)
    await message.answer("🏁 Куда едешь?")


@router.message(HabitFSM.waiting_travel_to)
async def fsm_travel_to(message: Message, state: FSMContext):
    await state.update_data(travel_to=message.text.strip())
    await state.set_state(HabitFSM.waiting_travel_dep)
    await message.answer("⏰ Время выезда? Например: <code>8:30</code>", parse_mode="HTML")


@router.message(HabitFSM.waiting_travel_dep)
async def fsm_travel_dep(message: Message, state: FSMContext):
    t = _parse_time(message.text)
    if not t:
        await message.answer("❌ Введи время, например: <code>8:30</code>", parse_mode="HTML")
        return
    await state.update_data(travel_dep=t)
    await state.set_state(HabitFSM.waiting_travel_arr)
    await message.answer("⏰ Время прибытия? Например: <code>9:15</code>", parse_mode="HTML")


@router.message(HabitFSM.waiting_travel_arr)
async def fsm_travel_arr(message: Message, state: FSMContext):
    t = _parse_time(message.text)
    if not t:
        await message.answer("❌ Введи время, например: <code>9:15</code>", parse_mode="HTML")
        return
    data = await state.get_data()
    frm  = data["travel_from"]
    to   = data["travel_to"]
    dep  = data["travel_dep"]
    tv   = f"{frm} → {to}"
    await log_habit(
        message.from_user.id, data["habit_id"], data["today"],
        time_start=dep, time_end=t, text_value=tv,
    )
    await state.clear()
    await message.answer(
        f"✅ 🚗 <b>Дорога записана</b>\n{frm} → {to}\n⏰ {dep} – {t}",
        parse_mode="HTML",
    )
    await _habits_screen(message.from_user.id, data["today"], message)


# — Delete log —

@router.callback_query(F.data.startswith("hb_del:"))
async def cb_hb_del(callback: CallbackQuery, state: FSMContext):
    habit_id = int(callback.data.split(":")[1])
    user     = await get_user(callback.from_user.id)
    today    = datetime.now(pytz.timezone(user[2])).date().isoformat()
    await delete_habit_log(callback.from_user.id, habit_id, today)
    await state.clear()
    await _habits_screen(callback.from_user.id, today, callback.message, edit=True)
    await callback.answer("🗑 Запись удалена")


# — New custom habit —

@router.callback_query(F.data == "hb_new")
async def cb_hb_new(callback: CallbackQuery, state: FSMContext):
    await state.set_state(HabitFSM.waiting_custom_name)
    await callback.message.edit_text(
        "➕ <b>Новая привычка</b>\n\nКак она называется?\n<i>Например: Медитация, Чтение, Прогулка</i>",
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(HabitFSM.waiting_custom_name)
async def fsm_habit_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) > 40:
        await message.answer("❌ Максимум 40 символов.")
        return
    await state.update_data(habit_name=name)
    await state.set_state(HabitFSM.waiting_custom_emoji)
    await message.answer(
        f"Отлично! Выбери эмодзи для <b>{name}</b>\n\n"
        "Просто отправь любой эмодзи, например: 🧘 📚 🏃 💊 💧",
        parse_mode="HTML",
    )


@router.message(HabitFSM.waiting_custom_emoji)
async def fsm_habit_emoji(message: Message, state: FSMContext):
    emoji = message.text.strip()
    data  = await state.get_data()
    await add_habit(message.from_user.id, data["habit_name"], emoji)
    await state.clear()
    user  = await get_user(message.from_user.id)
    today = datetime.now(pytz.timezone(user[2])).date().isoformat()
    await message.answer(f"✅ Привычка {emoji} <b>{data['habit_name']}</b> добавлена!", parse_mode="HTML")
    await _habits_screen(message.from_user.id, today, message)


# — Manage habits —

@router.callback_query(F.data == "hb_manage")
async def cb_hb_manage(callback: CallbackQuery):
    habits = await get_habits(callback.from_user.id)
    await callback.message.edit_text(
        "🗑 <b>Управление привычками</b>\n\nНажми 🗑 чтобы скрыть привычку:",
        parse_mode="HTML",
        reply_markup=habits_manage_keyboard(habits),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("hb_rm:"))
async def cb_hb_remove(callback: CallbackQuery):
    habit_id = int(callback.data.split(":")[1])
    await delete_habit(habit_id, callback.from_user.id)
    habits = await get_habits(callback.from_user.id)
    await callback.message.edit_reply_markup(reply_markup=habits_manage_keyboard(habits))
    await callback.answer("Скрыто")


# ── Places & People — activity attachment ────────────────────────────────────

async def _after_kb(act_id: int, date_str: str, hour: int, user_id: int) -> InlineKeyboardMarkup:
    """Build after_activity_keyboard with current place/people state."""
    place = await get_activity_place(act_id)
    people = await get_activity_people(act_id)
    place_name   = f"{place[2]} {place[1]}" if place else None
    people_names = [p[1] for p in people] if people else None
    return after_activity_keyboard(date_str, hour, act_id, place_name, people_names)


@router.callback_query(F.data.startswith("ap_place:"))
async def cb_ap_place(callback: CallbackQuery, state: FSMContext):
    act_id = int(callback.data.split(":")[1])
    places = await get_places(callback.from_user.id)
    if not places:
        await state.set_state(PlaceFSM.waiting_name)
        await state.update_data(act_id=act_id, from_activity=True)
        await callback.message.answer(
            "📍 У тебя пока нет мест. Введи название первого:\n<i>Дом, Офис, Кафе...</i>",
            parse_mode="HTML",
        )
    else:
        await callback.message.answer(
            "📍 Выбери место:",
            reply_markup=place_picker_keyboard(places, act_id),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("ap_setplace:"))
async def cb_ap_setplace(callback: CallbackQuery, state: FSMContext):
    parts    = callback.data.split(":")
    act_id   = int(parts[1])
    place_id = int(parts[2])
    await set_activity_place(act_id, callback.from_user.id, place_id)
    data = await state.get_data()
    date_str = data.get("date_str", "")
    hour     = data.get("hour", 0)
    place    = await get_activity_place(act_id)
    place_name = f"{place[2]} {place[1]}" if place else None
    people     = await get_activity_people(act_id)
    people_names = [p[1] for p in people] if people else None
    await callback.message.edit_text(
        f"📍 Место: <b>{place_name}</b>",
        parse_mode="HTML",
        reply_markup=after_activity_keyboard(date_str, hour, act_id, place_name, people_names),
    )
    await callback.answer("✅ Место сохранено")


@router.callback_query(F.data.startswith("ap_newplace:"))
async def cb_ap_newplace(callback: CallbackQuery, state: FSMContext):
    act_id = int(callback.data.split(":")[1])
    await state.set_state(PlaceFSM.waiting_name)
    await state.update_data(act_id=act_id, from_activity=True)
    await callback.message.edit_text(
        "📍 Введи название нового места:\n<i>Дом, Офис, Кафе, Спортзал...</i>",
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ap_person:"))
async def cb_ap_person(callback: CallbackQuery, state: FSMContext):
    act_id  = int(callback.data.split(":")[1])
    people  = await get_people(callback.from_user.id)
    current = await get_activity_people(act_id)
    selected = [p[0] for p in current]
    if not people:
        await state.set_state(PersonFSM.waiting_name)
        await state.update_data(act_id=act_id, from_activity=True)
        await callback.message.answer(
            "👤 У тебя пока нет людей. Введи имя первого:\n<i>Маша, Коллега Вася...</i>",
            parse_mode="HTML",
        )
    else:
        await state.set_state(PeoplePickFSM.selecting)
        await state.update_data(act_id=act_id, selected_people=selected)
        await callback.message.answer(
            "👥 Кто был рядом? (можно выбрать нескольких)",
            reply_markup=people_picker_keyboard(people, act_id, selected),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("ap_tp:"), PeoplePickFSM.selecting)
async def cb_ap_toggle_person(callback: CallbackQuery, state: FSMContext):
    parts     = callback.data.split(":")
    act_id    = int(parts[1])
    person_id = int(parts[2])
    data      = await state.get_data()
    selected  = data.get("selected_people", [])
    selected  = [p for p in selected if p != person_id] if person_id in selected else selected + [person_id]
    await state.update_data(selected_people=selected)
    people = await get_people(callback.from_user.id)
    await callback.message.edit_reply_markup(
        reply_markup=people_picker_keyboard(people, act_id, selected)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ap_sp:"), PeoplePickFSM.selecting)
async def cb_ap_save_people(callback: CallbackQuery, state: FSMContext):
    act_id   = int(callback.data.split(":")[1])
    data     = await state.get_data()
    selected = data.get("selected_people", [])
    await set_activity_people(act_id, selected)
    await state.clear()
    date_str = data.get("date_str", "")
    hour     = data.get("hour", 0)
    place    = await get_activity_place(act_id)
    people   = await get_activity_people(act_id)
    place_name   = f"{place[2]} {place[1]}" if place else None
    people_names = [p[1] for p in people] if people else None
    names_str    = ", ".join(people_names) if people_names else ""
    await callback.message.edit_text(
        f"👥 Люди: <b>{names_str}</b>",
        parse_mode="HTML",
        reply_markup=after_activity_keyboard(date_str, hour, act_id, place_name, people_names),
    )
    await callback.answer("✅ Сохранено")


@router.callback_query(F.data.startswith("ap_np:"))
async def cb_ap_new_person(callback: CallbackQuery, state: FSMContext):
    act_id = int(callback.data.split(":")[1])
    await state.set_state(PersonFSM.waiting_name)
    await state.update_data(act_id=act_id, from_activity=True)
    await callback.message.edit_text(
        "👤 Введи имя нового человека:",
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ap_back:"))
async def cb_ap_back(callback: CallbackQuery, state: FSMContext):
    """Skip place/people — go back to after-activity menu."""
    act_id = int(callback.data.split(":")[1])
    data   = await state.get_data()
    await state.clear()
    date_str = data.get("date_str", "")
    hour     = data.get("hour", 0)
    kb = await _after_kb(act_id, date_str, hour, callback.from_user.id)
    await callback.message.edit_reply_markup(reply_markup=kb)
    await callback.answer()


# ── Places FSM (new place from activity or /places) ───────────────────────────

@router.message(PlaceFSM.waiting_name)
async def fsm_place_name(message: Message, state: FSMContext):
    name = message.text.strip()[:40]
    await state.update_data(place_name=name)
    await state.set_state(PlaceFSM.waiting_emoji)
    await message.answer(
        f"Отлично! Выбери эмодзи для <b>{name}</b>\n\n"
        "Отправь любой эмодзи, например: 🏠 🏢 ☕ 🏋️ 🏫 🌳",
        parse_mode="HTML",
    )


@router.message(PlaceFSM.waiting_emoji)
async def fsm_place_emoji(message: Message, state: FSMContext):
    emoji  = message.text.strip()
    data   = await state.get_data()
    place_id = await add_place(message.from_user.id, data["place_name"], emoji)
    act_id = data.get("act_id")
    if act_id:
        await set_activity_place(act_id, message.from_user.id, place_id)
        await state.clear()
        place_name = f"{emoji} {data['place_name']}"
        people     = await get_activity_people(act_id)
        people_names = [p[1] for p in people] if people else None
        date_str   = data.get("date_str", "")
        hour       = data.get("hour", 0)
        await message.answer(
            f"✅ Место <b>{place_name}</b> создано и сохранено!",
            parse_mode="HTML",
            reply_markup=after_activity_keyboard(date_str, hour, act_id, place_name, people_names),
        )
    else:
        await state.clear()
        await message.answer(
            f"✅ Место {emoji} <b>{data['place_name']}</b> добавлено!",
            parse_mode="HTML",
            reply_markup=places_list_keyboard(await get_places(message.from_user.id)),
        )


# ── People FSM (new person from activity or /people) ──────────────────────────

@router.message(PersonFSM.waiting_name)
async def fsm_person_name(message: Message, state: FSMContext):
    name      = message.text.strip()[:40]
    person_id = await add_person(message.from_user.id, name)
    data      = await state.get_data()
    act_id    = data.get("act_id")
    await state.clear()
    if act_id:
        people   = await get_people(message.from_user.id)
        current  = await get_activity_people(act_id)
        selected = [p[0] for p in current] + [person_id]
        await set_activity_people(act_id, selected)
        date_str = data.get("date_str", "")
        hour     = data.get("hour", 0)
        place    = await get_activity_place(act_id)
        all_people = await get_activity_people(act_id)
        place_name   = f"{place[2]} {place[1]}" if place else None
        people_names = [p[1] for p in all_people]
        await message.answer(
            f"✅ <b>{name}</b> добавлен и отмечен!",
            parse_mode="HTML",
            reply_markup=after_activity_keyboard(date_str, hour, act_id, place_name, people_names),
        )
    else:
        await message.answer(
            f"✅ <b>{name}</b> добавлен в список!",
            parse_mode="HTML",
            reply_markup=people_list_keyboard(await get_people(message.from_user.id)),
        )


# ── /places — management ──────────────────────────────────────────────────────

@router.message(Command("places"))
async def cmd_places(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await state.clear()
    places = await get_places(message.from_user.id)
    text = "📍 <b>Твои места:</b>" if places else "📍 <b>Мест пока нет.</b>\n\nДобавь первое:"
    await message.answer(text, parse_mode="HTML", reply_markup=places_list_keyboard(places))


@router.callback_query(F.data == "pl_add")
async def cb_pl_add(callback: CallbackQuery, state: FSMContext):
    await state.set_state(PlaceFSM.waiting_name)
    await state.update_data(act_id=None, from_activity=False)
    await callback.message.edit_text(
        "📍 Введи название нового места:\n<i>Дом, Офис, Кафе...</i>",
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("pl_del:"))
async def cb_pl_del(callback: CallbackQuery):
    place_id = int(callback.data.split(":")[1])
    await delete_place(place_id, callback.from_user.id)
    places = await get_places(callback.from_user.id)
    text   = "📍 <b>Твои места:</b>" if places else "📍 <b>Мест пока нет.</b>"
    await callback.message.edit_text(
        text, parse_mode="HTML",
        reply_markup=places_list_keyboard(places),
    )
    await callback.answer("🗑 Удалено")


# ── /people — management ──────────────────────────────────────────────────────

@router.message(Command("people"))
async def cmd_people(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await state.clear()
    people = await get_people(message.from_user.id)
    text = "👥 <b>Твои люди:</b>" if people else "👥 <b>Людей пока нет.</b>\n\nДобавь первого:"
    await message.answer(text, parse_mode="HTML", reply_markup=people_list_keyboard(people))


@router.callback_query(F.data == "pp_add")
async def cb_pp_add(callback: CallbackQuery, state: FSMContext):
    await state.set_state(PersonFSM.waiting_name)
    await state.update_data(act_id=None)
    await callback.message.edit_text("👤 Введи имя:")
    await callback.answer()


@router.callback_query(F.data.startswith("pp_del:"))
async def cb_pp_del(callback: CallbackQuery):
    person_id = int(callback.data.split(":")[1])
    await delete_person(person_id, callback.from_user.id)
    people = await get_people(callback.from_user.id)
    text   = "👥 <b>Твои люди:</b>" if people else "👥 <b>Людей пока нет.</b>"
    await callback.message.edit_text(
        text, parse_mode="HTML",
        reply_markup=people_list_keyboard(people),
    )
    await callback.answer("🗑 Удалено")


# ── Main menu button handlers ────────────────────────────────────────────────

@router.message(F.text == "➕ Добавить")
async def menu_add(message: Message, state: FSMContext):
    await cmd_add(message, state)

@router.message(F.text == "⚡ Быстрые")
async def menu_quick(message: Message):
    await cmd_quick(message)

@router.message(F.text == "📊 Статистика")
async def menu_stats(message: Message):
    await cmd_stats(message)

@router.message(F.text == "🎯 Цели")
async def menu_goals(message: Message, state: FSMContext):
    await cmd_goals(message, state)

@router.message(F.text == "🏆 Топ")
async def menu_top(message: Message):
    await cmd_top(message)

@router.message(F.text == "🕐 Паттерны")
async def menu_patterns(message: Message):
    await cmd_patterns(message)

@router.message(F.text == "✏️ Редактировать")
async def menu_edit(message: Message, state: FSMContext):
    await cmd_edit(message, state)

@router.message(F.text == "📝 Заметка")
async def menu_note(message: Message, state: FSMContext):
    await cmd_note(message, state)

@router.message(F.text == "📤 Экспорт")
async def menu_export(message: Message):
    await cmd_export(message)

@router.message(F.text == "🗓 Привычки")
async def menu_habits(message: Message, state: FSMContext):
    await cmd_habits(message, state)

@router.message(F.text == "🌐 Пространство")
async def menu_space(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🌐 <b>Пространство</b>", parse_mode="HTML", reply_markup=space_menu_keyboard())

@router.callback_query(F.data == "space:contexts")
async def cb_space_contexts(callback: CallbackQuery, state: FSMContext):
    await _show_contexts(callback.from_user.id, callback.message, edit=True)
    await callback.answer()

@router.callback_query(F.data == "space:places")
async def cb_space_places(callback: CallbackQuery, state: FSMContext):
    places = await get_places(callback.from_user.id)
    text = "📍 <b>Твои места:</b>" if places else "📍 <b>Мест пока нет.</b>"
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=places_list_keyboard(places))
    await callback.answer()

@router.callback_query(F.data == "space:people")
async def cb_space_people(callback: CallbackQuery, state: FSMContext):
    people = await get_people(callback.from_user.id)
    text = "👥 <b>Твои люди:</b>" if people else "👥 <b>Людей пока нет.</b>"
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=people_list_keyboard(people))
    await callback.answer()

@router.callback_query(F.data == "space_back")
async def cb_space_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("🌐 <b>Пространство</b>", parse_mode="HTML", reply_markup=space_menu_keyboard())
    await callback.answer()

@router.message(F.text == "⏰ Расписание")
async def menu_schedule(message: Message):
    await cmd_schedule(message)

@router.message(F.text == "⚙️ Настройки")
async def menu_settings(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "⚙️ <b>Настройки</b>",
        parse_mode="HTML",
        reply_markup=settings_keyboard(),
    )

@router.callback_query(F.data == "set_tz")
async def cb_set_tz(callback: CallbackQuery, state: FSMContext):
    await state.set_state(Registration.choosing_timezone)
    await callback.message.edit_text(
        "🌍 Выбери часовой пояс:",
        reply_markup=timezone_keyboard(),
    )
    await callback.answer()

@router.callback_query(F.data == "set_back")
async def cb_set_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        "⚙️ <b>Настройки</b>",
        parse_mode="HTML",
        reply_markup=settings_keyboard(),
    )
    await callback.answer()

# ── Snapshots ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "set_snap")
async def cb_set_snap(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SnapshotFSM.waiting_name)
    await callback.message.edit_text(
        "📸 <b>Сохранить снапшот</b>\n\n"
        "Введи название снапшота, например:\n"
        "<i>Март 2026</i>  или  <i>Перед перезапуском</i>",
        parse_mode="HTML",
    )
    await callback.answer()

@router.message(SnapshotFSM.waiting_name)
async def fsm_snapshot_name(message: Message, state: FSMContext):
    name = message.text.strip()[:50]
    snap_id = await create_snapshot(message.from_user.id, name)
    await state.clear()
    await message.answer(
        f"✅ Снапшот <b>«{name}»</b> сохранён!\n\n"
        "Найти его можно в ⚙️ Настройки → 📂 Мои снапшоты.",
        parse_mode="HTML",
        reply_markup=settings_keyboard(),
    )

@router.callback_query(F.data == "set_snaps_list")
async def cb_snaps_list(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    snaps = await get_snapshots(callback.from_user.id)
    if not snaps:
        await callback.message.edit_text(
            "📂 Снапшотов пока нет.\n\nСохрани первый через 📸 Сохранить снапшот.",
            reply_markup=settings_keyboard(),
        )
    else:
        await callback.message.edit_text(
            "📂 <b>Мои снапшоты</b>\n\nВыбери снапшот:",
            parse_mode="HTML",
            reply_markup=snapshots_keyboard(snaps),
        )
    await callback.answer()

@router.callback_query(F.data.startswith("snap_view:"))
async def cb_snap_view(callback: CallbackQuery):
    snap_id = int(callback.data.split(":")[1])
    snaps   = await get_snapshots(callback.from_user.id)
    snap    = next((s for s in snaps if s[0] == snap_id), None)
    if not snap:
        await callback.answer("Снапшот не найден", show_alert=True)
        return
    _, name, date, cnt = snap
    await callback.message.edit_text(
        f"📸 <b>{name}</b>\n\n"
        f"📅 Дата: {date}\n"
        f"📝 Записей: {cnt}\n\n"
        "Что хочешь сделать?",
        parse_mode="HTML",
        reply_markup=snapshot_actions_keyboard(snap_id),
    )
    await callback.answer()

@router.callback_query(F.data.startswith("snap_restore:"))
async def cb_snap_restore(callback: CallbackQuery):
    snap_id = int(callback.data.split(":")[1])
    snaps   = await get_snapshots(callback.from_user.id)
    snap    = next((s for s in snaps if s[0] == snap_id), None)
    name    = snap[1] if snap else "?"
    await callback.message.edit_text(
        f"♻️ <b>Восстановить снапшот «{name}»?</b>\n\n"
        "⚠️ Все текущие данные будут заменены данными из снапшота.\n"
        "Текущие данные не сохранятся — сохрани их сначала если нужно.",
        parse_mode="HTML",
        reply_markup=snap_restore_confirm_keyboard(snap_id),
    )
    await callback.answer()

@router.callback_query(F.data.startswith("snap_restore_ok:"))
async def cb_snap_restore_ok(callback: CallbackQuery):
    snap_id = int(callback.data.split(":")[1])
    ok = await restore_snapshot(callback.from_user.id, snap_id)
    if ok:
        await callback.message.edit_text(
            "✅ <b>Снапшот восстановлен!</b>\n\n"
            "Все данные возвращены к сохранённому состоянию.",
            parse_mode="HTML",
            reply_markup=settings_keyboard(),
        )
    else:
        await callback.answer("Снапшот не найден", show_alert=True)
    await callback.answer()

@router.callback_query(F.data.startswith("snap_del:"))
async def cb_snap_del(callback: CallbackQuery):
    snap_id = int(callback.data.split(":")[1])
    await delete_snapshot(callback.from_user.id, snap_id)
    snaps = await get_snapshots(callback.from_user.id)
    if snaps:
        await callback.message.edit_text(
            "🗑 Удалено.\n\n📂 <b>Мои снапшоты:</b>",
            parse_mode="HTML",
            reply_markup=snapshots_keyboard(snaps),
        )
    else:
        await callback.message.edit_text(
            "🗑 Удалено. Снапшотов больше нет.",
            reply_markup=settings_keyboard(),
        )
    await callback.answer()

# ── Reset ─────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "set_reset")
async def cb_set_reset(callback: CallbackQuery):
    await callback.message.edit_text(
        "🔄 <b>Начать сначала</b>\n\n"
        "Все текущие записи, контексты, цели и привычки будут удалены.\n\n"
        "Рекомендуем сначала сохранить снапшот — потом сможешь восстановить данные.",
        parse_mode="HTML",
        reply_markup=reset_confirm_keyboard(),
    )
    await callback.answer()

@router.callback_query(F.data == "reset_with_snap")
async def cb_reset_with_snap(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SnapshotFSM.waiting_reset_name)
    await callback.message.edit_text(
        "📸 Введи название снапшота перед сбросом:\n"
        "<i>Например: До сброса март 2026</i>",
        parse_mode="HTML",
    )
    await callback.answer()

@router.message(SnapshotFSM.waiting_reset_name)
async def fsm_reset_with_snap(message: Message, state: FSMContext):
    name = message.text.strip()[:50]
    await create_snapshot(message.from_user.id, name)
    await reset_user_data(message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Снапшот <b>«{name}»</b> сохранён.\n\n"
        "🔄 Данные очищены. Можешь начинать заново!\n\n"
        "Восстановить данные: ⚙️ Настройки → 📂 Мои снапшоты",
        parse_mode="HTML",
        reply_markup=settings_keyboard(),
    )

@router.callback_query(F.data == "reset_no_snap")
async def cb_reset_no_snap(callback: CallbackQuery, state: FSMContext):
    await reset_user_data(callback.from_user.id)
    await state.clear()
    await callback.message.edit_text(
        "🔄 <b>Данные очищены.</b>\n\nМожешь начинать заново!",
        parse_mode="HTML",
        reply_markup=settings_keyboard(),
    )
    await callback.answer()


# ── Top activities ────────────────────────────────────────────────────────────

@router.message(Command("top"))
async def cmd_top(message: Message):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    rows = await get_top_activities(message.from_user.id, limit=10)
    if not rows:
        await message.answer("Нет данных.")
        return
    lines = ["🏆 <b>Топ активностей</b>\n"]
    for i, (desc, ctx_name, color, cnt, total_m) in enumerate(rows, 1):
        lines.append(
            f"{i}. {color} <b>{desc}</b>\n"
            f"   {ctx_name} · {cnt} раз · {fmt_dur(total_m)} всего\n"
        )
    await message.answer("\n".join(lines), parse_mode="HTML")


# ── Time-of-day patterns ──────────────────────────────────────────────────────

@router.message(Command("patterns"))
async def cmd_patterns(message: Message):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    rows = await get_hour_patterns(message.from_user.id)
    if not rows:
        await message.answer("Нет данных.")
        return

    # For each hour keep only the top context (already sorted by total_m desc within hour)
    hours_top: dict = {}
    for hour, ctx_name, color, total_m in rows:
        if hour not in hours_top:
            hours_top[hour] = (ctx_name, color, total_m)

    lines = ["🕐 <b>Паттерны по времени суток</b>\n"]
    for hour in sorted(hours_top):
        ctx_name, color, total_m = hours_top[hour]
        bar = "█" * min(int(total_m / 60 * 2), 12)
        lines.append(f"{hour:02d}:00  {color} {ctx_name:<14} {bar} {fmt_dur(total_m)}")

    await message.answer("\n".join(lines), parse_mode="HTML")


# ── Quick activities (templates) ──────────────────────────────────────────────

@router.message(Command("quick"))
async def cmd_quick(message: Message):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    templates = await get_templates(message.from_user.id)
    if not templates:
        await message.answer(
            "У тебя пока нет шаблонов быстрых дел.\n\n"
            "После сохранения любого дела нажми <b>💾 Шаблон</b> — "
            "и оно появится здесь.",
            parse_mode="HTML",
        )
        return
    await message.answer(
        "⚡ <b>Быстрые дела</b>\n\nВыбери шаблон — запись добавится за текущий час:",
        parse_mode="HTML",
        reply_markup=templates_keyboard(templates),
    )


@router.callback_query(F.data.startswith("qt:"))
async def cb_quick_use(callback: CallbackQuery):
    tmpl_id  = int(callback.data.split(":")[1])
    tmpl     = await get_template_by_id(tmpl_id, callback.from_user.id)
    if not tmpl:
        await callback.answer("Шаблон не найден", show_alert=True)
        return

    _, ctx_id, ctx_name, color, desc, dur = tmpl
    user     = await get_user(callback.from_user.id)
    tz       = pytz.timezone(user[2])
    now      = datetime.now(tz)
    date_str = now.date().isoformat()
    hour     = now.hour

    act_id = await add_activity(
        user_id=callback.from_user.id,
        context_id=ctx_id,
        description=desc,
        duration_minutes=dur,
        activity_date=date_str,
        hour_slot=hour,
    )
    await increment_template_use(tmpl_id, callback.from_user.id)

    await callback.message.edit_text(
        f"✅ Записано!\n\n"
        f"{color} {ctx_name}  ·  {desc}  ·  {fmt_dur(dur)}\n"
        f"📅 {date_str}  🕐 {hour:02d}:00",
        reply_markup=after_activity_keyboard(date_str, hour, act_id),
    )
    await callback.answer()


@router.callback_query(F.data == "qt_manage")
async def cb_quick_manage(callback: CallbackQuery):
    templates = await get_templates(callback.from_user.id)
    if not templates:
        await callback.message.edit_text("Шаблонов нет.")
        await callback.answer()
        return
    await callback.message.edit_text(
        "🗑 <b>Управление шаблонами</b>\n\nНажми 🗑 рядом с шаблоном чтобы удалить:",
        parse_mode="HTML",
        reply_markup=templates_keyboard(templates, manage=True),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("qt_del:"))
async def cb_quick_delete(callback: CallbackQuery):
    tmpl_id = int(callback.data.split(":")[1])
    await delete_template(tmpl_id, callback.from_user.id)
    templates = await get_templates(callback.from_user.id)
    if templates:
        await callback.message.edit_text(
            "✅ Удалено.\n\n🗑 <b>Управление шаблонами:</b>",
            parse_mode="HTML",
            reply_markup=templates_keyboard(templates, manage=True),
        )
    else:
        await callback.message.edit_text("✅ Шаблон удалён. Шаблонов больше нет.")
    await callback.answer()


@router.callback_query(F.data == "qt_back")
async def cb_quick_back(callback: CallbackQuery):
    templates = await get_templates(callback.from_user.id)
    if not templates:
        await callback.message.edit_text("Шаблонов нет.")
        await callback.answer()
        return
    await callback.message.edit_text(
        "⚡ <b>Быстрые дела</b>\n\nВыбери шаблон — запись добавится за текущий час:",
        parse_mode="HTML",
        reply_markup=templates_keyboard(templates),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("act_tmpl:"))
async def cb_save_template(callback: CallbackQuery):
    act_id = int(callback.data.split(":")[1])
    act    = await get_activity_by_id(act_id, callback.from_user.id)
    if not act:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    # act: (id, date, hour, ctx_name, color, desc, dur)
    # Need context_id — look it up via context name
    contexts = await get_user_contexts(callback.from_user.id)
    ctx_match = next((c for c in contexts if c[1] == act[3]), None)
    if not ctx_match:
        await callback.answer("Контекст не найден", show_alert=True)
        return
    await add_template(callback.from_user.id, ctx_match[0], act[5], act[6])
    await callback.answer("💾 Шаблон сохранён!", show_alert=False)


# ── Voice input ───────────────────────────────────────────────────────────────

@router.message(F.voice)
async def handle_voice(message: Message, state: FSMContext, bot: Bot):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return

    await message.answer("🎤 Распознаю голос…")

    buf = BytesIO()
    await bot.download(message.voice, destination=buf)
    text = await _transcribe_voice(buf.getvalue())

    if not text:
        await message.answer(
            "❌ Не удалось распознать голос.\n"
            "Используй /add чтобы добавить дело вручную."
        )
        return

    # Try to extract duration from the transcription
    duration = parse_duration(text)
    user     = await get_user(message.from_user.id)
    tz       = pytz.timezone(user[2])
    now      = datetime.now(tz)
    date_str = now.date().isoformat()
    hour     = now.hour

    await message.answer(f"📝 Распознано: <i>{text}</i>", parse_mode="HTML")
    await state.update_data(date_str=date_str, hour=hour, description=text)

    if duration and duration <= 600:
        # Duration found in transcription — skip to context
        await state.update_data(duration=duration)
        await state.set_state(ActivityFSM.choosing_context)
        contexts = await get_user_contexts(message.from_user.id)
        await message.answer(
            f"⏱ Длительность: <b>{fmt_dur(duration)}</b>\n\n🏷 Выбери контекст:",
            parse_mode="HTML",
            reply_markup=contexts_keyboard(contexts, date_str, hour),
        )
    else:
        await state.set_state(ActivityFSM.waiting_duration)
        await message.answer(
            "⏱ Сколько времени это заняло?\n\n"
            "Примеры: <code>30</code>  <code>45 мин</code>  <code>1ч</code>  <code>1ч 30мин</code>",
            parse_mode="HTML",
        )


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
        streak = await get_streak(callback.from_user.id, today.isoformat())
        streak_str = f"  🔥 {streak} дн подряд" if streak >= 2 else ""
        title = today.strftime("📅 %d %B %Y") + streak_str
    elif period == "week":
        start = today - timedelta(days=today.weekday())
        end = today
        title = f"📆 Неделя ({start.strftime('%d.%m')}–{end.strftime('%d.%m.%Y')})"
    elif period == "month":
        start = today.replace(day=1)
        end = today
        title = today.strftime("📊 %B %Y")
    elif period == "compare":
        w1_end   = today
        w1_start = today - timedelta(days=today.weekday())
        w2_end   = w1_start - timedelta(days=1)
        w2_start = w2_end - timedelta(days=6)
        rows = await get_week_comparison(
            callback.from_user.id,
            w1_start.isoformat(), w1_end.isoformat(),
            w2_start.isoformat(), w2_end.isoformat(),
        )
        w1_label = f"{w1_start.strftime('%d.%m')}–{w1_end.strftime('%d.%m')}"
        w2_label = f"{w2_start.strftime('%d.%m')}–{w2_end.strftime('%d.%m')}"
        lines = [f"↔️ <b>Сравнение недель</b>\n<i>эта ({w1_label}) vs прошлая ({w2_label})</i>\n"]
        if not rows:
            lines.append("😔 Нет данных.")
        else:
            for ctx_name, color, w1_m, w2_m in rows:
                diff   = w1_m - w2_m
                arrow  = "▲" if diff > 0 else ("▼" if diff < 0 else "=")
                d_str  = f"{arrow} {fmt_dur(abs(diff))}" if diff != 0 else "= без изменений"
                lines.append(
                    f"{color} <b>{ctx_name}</b>\n"
                    f"  {fmt_dur(w1_m)} vs {fmt_dur(w2_m)}  {d_str}\n"
                )
        try:
            await callback.message.edit_text(
                "\n".join(lines), parse_mode="HTML", reply_markup=stats_keyboard()
            )
        except Exception:
            pass
        await callback.answer()
        return

    elif period in ("grid_week", "grid_month"):
        if period == "grid_week":
            start = today - timedelta(days=today.weekday())
            end = today
            title = f"Неделя {start.strftime('%d.%m')}–{end.strftime('%d.%m.%Y')}"
        else:
            start = today.replace(day=1)
            end = today
            title = today.strftime("%B %Y")

        await callback.answer("⏳ Генерирую...")
        activities = await get_activities_for_period(
            callback.from_user.id, start.isoformat(), end.isoformat()
        )
        if not activities:
            await callback.message.answer("😔 Нет данных за этот период.")
        else:
            image = await generate_grid(callback.from_user.id, start, end, title)
            doc = BufferedInputFile(image.read(), filename="grid.png")
            await callback.message.answer_document(document=doc, caption=f"🗓 {title}")
        return

    elif period == "dynamics":
        await callback.answer("⏳ Генерирую...")
        image = await generate_dynamics(callback.from_user.id)
        if not image:
            await callback.message.answer("😔 Недостаточно данных для графика (нужно хотя бы 2 недели).")
        else:
            doc = BufferedInputFile(image.read(), filename="dynamics.png")
            await callback.message.answer_document(document=doc, caption="📈 Динамика по неделям")
        return

    else:
        return

    activities = await get_activities_for_period(
        callback.from_user.id, start.isoformat(), end.isoformat()
    )

    if not activities:
        text = f"{title}\n\n😔 Нет данных за этот период."
    else:
        text = _format_stats(activities, period, title)
        if period == "day":
            note = await get_day_note(callback.from_user.id, today.isoformat())
            if note:
                text += f"\n\n📝 <b>Заметка:</b>\n{note}"
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

@router.message(Command("schedule"))
async def cmd_schedule(message: Message):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    hours = await get_notification_hours(message.from_user.id)
    await message.answer(
        "⏰ <b>Настройка расписания</b>\n\n"
        "Выбери в какие часы получать напоминания.\n"
        "Каждое напоминание спрашивает о <b>предыдущем</b> часе.\n"
        "Например: ✅ 12 → спросит «что делал с 11 до 12?»\n\n"
        "Нажми на час чтобы включить/выключить:",
        parse_mode="HTML",
        reply_markup=schedule_keyboard(hours),
    )


@router.callback_query(F.data.startswith("sched:"))
async def cb_toggle_hour(callback: CallbackQuery):
    hour = int(callback.data.split(":")[1])
    updated_hours = await toggle_notification_hour(callback.from_user.id, hour)
    await callback.message.edit_reply_markup(
        reply_markup=schedule_keyboard(updated_hours)
    )
    status = "включён ✅" if hour in updated_hours else "выключен ☐"
    await callback.answer(f"{hour}:00 {status}")


@router.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Твой Telegram ID: `{message.from_user.id}`", parse_mode="Markdown")


# ── Admin ─────────────────────────────────────────────────────────────────────

def admin_only(func):
    """Decorator: blocks non-admins."""
    @functools.wraps(func)
    async def wrapper(message: Message, **kwargs):
        if message.from_user.id not in ADMIN_IDS:
            await message.answer("⛔ Нет доступа.")
            return
        return await func(message, **kwargs)
    return wrapper


@router.message(Command("admin"))
@admin_only
async def cmd_admin(message: Message):
    users = await get_all_users_stats()
    if not users:
        await message.answer("Пользователей нет.")
        return

    lines = ["<b>👥 Все пользователи</b>\n"]
    for uid, username, tz, reg_date, act_count, last_act in users:
        name = f"@{username}" if username else f"id{uid}"
        reg = reg_date[:10] if reg_date else "—"
        last = last_act[:10] if last_act else "никогда"
        lines.append(
            f"<b>{name}</b>  (id: <code>{uid}</code>)\n"
            f"  🌍 {tz}\n"
            f"  📅 Регистрация: {reg}\n"
            f"  📝 Записей: {act_count}  |  Последняя: {last}\n"
            f"  /user_{uid}\n"
        )

    lines.append(f"\n<b>Всего пользователей: {len(users)}</b>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(F.text.regexp(r"^/user_\d+$"))
@admin_only
async def cmd_admin_user(message: Message):
    try:
        target_id = int(message.text.split("_")[1])
    except (IndexError, ValueError):
        await message.answer("Неверный формат. Используй /user_123456")
        return

    user = await get_user(target_id)
    if not user:
        await message.answer("Пользователь не найден.")
        return

    activities = await get_user_full_stats(target_id)
    name = f"@{user[1]}" if user[1] else f"id{target_id}"

    lines = [f"<b>📊 Активность: {name}</b>\n"]

    if not activities:
        lines.append("Нет записей.")
    else:
        # Group by date
        days: dict = {}
        for act_date, hour, ctx, color, desc, dur in activities:
            days.setdefault(act_date, []).append((hour, ctx, color, desc, dur))

        for day in sorted(days.keys(), reverse=True):
            lines.append(f"<b>📅 {day}</b>")
            for hour, ctx, color, desc, dur in sorted(days[day]):
                lines.append(f"  {color} {ctx}  ·  {desc}  ·  {fmt_dur(dur)}")
            lines.append("")

        total_min = sum(a[5] for a in activities)
        lines.append(f"<b>Всего за последние 50 записей: {fmt_dur(total_min)}</b>")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3900] + "\n\n…(показано частично)"
    await message.answer(text, parse_mode="HTML")


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(WELCOME_TEXT, parse_mode="HTML")

