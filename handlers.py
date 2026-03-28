import re
import logging
import functools
from datetime import datetime, timedelta
from typing import Optional, List, Tuple

import pytz
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext

from config import TIMEZONES, NOTIFY_HOURS_START, NOTIFY_HOURS_END, ADMIN_IDS
from visualizer import generate_grid
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
)
from keyboards import (
    timezone_keyboard, notification_keyboard, contexts_keyboard,
    after_activity_keyboard, stats_keyboard, schedule_keyboard,
    activities_list_keyboard, edit_menu_keyboard, delete_confirm_keyboard,
    contexts_list_keyboard, context_menu_keyboard, color_picker_keyboard,
    ctx_delete_confirm_keyboard, goals_contexts_keyboard, export_keyboard,
    tags_keyboard, PREDEFINED_TAGS,
)
import csv
import io
from states import Registration, ActivityFSM, EditFSM, ContextFSM, GoalFSM, NoteFSM

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

/stats — пять режимов просмотра:
• <b>День</b> — все записи за сегодня по часам
• <b>Неделя / Месяц</b> — сводка с % по контекстам
• <b>Сетка недели / Сетка месяца</b> — картинка-грид, где каждый час закрашен цветом контекста
• <b>Сравнение недель</b> — эта неделя vs прошлая по каждому контексту

━━━━━━━━━━━━━━━━━━━━━━
🎯 <b>ЦЕЛИ</b>

/goals — задай сколько часов в неделю хочешь тратить на каждый контекст и следи за прогрессом:
<code>█████░░░░░  5ч / 10ч  (50%)</code>

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
/add · /edit · /note · /stats · /goals · /contexts · /schedule · /export · /timezone · /cancel"""


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    if await user_exists(message.from_user.id):
        await message.answer(
            WELCOME_TEXT + "\n\n📍 Твой часовой пояс и расписание уже настроены.",
            parse_mode="HTML",
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
        f"🏷 {tag_text}",
        reply_markup=after_activity_keyboard(data["date_str"], data["hour"]),
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
        "⏱ Введи новую длительность:\n"
        "<code>30</code>  <code>1ч</code>  <code>1ч 30мин</code>",
        parse_mode="HTML",
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

@router.message(Command("contexts"))
async def cmd_contexts(message: Message, state: FSMContext):
    if not await user_exists(message.from_user.id):
        await message.answer("Сначала зарегистрируйся: /start")
        return
    await state.clear()
    contexts = await get_user_contexts(message.from_user.id)
    if not contexts:
        await message.answer("У тебя пока нет контекстов.")
        return
    await message.answer(
        "🏷 <b>Твои контексты:</b>",
        parse_mode="HTML",
        reply_markup=contexts_list_keyboard(contexts),
    )


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
async def cb_ctx_back(callback: CallbackQuery):
    contexts = await get_user_contexts(callback.from_user.id)
    await callback.message.edit_text(
        "🏷 <b>Твои контексты:</b>",
        parse_mode="HTML",
        reply_markup=contexts_list_keyboard(contexts),
    )
    await callback.answer()


# — Rename —

@router.callback_query(F.data.startswith("cm_ren:"))
async def cb_ctx_rename_start(callback: CallbackQuery, state: FSMContext):
    ctx_id = int(callback.data.split(":")[1])
    await state.set_state(ContextFSM.waiting_new_name)
    await state.update_data(ctx_id=ctx_id)
    await callback.message.answer("✏️ Введи новое название контекста:")
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
    await message.answer(
        f"✅ Переименовано: {ctx[2]} <b>{ctx[1]}</b>",
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
    contexts = await get_user_contexts(callback.from_user.id)
    if contexts:
        await callback.message.edit_text(
            "✅ Контекст удалён.\n\n🏷 <b>Твои контексты:</b>",
            parse_mode="HTML",
            reply_markup=contexts_list_keyboard(contexts),
        )
    else:
        await callback.message.edit_text("✅ Контекст удалён. Контекстов больше нет.")
    await callback.answer()


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
