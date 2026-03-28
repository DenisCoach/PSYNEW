from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List, Tuple
from config import TIMEZONES

MAX_CONTEXTS_SHOWN = 8


def timezone_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for label, tz in TIMEZONES.items():
        builder.button(text=label, callback_data=f"tz:{tz}")
    builder.adjust(2)
    return builder.as_markup()


def notification_keyboard(date_str: str, hour: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить дело", callback_data=f"act_add:{date_str}:{hour}")
    builder.button(text="⏭ Пропустить",    callback_data=f"act_skip:{date_str}:{hour}")
    builder.adjust(2)
    return builder.as_markup()


def contexts_keyboard(
    contexts: List[Tuple[int, str, str]], date_str: str, hour: int
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for ctx_id, name, color in contexts[:MAX_CONTEXTS_SHOWN]:
        builder.button(
            text=f"{color} {name}",
            callback_data=f"ctx:{ctx_id}:{date_str}:{hour}",
        )
    builder.button(text="➕ Новый контекст", callback_data=f"ctx_new:{date_str}:{hour}")
    builder.adjust(2)
    return builder.as_markup()


def after_activity_keyboard(date_str: str, hour: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить ещё", callback_data=f"act_more:{date_str}:{hour}")
    builder.button(text="✅ Готово",        callback_data=f"act_done:{date_str}:{hour}")
    builder.adjust(2)
    return builder.as_markup()


def activities_list_keyboard(activities: List[Tuple]) -> InlineKeyboardMarkup:
    """List of recent activities as buttons. Each tuple: (id, date, hour, ctx, color, desc, dur)"""
    builder = InlineKeyboardBuilder()
    for act_id, act_date, hour, ctx, color, desc, dur in activities:
        short_desc = desc[:18] + "…" if len(desc) > 18 else desc
        label = f"{color} {short_desc} · {act_date[5:]} {hour:02d}:00"
        builder.button(text=label, callback_data=f"ea:{act_id}")
    builder.adjust(1)
    return builder.as_markup()


def edit_menu_keyboard(act_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Описание",   callback_data=f"ed:{act_id}")
    builder.button(text="⏱ Время",       callback_data=f"et:{act_id}")
    builder.button(text="🔄 Контекст",   callback_data=f"ec:{act_id}")
    builder.button(text="🗑 Удалить",    callback_data=f"edel:{act_id}")
    builder.button(text="◀️ Назад",      callback_data="edit_back")
    builder.adjust(2, 2, 1)
    return builder.as_markup()


def delete_confirm_keyboard(act_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data=f"edel_ok:{act_id}")
    builder.button(text="❌ Отмена",      callback_data=f"ea:{act_id}")
    builder.adjust(2)
    return builder.as_markup()


def schedule_keyboard(active_hours: List[int]) -> InlineKeyboardMarkup:
    """Grid of all 24 hours. Active = ✅, inactive = ☐"""
    builder = InlineKeyboardBuilder()
    for hour in range(0, 24):
        label = f"✅ {hour:02d}" if hour in active_hours else f"☐ {hour:02d}"
        builder.button(text=label, callback_data=f"sched:{hour}")
    builder.adjust(4)
    return builder.as_markup()


def stats_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📅 День",          callback_data="stats:day")
    builder.button(text="📆 Неделя",        callback_data="stats:week")
    builder.button(text="📊 Месяц",         callback_data="stats:month")
    builder.button(text="🗓 Сетка недели",  callback_data="stats:grid_week")
    builder.button(text="🗓 Сетка месяца",  callback_data="stats:grid_month")
    builder.adjust(3, 2)
    return builder.as_markup()
