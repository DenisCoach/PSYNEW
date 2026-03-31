from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from typing import List, Tuple
from config import TIMEZONES

MAX_CONTEXTS_SHOWN = 8


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Persistent bottom keyboard with all main commands."""
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="➕ Добавить"),
        KeyboardButton(text="⚡ Быстрые"),
        KeyboardButton(text="📊 Статистика"),
    )
    builder.row(
        KeyboardButton(text="🎯 Цели"),
        KeyboardButton(text="🏆 Топ"),
        KeyboardButton(text="🕐 Паттерны"),
    )
    builder.row(
        KeyboardButton(text="✏️ Редактировать"),
        KeyboardButton(text="📝 Заметка"),
        KeyboardButton(text="📤 Экспорт"),
    )
    builder.row(
        KeyboardButton(text="🗓 Привычки"),
        KeyboardButton(text="🏷 Контексты"),
        KeyboardButton(text="⏰ Расписание"),
    )
    builder.row(
        KeyboardButton(text="⚙️ Настройки"),
    )
    return builder.as_markup(resize_keyboard=True, persistent=True)


def _fmt_dur(minutes: int) -> str:
    if minutes < 60:
        return f"{minutes}м"
    h, m = divmod(minutes, 60)
    return f"{h}ч" if m == 0 else f"{h}ч{m}м"


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


def notification_quick_keyboard(
    recent: List[Tuple], date_str: str, hour: int
) -> InlineKeyboardMarkup:
    """Quick-add keyboard shown inline in the notification.
    recent: [(act_id, ctx_name, color, description, duration_minutes), ...]"""
    builder = InlineKeyboardBuilder()
    for act_id, ctx_name, color, desc, dur in recent:
        short = desc[:22] + "…" if len(desc) > 22 else desc
        builder.button(
            text=f"{color} {short} · {_fmt_dur(dur)}",
            callback_data=f"qk:{act_id}:{date_str}:{hour}",
        )
    builder.button(text="✏️ Своё дело",  callback_data=f"qk_custom:{date_str}:{hour}")
    builder.button(text="⏭ Пропустить", callback_data=f"act_skip:{date_str}:{hour}")
    builder.adjust(1)
    return builder.as_markup()


def notification_added_keyboard(
    date_str: str, hour: int, added: List[str]
) -> InlineKeyboardMarkup:
    """Shown after each quick-add inside the notification.
    added: list of human-readable strings of what was added so far."""
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Ещё дело",  callback_data=f"qk_more:{date_str}:{hour}")
    builder.button(text="✅ Готово",     callback_data=f"act_done:{date_str}:{hour}")
    builder.adjust(2)
    return builder.as_markup()


def duration_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for label, minutes in [
        ("15 мин", 15), ("30 мин", 30), ("45 мин", 45),
        ("1 ч",    60), ("1.5 ч",  90), ("2 ч",   120),
        ("3 ч",   180), ("✏️ Своё", 0),
    ]:
        builder.button(text=label, callback_data=f"dur:{minutes}")
    builder.adjust(3, 3, 2)
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


def after_activity_keyboard(date_str: str, hour: int, act_id: int = 0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить ещё", callback_data=f"act_more:{date_str}:{hour}")
    builder.button(text="✅ Готово",        callback_data=f"act_done:{date_str}:{hour}")
    if act_id:
        builder.button(text="💾 Шаблон",   callback_data=f"act_tmpl:{act_id}")
    builder.adjust(2)
    return builder.as_markup()


COLORS = ["🟥","🟧","🟨","🟩","🟦","🟪","🟫","⬛","🔴","🔵","🟤","⚪"]


def contexts_list_keyboard(contexts: List[Tuple]) -> InlineKeyboardMarkup:
    """List of user's contexts. Each tuple: (id, name, color)"""
    builder = InlineKeyboardBuilder()
    for ctx_id, name, color in contexts:
        builder.button(text=f"{color} {name}", callback_data=f"cm:{ctx_id}")
    builder.adjust(1)
    return builder.as_markup()


def context_menu_keyboard(ctx_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Переименовать", callback_data=f"cm_ren:{ctx_id}")
    builder.button(text="🎨 Цвет",          callback_data=f"cm_col:{ctx_id}")
    builder.button(text="🗑 Удалить",       callback_data=f"cm_del:{ctx_id}")
    builder.button(text="◀️ Назад",         callback_data="cm_back")
    builder.adjust(2, 1, 1)
    return builder.as_markup()


def color_picker_keyboard(ctx_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for color in COLORS:
        builder.button(text=color, callback_data=f"cm_setcol:{ctx_id}:{color}")
    builder.adjust(4)
    return builder.as_markup()


def ctx_delete_confirm_keyboard(ctx_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data=f"cm_del_ok:{ctx_id}")
    builder.button(text="❌ Отмена",      callback_data=f"cm:{ctx_id}")
    builder.adjust(2)
    return builder.as_markup()


def goals_contexts_keyboard(contexts: List[Tuple], goals: dict) -> InlineKeyboardMarkup:
    """contexts: [(id, name, color)], goals: {context_id: weekly_hours}"""
    builder = InlineKeyboardBuilder()
    for ctx_id, name, color in contexts:
        target = goals.get(ctx_id)
        suffix = f" → {target}ч/нед" if target else ""
        builder.button(text=f"{color} {name}{suffix}", callback_data=f"gl:{ctx_id}")
    builder.adjust(1)
    return builder.as_markup()


def habits_keyboard(habits: List[Tuple], logs: dict) -> InlineKeyboardMarkup:
    """Main habits screen.
    habits: [(id, name, habit_type, emoji, sort_order), ...]
    logs:   {habit_id: (time_start, time_end, text_value)}"""
    builder = InlineKeyboardBuilder()
    for habit_id, name, habit_type, emoji, _ in habits:
        if habit_id in logs:
            ts, te, tv = logs[habit_id]
            if habit_type == "travel" and tv:
                status = f"✅ {tv}"
            elif te:
                status = f"✅ {ts}–{te}"
            elif ts:
                status = f"✅ {ts}"
            else:
                status = "✅"
            label = f"{emoji} {name}  {status}"
        else:
            label = f"{emoji} {name}  ○"
        builder.button(text=label, callback_data=f"hb:{habit_id}")
    builder.button(text="➕ Своя привычка", callback_data="hb_new")
    builder.button(text="🗑 Управление",    callback_data="hb_manage")
    builder.adjust(1)
    return builder.as_markup()


def habit_action_keyboard(habit_id: int, logged: bool) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Записать",  callback_data=f"hb_log:{habit_id}")
    if logged:
        builder.button(text="🗑 Сбросить", callback_data=f"hb_del:{habit_id}")
    builder.button(text="◀️ Назад",     callback_data="hb_back")
    builder.adjust(2, 1) if logged else builder.adjust(1, 1)
    return builder.as_markup()


def habits_manage_keyboard(habits: List[Tuple]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for habit_id, name, habit_type, emoji, _ in habits:
        builder.button(text=f"{emoji} {name}", callback_data=f"hb_info:{habit_id}")
        builder.button(text="🗑",              callback_data=f"hb_rm:{habit_id}")
    builder.button(text="◀️ Назад", callback_data="hb_back")
    builder.adjust(*([2] * len(habits) + [1]))
    return builder.as_markup()


def export_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📅 Неделя",  callback_data="exp:week")
    builder.button(text="📆 Месяц",   callback_data="exp:month")
    builder.button(text="📊 Всё время", callback_data="exp:all")
    builder.adjust(3)
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


def hour_picker_keyboard(today_str: str, yesterday_str: str, current_hour: int) -> InlineKeyboardMarkup:
    """Grid of hours for manual add. Shows today and yesterday tabs + hour buttons."""
    builder = InlineKeyboardBuilder()
    # Day selector
    builder.button(text="📅 Сегодня ✓", callback_data=f"addday:{today_str}:{current_hour}")
    builder.button(text="📅 Вчера",      callback_data=f"addday:{yesterday_str}:{current_hour}")
    builder.adjust(2)
    # Hours 0–23, mark current hour
    for h in range(0, 24):
        label = f"▶ {h:02d}:00" if h == current_hour else f"{h:02d}:00"
        builder.button(text=label, callback_data=f"addhour:{today_str}:{h}")
    builder.adjust(2, *([4] * 6))
    return builder.as_markup()


def hour_picker_day_keyboard(date_str: str, today_str: str, yesterday_str: str, current_hour: int) -> InlineKeyboardMarkup:
    """Same picker but with correct day selected."""
    builder = InlineKeyboardBuilder()
    is_today = date_str == today_str
    builder.button(
        text="📅 Сегодня ✓" if is_today else "📅 Сегодня",
        callback_data=f"addday:{today_str}:{current_hour}",
    )
    builder.button(
        text="📅 Вчера ✓" if not is_today else "📅 Вчера",
        callback_data=f"addday:{yesterday_str}:{current_hour}",
    )
    builder.adjust(2)
    for h in range(0, 24):
        label = f"▶ {h:02d}:00" if h == current_hour else f"{h:02d}:00"
        builder.button(text=label, callback_data=f"addhour:{date_str}:{h}")
    builder.adjust(2, *([4] * 6))
    return builder.as_markup()


def schedule_keyboard(active_hours: List[int]) -> InlineKeyboardMarkup:
    """Grid of all 24 hours. Active = ✅, inactive = ☐"""
    builder = InlineKeyboardBuilder()
    for hour in range(0, 24):
        label = f"✅ {hour:02d}" if hour in active_hours else f"☐ {hour:02d}"
        builder.button(text=label, callback_data=f"sched:{hour}")
    builder.adjust(4)
    return builder.as_markup()


PREDEFINED_TAGS = ["важное", "срочное", "рутина", "фокус", "встреча", "отдых"]


def templates_keyboard(templates: List[Tuple], manage: bool = False) -> InlineKeyboardMarkup:
    """templates: [(id, ctx_name, color, description, duration_minutes, use_count), ...]"""
    builder = InlineKeyboardBuilder()
    for tmpl_id, ctx_name, color, desc, dur, _ in templates:
        short = desc[:22] + "…" if len(desc) > 22 else desc
        label = f"{color} {short} · {_fmt_dur(dur)}"
        builder.button(text=label, callback_data=f"qt:{tmpl_id}")
        if manage:
            builder.button(text="🗑", callback_data=f"qt_del:{tmpl_id}")
    if manage:
        builder.button(text="◀️ Назад", callback_data="qt_back")
        builder.adjust(*([2] * len(templates) + [1]))
    else:
        builder.button(text="🗑 Управление шаблонами", callback_data="qt_manage")
        builder.adjust(*([1] * len(templates) + [1]))
    return builder.as_markup()


def tags_keyboard(selected: List[str], act_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for tag in PREDEFINED_TAGS:
        prefix = "✅" if tag in selected else "☐"
        builder.button(text=f"{prefix} #{tag}", callback_data=f"tg:{tag}:{act_id}")
    builder.button(text="💾 Сохранить", callback_data=f"tg_save:{act_id}")
    builder.adjust(3, 3, 1)
    return builder.as_markup()


def stats_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📅 День",          callback_data="stats:day")
    builder.button(text="📆 Неделя",        callback_data="stats:week")
    builder.button(text="📊 Месяц",         callback_data="stats:month")
    builder.button(text="🗓 Сетка недели",  callback_data="stats:grid_week")
    builder.button(text="🗓 Сетка месяца",  callback_data="stats:grid_month")
    builder.button(text="↔️ Сравнение недель", callback_data="stats:compare")
    builder.button(text="📈 Динамика",      callback_data="stats:dynamics")
    builder.adjust(3, 2, 1, 1)
    return builder.as_markup()
