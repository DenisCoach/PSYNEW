from aiogram.fsm.state import State, StatesGroup


class Registration(StatesGroup):
    choosing_timezone = State()


class ActivityFSM(StatesGroup):
    waiting_description = State()
    waiting_duration = State()
    choosing_context = State()
    entering_new_context = State()
    choosing_tags = State()


class EditFSM(StatesGroup):
    waiting_new_description = State()
    waiting_new_duration = State()
    choosing_new_context = State()


class ContextFSM(StatesGroup):
    waiting_new_name    = State()
    waiting_create_name = State()


class GoalFSM(StatesGroup):
    waiting_hours = State()


class NoteFSM(StatesGroup):
    waiting_text = State()
    editing_text = State()   # editing existing note


class NotifFSM(StatesGroup):
    quick_adding = State()


class PlaceFSM(StatesGroup):
    waiting_name  = State()
    waiting_emoji = State()


class PersonFSM(StatesGroup):
    waiting_name = State()


class PeoplePickFSM(StatesGroup):
    selecting = State()


class SnapshotFSM(StatesGroup):
    waiting_name = State()
    waiting_reset_name = State()


class HabitFSM(StatesGroup):
    waiting_time        = State()   # simple time HH:MM
    waiting_travel_from = State()
    waiting_travel_to   = State()
    waiting_travel_dep  = State()
    waiting_travel_arr  = State()
    waiting_custom_name = State()
    waiting_custom_emoji = State()
