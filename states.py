from aiogram.fsm.state import State, StatesGroup


class Registration(StatesGroup):
    choosing_timezone = State()


class ActivityFSM(StatesGroup):
    waiting_description = State()
    waiting_duration = State()
    choosing_context = State()
    entering_new_context = State()


class EditFSM(StatesGroup):
    waiting_new_description = State()
    waiting_new_duration = State()
    choosing_new_context = State()


class ContextFSM(StatesGroup):
    waiting_new_name = State()


class GoalFSM(StatesGroup):
    waiting_hours = State()
