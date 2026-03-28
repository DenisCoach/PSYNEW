import aiosqlite
from typing import Optional, List, Tuple
from config import DATABASE_PATH, CONTEXT_COLORS


async def init_db():
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id            INTEGER PRIMARY KEY,
                username           TEXT,
                timezone           TEXT NOT NULL DEFAULT 'Europe/Moscow',
                notification_hours TEXT NOT NULL DEFAULT '10,11,12,13,14,15,16,17,18,19,20,21',
                created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS contexts (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                name       TEXT    NOT NULL,
                color      TEXT    NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, name),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS activities (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id          INTEGER NOT NULL,
                context_id       INTEGER NOT NULL,
                description      TEXT    NOT NULL,
                duration_minutes INTEGER NOT NULL,
                activity_date    TEXT    NOT NULL,
                hour_slot        INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id)    REFERENCES users(user_id),
                FOREIGN KEY (context_id) REFERENCES contexts(id)
            );

            CREATE TABLE IF NOT EXISTS notifications_sent (
                user_id           INTEGER NOT NULL,
                notification_date TEXT    NOT NULL,
                hour_slot         INTEGER NOT NULL,
                PRIMARY KEY (user_id, notification_date, hour_slot)
            );
        """)
        # Migration: add notification_hours column if missing
        try:
            await db.execute(
                "ALTER TABLE users ADD COLUMN notification_hours TEXT NOT NULL "
                "DEFAULT '10,11,12,13,14,15,16,17,18,19,20,21'"
            )
            await db.commit()
        except Exception:
            pass  # Column already exists


# ── Users ─────────────────────────────────────────────────────────────────────

async def user_exists(user_id: int) -> bool:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        return await cur.fetchone() is not None


async def register_user(user_id: int, username: str, timezone: str) -> bool:
    """Returns True if newly registered, False if already exists."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
        if await cur.fetchone():
            return False
        await db.execute(
            "INSERT INTO users (user_id, username, timezone) VALUES (?, ?, ?)",
            (user_id, username or "", timezone),
        )
        await db.commit()
        return True


async def update_timezone(user_id: int, timezone: str):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE users SET timezone = ? WHERE user_id = ?", (timezone, user_id)
        )
        await db.commit()


async def get_user(user_id: int) -> Optional[Tuple]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT user_id, username, timezone FROM users WHERE user_id = ?", (user_id,)
        )
        return await cur.fetchone()


async def get_all_users() -> List[Tuple]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute("SELECT user_id, timezone, notification_hours FROM users")
        return await cur.fetchall()


async def get_notification_hours(user_id: int) -> List[int]:
    """Returns list of hours when user wants notifications, e.g. [10, 12, 15, 18]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT notification_hours FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cur.fetchone()
        if not row or not row[0]:
            return list(range(0, 24))
        return sorted([int(h) for h in row[0].split(",") if h.strip().isdigit()])


async def set_notification_hours(user_id: int, hours: List[int]):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        hours_str = ",".join(str(h) for h in sorted(hours))
        await db.execute(
            "UPDATE users SET notification_hours = ? WHERE user_id = ?",
            (hours_str, user_id),
        )
        await db.commit()


async def toggle_notification_hour(user_id: int, hour: int) -> List[int]:
    """Toggle a single hour on/off. Returns the updated hours list."""
    hours = await get_notification_hours(user_id)
    if hour in hours:
        hours = [h for h in hours if h != hour]
    else:
        hours = sorted(hours + [hour])
    await set_notification_hours(user_id, hours)
    return hours


# ── Contexts ──────────────────────────────────────────────────────────────────

async def get_user_contexts(user_id: int) -> List[Tuple]:
    """Returns [(id, name, color), ...] ordered by most recently used."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT c.id, c.name, c.color
               FROM contexts c
               LEFT JOIN activities a ON a.context_id = c.id
               WHERE c.user_id = ?
               GROUP BY c.id
               ORDER BY MAX(a.created_at) DESC, c.created_at DESC""",
            (user_id,),
        )
        return await cur.fetchall()


async def get_or_create_context(user_id: int, name: str) -> Tuple[int, str]:
    """Returns (context_id, color)."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT id, color FROM contexts WHERE user_id = ? AND LOWER(name) = LOWER(?)",
            (user_id, name.strip()),
        )
        existing = await cur.fetchone()
        if existing:
            return existing[0], existing[1]

        cur = await db.execute(
            "SELECT COUNT(*) FROM contexts WHERE user_id = ?", (user_id,)
        )
        count = (await cur.fetchone())[0]
        color = CONTEXT_COLORS[count % len(CONTEXT_COLORS)]

        cur = await db.execute(
            "INSERT INTO contexts (user_id, name, color) VALUES (?, ?, ?)",
            (user_id, name.strip(), color),
        )
        await db.commit()
        return cur.lastrowid, color


# ── Activities ────────────────────────────────────────────────────────────────

async def add_activity(
    user_id: int,
    context_id: int,
    description: str,
    duration_minutes: int,
    activity_date: str,
    hour_slot: int,
):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """INSERT INTO activities
               (user_id, context_id, description, duration_minutes, activity_date, hour_slot)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, context_id, description, duration_minutes, activity_date, hour_slot),
        )
        await db.commit()


async def get_activities_for_period(
    user_id: int, start_date: str, end_date: str
) -> List[Tuple]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT a.activity_date, a.hour_slot, c.name, c.color,
                      a.description, a.duration_minutes
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ? AND a.activity_date BETWEEN ? AND ?
               ORDER BY a.activity_date, a.hour_slot, a.created_at""",
            (user_id, start_date, end_date),
        )
        return await cur.fetchall()


# ── Notifications dedup ───────────────────────────────────────────────────────

async def get_all_users_stats() -> List[Tuple]:
    """Returns [(user_id, username, timezone, reg_date, activity_count, last_activity), ...]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT u.user_id, u.username, u.timezone, u.created_at,
                      COUNT(a.id) as activity_count,
                      MAX(a.created_at) as last_activity
               FROM users u
               LEFT JOIN activities a ON a.user_id = u.user_id
               GROUP BY u.user_id
               ORDER BY u.created_at DESC"""
        )
        return await cur.fetchall()


async def get_user_full_stats(user_id: int) -> List[Tuple]:
    """Returns recent activities for a specific user."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT a.activity_date, a.hour_slot, c.name, c.color,
                      a.description, a.duration_minutes
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ?
               ORDER BY a.activity_date DESC, a.hour_slot DESC
               LIMIT 50""",
            (user_id,),
        )
        return await cur.fetchall()


async def mark_notification_sent(user_id: int, date_str: str, hour_slot: int) -> bool:
    """Returns True if inserted (not a duplicate), False if already sent."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO notifications_sent (user_id, notification_date, hour_slot) "
                "VALUES (?, ?, ?)",
                (user_id, date_str, hour_slot),
            )
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False
