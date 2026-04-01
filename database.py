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
                tags             TEXT    NOT NULL DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id)    REFERENCES users(user_id),
                FOREIGN KEY (context_id) REFERENCES contexts(id)
            );

            CREATE TABLE IF NOT EXISTS day_notes (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                note_date   TEXT    NOT NULL,
                text        TEXT    NOT NULL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, note_date),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS notifications_sent (
                user_id           INTEGER NOT NULL,
                notification_date TEXT    NOT NULL,
                hour_slot         INTEGER NOT NULL,
                PRIMARY KEY (user_id, notification_date, hour_slot)
            );

            CREATE TABLE IF NOT EXISTS goals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                context_id  INTEGER NOT NULL,
                weekly_hours REAL    NOT NULL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, context_id),
                FOREIGN KEY (user_id)   REFERENCES users(user_id),
                FOREIGN KEY (context_id) REFERENCES contexts(id)
            );

            CREATE TABLE IF NOT EXISTS places (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                name       TEXT    NOT NULL,
                emoji      TEXT    NOT NULL DEFAULT '📍',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS people (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                name       TEXT    NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS activity_people (
                activity_id INTEGER NOT NULL,
                person_id   INTEGER NOT NULL,
                PRIMARY KEY (activity_id, person_id),
                FOREIGN KEY (activity_id) REFERENCES activities(id),
                FOREIGN KEY (person_id)   REFERENCES people(id)
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                name       TEXT    NOT NULL,
                data       TEXT    NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS habits (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                name       TEXT    NOT NULL,
                habit_type TEXT    NOT NULL DEFAULT 'custom',
                emoji      TEXT    NOT NULL DEFAULT '📌',
                is_active  INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 99,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS habit_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                habit_id    INTEGER NOT NULL,
                log_date    TEXT    NOT NULL,
                time_start  TEXT,
                time_end    TEXT,
                text_value  TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id)  REFERENCES users(user_id),
                FOREIGN KEY (habit_id) REFERENCES habits(id)
            );

            CREATE TABLE IF NOT EXISTS templates (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id          INTEGER NOT NULL,
                context_id       INTEGER NOT NULL,
                description      TEXT    NOT NULL,
                duration_minutes INTEGER NOT NULL,
                use_count        INTEGER NOT NULL DEFAULT 0,
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id)    REFERENCES users(user_id),
                FOREIGN KEY (context_id) REFERENCES contexts(id)
            );
        """)
        # Migrations for existing databases
        for sql in [
            "ALTER TABLE users ADD COLUMN notification_hours TEXT NOT NULL DEFAULT '10,11,12,13,14,15,16,17,18,19,20,21'",
            "ALTER TABLE activities ADD COLUMN tags TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE activities ADD COLUMN place_id INTEGER",
            """CREATE TABLE IF NOT EXISTS places (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, name TEXT NOT NULL,
                emoji TEXT NOT NULL DEFAULT '📍',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            """CREATE TABLE IF NOT EXISTS people (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL, name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
            """CREATE TABLE IF NOT EXISTS activity_people (
                activity_id INTEGER NOT NULL, person_id INTEGER NOT NULL,
                PRIMARY KEY (activity_id, person_id))""",
            """CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                data TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            """CREATE TABLE IF NOT EXISTS habits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                habit_type TEXT NOT NULL DEFAULT 'custom',
                emoji TEXT NOT NULL DEFAULT '📌',
                is_active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 99,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            """CREATE TABLE IF NOT EXISTS habit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                habit_id INTEGER NOT NULL,
                log_date TEXT NOT NULL,
                time_start TEXT,
                time_end TEXT,
                text_value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            """CREATE TABLE IF NOT EXISTS templates (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id          INTEGER NOT NULL,
                context_id       INTEGER NOT NULL,
                description      TEXT    NOT NULL,
                duration_minutes INTEGER NOT NULL,
                use_count        INTEGER NOT NULL DEFAULT 0,
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id)    REFERENCES users(user_id),
                FOREIGN KEY (context_id) REFERENCES contexts(id)
            )""",
        ]:
            try:
                await db.execute(sql)
                await db.commit()
            except Exception:
                pass


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


async def get_context_by_id(ctx_id: int, user_id: int) -> Optional[Tuple]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT id, name, color FROM contexts WHERE id = ? AND user_id = ?",
            (ctx_id, user_id),
        )
        return await cur.fetchone()


async def rename_context(ctx_id: int, user_id: int, new_name: str):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE contexts SET name = ? WHERE id = ? AND user_id = ?",
            (new_name.strip(), ctx_id, user_id),
        )
        await db.commit()


async def update_context_color(ctx_id: int, user_id: int, color: str):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE contexts SET color = ? WHERE id = ? AND user_id = ?",
            (color, ctx_id, user_id),
        )
        await db.commit()


async def count_context_activities(ctx_id: int, user_id: int) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM activities WHERE context_id = ? AND user_id = ?",
            (ctx_id, user_id),
        )
        return (await cur.fetchone())[0]


async def delete_context(ctx_id: int, user_id: int):
    """Deletes context and all its activities."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM activities WHERE context_id = ? AND user_id = ?",
            (ctx_id, user_id),
        )
        await db.execute(
            "DELETE FROM contexts WHERE id = ? AND user_id = ?",
            (ctx_id, user_id),
        )
        await db.commit()


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


async def get_or_create_habits_context(user_id: int) -> int:
    """Get or create the special 'Привычки' context for habit activities."""
    ctx_id, _ = await get_or_create_context(user_id, "Привычки")
    return ctx_id


# ── Activities ────────────────────────────────────────────────────────────────

async def add_activity(
    user_id: int,
    context_id: int,
    description: str,
    duration_minutes: int,
    activity_date: str,
    hour_slot: int,
) -> int:
    """Returns the new activity ID."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """INSERT INTO activities
               (user_id, context_id, description, duration_minutes, activity_date, hour_slot)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, context_id, description, duration_minutes, activity_date, hour_slot),
        )
        await db.commit()
        return cur.lastrowid


async def update_activity_tags(activity_id: int, user_id: int, tags: List[str]):
    tags_str = ",".join(tags)
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE activities SET tags = ? WHERE id = ? AND user_id = ?",
            (tags_str, activity_id, user_id),
        )
        await db.commit()


# ── Day notes ─────────────────────────────────────────────────────────────────

async def save_day_note(user_id: int, note_date: str, text: str):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """INSERT INTO day_notes (user_id, note_date, text) VALUES (?, ?, ?)
               ON CONFLICT(user_id, note_date) DO UPDATE SET text = excluded.text""",
            (user_id, note_date, text),
        )
        await db.commit()


async def get_day_note(user_id: int, note_date: str) -> Optional[str]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT text FROM day_notes WHERE user_id = ? AND note_date = ?",
            (user_id, note_date),
        )
        row = await cur.fetchone()
        return row[0] if row else None


async def get_all_day_notes(user_id: int) -> List[Tuple]:
    """Returns [(note_date, text), ...] ordered by date desc."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT note_date, text FROM day_notes WHERE user_id = ? ORDER BY note_date DESC",
            (user_id,),
        )
        return await cur.fetchall()


async def delete_day_note(user_id: int, note_date: str):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM day_notes WHERE user_id = ? AND note_date = ?",
            (user_id, note_date),
        )
        await db.commit()


# ── Week comparison ───────────────────────────────────────────────────────────

async def get_week_comparison(
    user_id: int, w1_start: str, w1_end: str, w2_start: str, w2_end: str
) -> List[Tuple]:
    """Returns [(ctx_name, color, w1_minutes, w2_minutes), ...]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT c.name, c.color,
                  SUM(CASE WHEN a.activity_date BETWEEN ? AND ? THEN a.duration_minutes ELSE 0 END) as w1,
                  SUM(CASE WHEN a.activity_date BETWEEN ? AND ? THEN a.duration_minutes ELSE 0 END) as w2
               FROM contexts c
               LEFT JOIN activities a ON a.context_id = c.id AND a.user_id = c.user_id
               WHERE c.user_id = ?
               GROUP BY c.id
               HAVING w1 > 0 OR w2 > 0
               ORDER BY (w1 + w2) DESC""",
            (w1_start, w1_end, w2_start, w2_end, user_id),
        )
        return await cur.fetchall()


async def get_recent_activities(user_id: int, limit: int = 10) -> List[Tuple]:
    """Returns [(id, activity_date, hour_slot, ctx_name, color, description, duration), ...]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT a.id, a.activity_date, a.hour_slot, c.name, c.color,
                      a.description, a.duration_minutes
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ?
               ORDER BY a.activity_date DESC, a.hour_slot DESC, a.created_at DESC
               LIMIT ?""",
            (user_id, limit),
        )
        return await cur.fetchall()


async def get_activity_by_id(activity_id: int, user_id: int) -> Optional[Tuple]:
    """Returns (id, activity_date, hour_slot, ctx_name, color, description, duration)"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT a.id, a.activity_date, a.hour_slot, c.name, c.color,
                      a.description, a.duration_minutes
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.id = ? AND a.user_id = ?""",
            (activity_id, user_id),
        )
        return await cur.fetchone()


async def delete_activity(activity_id: int, user_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM activities WHERE id = ? AND user_id = ?",
            (activity_id, user_id),
        )
        await db.commit()


async def update_activity_description(activity_id: int, user_id: int, description: str):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE activities SET description = ? WHERE id = ? AND user_id = ?",
            (description, activity_id, user_id),
        )
        await db.commit()


async def update_activity_duration(activity_id: int, user_id: int, duration: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE activities SET duration_minutes = ? WHERE id = ? AND user_id = ?",
            (duration, activity_id, user_id),
        )
        await db.commit()


async def update_activity_context(activity_id: int, user_id: int, context_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE activities SET context_id = ? WHERE id = ? AND user_id = ?",
            (context_id, activity_id, user_id),
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


async def get_export_activities(user_id: int, start_date: str, end_date: str) -> List[Tuple]:
    """Returns all activities for CSV export."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT a.activity_date, a.hour_slot, c.name,
                      a.description, a.duration_minutes
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ? AND a.activity_date BETWEEN ? AND ?
               ORDER BY a.activity_date, a.hour_slot, a.created_at""",
            (user_id, start_date, end_date),
        )
        return await cur.fetchall()


# ── Goals ─────────────────────────────────────────────────────────────────────

async def set_goal(user_id: int, context_id: int, weekly_hours: float):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """INSERT INTO goals (user_id, context_id, weekly_hours)
               VALUES (?, ?, ?)
               ON CONFLICT(user_id, context_id)
               DO UPDATE SET weekly_hours = excluded.weekly_hours""",
            (user_id, context_id, weekly_hours),
        )
        await db.commit()


async def delete_goal(user_id: int, context_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM goals WHERE user_id = ? AND context_id = ?",
            (user_id, context_id),
        )
        await db.commit()


async def get_goals_with_progress(user_id: int, week_start: str, week_end: str) -> List[Tuple]:
    """Returns [(ctx_name, color, weekly_hours_target, actual_minutes), ...]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT c.name, c.color, g.weekly_hours, g.context_id,
                      COALESCE(SUM(a.duration_minutes), 0) as actual_minutes
               FROM goals g
               JOIN contexts c ON g.context_id = c.id
               LEFT JOIN activities a ON a.context_id = g.context_id
                   AND a.user_id = g.user_id
                   AND a.activity_date BETWEEN ? AND ?
               WHERE g.user_id = ?
               GROUP BY g.context_id
               ORDER BY g.weekly_hours DESC""",
            (week_start, week_end, user_id),
        )
        return await cur.fetchall()


async def get_recorded_hours_today(user_id: int, activity_date: str) -> set:
    """Returns set of hour_slots that have at least one activity today."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT DISTINCT hour_slot FROM activities WHERE user_id = ? AND activity_date = ?",
            (user_id, activity_date),
        )
        return {row[0] for row in await cur.fetchall()}


async def get_day_summary(user_id: int, activity_date: str) -> List[Tuple]:
    """Returns [(ctx_name, color, total_minutes), ...] for the day."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT c.name, c.color, SUM(a.duration_minutes) as total
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ? AND a.activity_date = ?
               GROUP BY c.id
               ORDER BY total DESC""",
            (user_id, activity_date),
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


# ── Templates (quick activities) ─────────────────────────────────────────────

async def get_templates(user_id: int) -> List[Tuple]:
    """Returns [(id, ctx_name, color, description, duration_minutes, use_count), ...]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT t.id, c.name, c.color, t.description, t.duration_minutes, t.use_count
               FROM templates t
               JOIN contexts c ON t.context_id = c.id
               WHERE t.user_id = ?
               ORDER BY t.use_count DESC, t.created_at DESC""",
            (user_id,),
        )
        return await cur.fetchall()


async def get_template_by_id(template_id: int, user_id: int) -> Optional[Tuple]:
    """Returns (id, context_id, ctx_name, color, description, duration_minutes)"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT t.id, t.context_id, c.name, c.color, t.description, t.duration_minutes
               FROM templates t
               JOIN contexts c ON t.context_id = c.id
               WHERE t.id = ? AND t.user_id = ?""",
            (template_id, user_id),
        )
        return await cur.fetchone()


async def add_template(
    user_id: int, context_id: int, description: str, duration_minutes: int
) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """INSERT INTO templates (user_id, context_id, description, duration_minutes)
               VALUES (?, ?, ?, ?)""",
            (user_id, context_id, description, duration_minutes),
        )
        await db.commit()
        return cur.lastrowid


async def delete_template(template_id: int, user_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM templates WHERE id = ? AND user_id = ?",
            (template_id, user_id),
        )
        await db.commit()


async def increment_template_use(template_id: int, user_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE templates SET use_count = use_count + 1 WHERE id = ? AND user_id = ?",
            (template_id, user_id),
        )
        await db.commit()


# ── Analytics ────────────────────────────────────────────────────────────────

async def get_recent_unique_for_quick(user_id: int, limit: int = 6) -> List[Tuple]:
    """Returns recent unique (description, context) pairs for quick-add buttons.
    Returns [(act_id, ctx_name, color, description, duration_minutes), ...]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT a.id, c.name, c.color, a.description, a.duration_minutes
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ?
               GROUP BY LOWER(a.description), a.context_id
               ORDER BY MAX(a.created_at) DESC
               LIMIT ?""",
            (user_id, limit),
        )
        return await cur.fetchall()


async def get_top_activities(user_id: int, limit: int = 10) -> List[Tuple]:
    """Returns [(description, ctx_name, color, count, total_minutes), ...] most frequent."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT a.description, c.name, c.color,
                      COUNT(*) as cnt, SUM(a.duration_minutes) as total_m
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ?
               GROUP BY LOWER(a.description), c.id
               ORDER BY cnt DESC
               LIMIT ?""",
            (user_id, limit),
        )
        return await cur.fetchall()


async def get_streak(user_id: int, today_str: str) -> int:
    """Returns number of consecutive days with at least one activity, ending today or yesterday."""
    from datetime import date, timedelta
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT DISTINCT activity_date FROM activities WHERE user_id = ? ORDER BY activity_date DESC",
            (user_id,),
        )
        dates = [row[0] for row in await cur.fetchall()]
    if not dates:
        return 0
    today = date.fromisoformat(today_str)
    latest = date.fromisoformat(dates[0])
    if latest < today - timedelta(days=1):
        return 0
    streak = 0
    expected = latest
    for d_str in dates:
        d = date.fromisoformat(d_str)
        if d == expected:
            streak += 1
            expected -= timedelta(days=1)
        else:
            break
    return streak


async def get_hour_patterns(user_id: int) -> List[Tuple]:
    """Returns [(hour_slot, ctx_name, color, total_minutes), ...] ordered by hour then minutes desc."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT a.hour_slot, c.name, c.color, SUM(a.duration_minutes) as total_m
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ?
               GROUP BY a.hour_slot, c.id
               ORDER BY a.hour_slot, total_m DESC""",
            (user_id,),
        )
        return await cur.fetchall()


async def get_weekly_dynamics(user_id: int, weeks: int = 8) -> List[Tuple]:
    """Returns [(week_key, ctx_name, color, total_minutes, week_start_date), ...] for last N weeks."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT strftime('%Y-%W', a.activity_date) as week_key,
                      c.name, c.color,
                      SUM(a.duration_minutes) as total_m,
                      MIN(a.activity_date) as week_start_date
               FROM activities a
               JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ?
               GROUP BY week_key, c.id
               ORDER BY week_key""",
            (user_id,),
        )
        all_rows = await cur.fetchall()
    if not all_rows:
        return []
    all_weeks = sorted(set(r[0] for r in all_rows))
    recent_weeks = set(all_weeks[-weeks:])
    return [r for r in all_rows if r[0] in recent_weeks]


# ── Habits ───────────────────────────────────────────────────────────────────

DEFAULT_HABITS = [
    ("🌅", "Подъём",        "wake",    0),
    ("🌙", "Отход ко сну",  "sleep",   1),
    ("🍳", "Завтрак",       "meal",    2),
    ("🥗", "Обед",          "meal",    3),
    ("🍽", "Ужин",          "meal",    4),
    ("🚗", "Дорога",        "travel",  5),
]


async def ensure_default_habits(user_id: int):
    """Create default habits for user if they have none yet."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM habits WHERE user_id = ?", (user_id,)
        )
        count = (await cur.fetchone())[0]
        if count == 0:
            for emoji, name, habit_type, sort_order in DEFAULT_HABITS:
                await db.execute(
                    "INSERT INTO habits (user_id, name, habit_type, emoji, sort_order) VALUES (?,?,?,?,?)",
                    (user_id, name, habit_type, emoji, sort_order),
                )
            await db.commit()


async def get_habits(user_id: int) -> List[Tuple]:
    """Returns [(id, name, habit_type, emoji, sort_order), ...] active habits."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT id, name, habit_type, emoji, sort_order
               FROM habits WHERE user_id = ? AND is_active = 1
               ORDER BY sort_order, id""",
            (user_id,),
        )
        return await cur.fetchall()


async def get_habit_by_id(habit_id: int, user_id: int) -> Optional[Tuple]:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT id, name, habit_type, emoji FROM habits WHERE id = ? AND user_id = ?",
            (habit_id, user_id),
        )
        return await cur.fetchone()


async def add_habit(user_id: int, name: str, emoji: str, habit_type: str = "custom") -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT COALESCE(MAX(sort_order), 0) + 1 FROM habits WHERE user_id = ?",
            (user_id,),
        )
        sort_order = (await cur.fetchone())[0]
        cur = await db.execute(
            "INSERT INTO habits (user_id, name, habit_type, emoji, sort_order) VALUES (?,?,?,?,?)",
            (user_id, name, "custom", emoji, sort_order),
        )
        await db.commit()
        return cur.lastrowid


async def delete_habit(habit_id: int, user_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE habits SET is_active = 0 WHERE id = ? AND user_id = ?",
            (habit_id, user_id),
        )
        await db.commit()


async def get_habit_logs_today(user_id: int, log_date: str) -> List[Tuple]:
    """Returns [(habit_id, time_start, time_end, text_value), ...] for today."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT habit_id, time_start, time_end, text_value
               FROM habit_logs WHERE user_id = ? AND log_date = ?""",
            (user_id, log_date),
        )
        return await cur.fetchall()


async def log_habit(
    user_id: int, habit_id: int, log_date: str,
    time_start: Optional[str] = None,
    time_end: Optional[str] = None,
    text_value: Optional[str] = None,
) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """INSERT INTO habit_logs (user_id, habit_id, log_date, time_start, time_end, text_value)
               VALUES (?,?,?,?,?,?)""",
            (user_id, habit_id, log_date, time_start, time_end, text_value),
        )
        log_id = cur.lastrowid
        await db.commit()

    # Also create an activity entry so habits appear in stats
    habit = await get_habit_by_id(habit_id, user_id)
    if habit:
        _, name, habit_type, emoji = habit
        # Build description
        if habit_type == "travel" and text_value:
            desc = f"{emoji} {name}: {time_start}–{time_end}" if time_start and time_end else f"{emoji} {name}: {text_value}"
        elif time_start and time_end:
            desc = f"{emoji} {name}: {time_start}–{time_end}"
        elif time_start:
            desc = f"{emoji} {name}: {time_start}"
        else:
            desc = f"{emoji} {name}"

        # Calculate duration
        duration = 30  # default
        if time_start and time_end:
            try:
                sh, sm = int(time_start.split(":")[0]), int(time_start.split(":")[1])
                eh, em = int(time_end.split(":")[0]), int(time_end.split(":")[1])
                diff = (eh * 60 + em) - (sh * 60 + sm)
                if diff > 0:
                    duration = diff
            except (ValueError, IndexError):
                pass

        # Derive hour slot from time_start if available, else use 0
        hour_slot = 0
        if time_start:
            try:
                hour_slot = int(time_start.split(":")[0])
            except (ValueError, IndexError):
                pass

        ctx_id = await get_or_create_habits_context(user_id)
        await add_activity(user_id, ctx_id, desc, duration, log_date, hour_slot)

    return log_id


async def delete_habit_log(user_id: int, habit_id: int, log_date: str):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM habit_logs WHERE user_id = ? AND habit_id = ? AND log_date = ?",
            (user_id, habit_id, log_date),
        )
        await db.commit()


# ── Places ───────────────────────────────────────────────────────────────────

async def get_places(user_id: int) -> List[Tuple]:
    """Returns [(id, name, emoji), ...]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT id, name, emoji FROM places WHERE user_id = ? ORDER BY name",
            (user_id,),
        )
        return await cur.fetchall()


async def add_place(user_id: int, name: str, emoji: str = "📍") -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "INSERT INTO places (user_id, name, emoji) VALUES (?,?,?)",
            (user_id, name.strip(), emoji),
        )
        await db.commit()
        return cur.lastrowid


async def delete_place(place_id: int, user_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE activities SET place_id = NULL WHERE place_id = ? AND user_id = ?",
            (place_id, user_id),
        )
        await db.execute("DELETE FROM places WHERE id = ? AND user_id = ?", (place_id, user_id))
        await db.commit()


async def set_activity_place(activity_id: int, user_id: int, place_id: Optional[int]):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "UPDATE activities SET place_id = ? WHERE id = ? AND user_id = ?",
            (place_id, activity_id, user_id),
        )
        await db.commit()


async def get_activity_place(activity_id: int) -> Optional[Tuple]:
    """Returns (place_id, name, emoji) or None."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT p.id, p.name, p.emoji FROM activities a
               JOIN places p ON a.place_id = p.id
               WHERE a.id = ?""",
            (activity_id,),
        )
        return await cur.fetchone()


# ── People ────────────────────────────────────────────────────────────────────

async def get_people(user_id: int) -> List[Tuple]:
    """Returns [(id, name), ...]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT id, name FROM people WHERE user_id = ? ORDER BY name",
            (user_id,),
        )
        return await cur.fetchall()


async def add_person(user_id: int, name: str) -> int:
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "INSERT INTO people (user_id, name) VALUES (?,?)",
            (user_id, name.strip()),
        )
        await db.commit()
        return cur.lastrowid


async def delete_person(person_id: int, user_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM activity_people WHERE person_id = ?", (person_id,)
        )
        await db.execute("DELETE FROM people WHERE id = ? AND user_id = ?", (person_id, user_id))
        await db.commit()


async def set_activity_people(activity_id: int, person_ids: List[int]):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("DELETE FROM activity_people WHERE activity_id = ?", (activity_id,))
        for pid in person_ids:
            await db.execute(
                "INSERT OR IGNORE INTO activity_people (activity_id, person_id) VALUES (?,?)",
                (activity_id, pid),
            )
        await db.commit()


async def get_activity_people(activity_id: int) -> List[Tuple]:
    """Returns [(person_id, name), ...]"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            """SELECT p.id, p.name FROM activity_people ap
               JOIN people p ON ap.person_id = p.id
               WHERE ap.activity_id = ?
               ORDER BY p.name""",
            (activity_id,),
        )
        return await cur.fetchall()


# ── Snapshots ────────────────────────────────────────────────────────────────

async def create_snapshot(user_id: int, name: str) -> int:
    """Collects all user data into JSON and saves as snapshot. Returns snapshot id."""
    import json
    async with aiosqlite.connect(DATABASE_PATH) as db:
        data = {}

        cur = await db.execute(
            "SELECT username, timezone, notification_hours FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cur.fetchone()
        data["user"] = {"username": row[0], "timezone": row[1], "notification_hours": row[2]}

        cur = await db.execute(
            "SELECT name, color FROM contexts WHERE user_id = ?", (user_id,)
        )
        data["contexts"] = [{"name": r[0], "color": r[1]} for r in await cur.fetchall()]

        cur = await db.execute(
            """SELECT a.activity_date, a.hour_slot, a.description, a.duration_minutes,
                      a.tags, c.name as ctx_name
               FROM activities a JOIN contexts c ON a.context_id = c.id
               WHERE a.user_id = ?""",
            (user_id,)
        )
        data["activities"] = [
            {"date": r[0], "hour": r[1], "desc": r[2], "dur": r[3], "tags": r[4], "ctx": r[5]}
            for r in await cur.fetchall()
        ]

        cur = await db.execute(
            """SELECT c.name, g.weekly_hours FROM goals g
               JOIN contexts c ON g.context_id = c.id WHERE g.user_id = ?""",
            (user_id,)
        )
        data["goals"] = [{"ctx": r[0], "hours": r[1]} for r in await cur.fetchall()]

        cur = await db.execute(
            "SELECT note_date, text FROM day_notes WHERE user_id = ?", (user_id,)
        )
        data["day_notes"] = [{"date": r[0], "text": r[1]} for r in await cur.fetchall()]

        cur = await db.execute(
            """SELECT c.name, c.color, t.description, t.duration_minutes FROM templates t
               JOIN contexts c ON t.context_id = c.id WHERE t.user_id = ?""",
            (user_id,)
        )
        data["templates"] = [
            {"ctx": r[0], "color": r[1], "desc": r[2], "dur": r[3]}
            for r in await cur.fetchall()
        ]

        cur = await db.execute(
            "SELECT name, habit_type, emoji, sort_order FROM habits WHERE user_id = ? AND is_active=1",
            (user_id,)
        )
        data["habits"] = [
            {"name": r[0], "type": r[1], "emoji": r[2], "sort": r[3]}
            for r in await cur.fetchall()
        ]

        cur = await db.execute(
            """SELECT h.name, hl.log_date, hl.time_start, hl.time_end, hl.text_value
               FROM habit_logs hl JOIN habits h ON hl.habit_id = h.id
               WHERE hl.user_id = ?""",
            (user_id,)
        )
        data["habit_logs"] = [
            {"habit": r[0], "date": r[1], "start": r[2], "end": r[3], "val": r[4]}
            for r in await cur.fetchall()
        ]

        cur = await db.execute(
            "INSERT INTO snapshots (user_id, name, data) VALUES (?, ?, ?)",
            (user_id, name, json.dumps(data, ensure_ascii=False))
        )
        await db.commit()
        return cur.lastrowid


async def get_snapshots(user_id: int) -> List[Tuple]:
    """Returns [(id, name, created_at, activity_count), ...]"""
    import json
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT id, name, data, created_at FROM snapshots WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,)
        )
        rows = await cur.fetchall()
    result = []
    for snap_id, name, data_json, created_at in rows:
        data = json.loads(data_json)
        act_count = len(data.get("activities", []))
        result.append((snap_id, name, created_at[:10], act_count))
    return result


async def restore_snapshot(user_id: int, snapshot_id: int):
    """Clears current user data and restores from snapshot."""
    import json
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cur = await db.execute(
            "SELECT data FROM snapshots WHERE id = ? AND user_id = ?",
            (snapshot_id, user_id)
        )
        row = await cur.fetchone()
        if not row:
            return False
        data = json.loads(row[0])

        # Clear current data
        for table in ["activities", "goals", "templates", "habits",
                      "habit_logs", "day_notes", "notifications_sent"]:
            await db.execute(f"DELETE FROM {table} WHERE user_id = ?", (user_id,))
        await db.execute("DELETE FROM contexts WHERE user_id = ?", (user_id,))

        # Restore contexts
        ctx_id_map = {}
        for c in data.get("contexts", []):
            cur = await db.execute(
                "INSERT INTO contexts (user_id, name, color) VALUES (?,?,?)",
                (user_id, c["name"], c["color"])
            )
            ctx_id_map[c["name"]] = cur.lastrowid

        # Restore activities
        for a in data.get("activities", []):
            ctx_id = ctx_id_map.get(a["ctx"])
            if ctx_id:
                await db.execute(
                    """INSERT INTO activities
                       (user_id, context_id, description, duration_minutes, activity_date, hour_slot, tags)
                       VALUES (?,?,?,?,?,?,?)""",
                    (user_id, ctx_id, a["desc"], a["dur"], a["date"], a["hour"], a.get("tags", ""))
                )

        # Restore goals
        for g in data.get("goals", []):
            ctx_id = ctx_id_map.get(g["ctx"])
            if ctx_id:
                await db.execute(
                    "INSERT INTO goals (user_id, context_id, weekly_hours) VALUES (?,?,?)",
                    (user_id, ctx_id, g["hours"])
                )

        # Restore day notes
        for n in data.get("day_notes", []):
            await db.execute(
                "INSERT INTO day_notes (user_id, note_date, text) VALUES (?,?,?)",
                (user_id, n["date"], n["text"])
            )

        # Restore templates
        for t in data.get("templates", []):
            ctx_id = ctx_id_map.get(t["ctx"])
            if ctx_id:
                await db.execute(
                    "INSERT INTO templates (user_id, context_id, description, duration_minutes) VALUES (?,?,?,?)",
                    (user_id, ctx_id, t["desc"], t["dur"])
                )

        # Restore habits
        habit_id_map = {}
        for h in data.get("habits", []):
            cur = await db.execute(
                "INSERT INTO habits (user_id, name, habit_type, emoji, sort_order) VALUES (?,?,?,?,?)",
                (user_id, h["name"], h["type"], h["emoji"], h["sort"])
            )
            habit_id_map[h["name"]] = cur.lastrowid

        # Restore habit logs
        for hl in data.get("habit_logs", []):
            habit_id = habit_id_map.get(hl["habit"])
            if habit_id:
                await db.execute(
                    "INSERT INTO habit_logs (user_id, habit_id, log_date, time_start, time_end, text_value) VALUES (?,?,?,?,?,?)",
                    (user_id, habit_id, hl["date"], hl["start"], hl["end"], hl["val"])
                )

        # Restore notification_hours
        if "user" in data and data["user"].get("notification_hours"):
            await db.execute(
                "UPDATE users SET notification_hours = ? WHERE user_id = ?",
                (data["user"]["notification_hours"], user_id)
            )

        await db.commit()
        return True


async def delete_snapshot(user_id: int, snapshot_id: int):
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "DELETE FROM snapshots WHERE id = ? AND user_id = ?",
            (snapshot_id, user_id)
        )
        await db.commit()


async def reset_user_data(user_id: int):
    """Delete all user data but keep the user record."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        for table in ["activities", "goals", "templates", "habits",
                      "habit_logs", "day_notes", "notifications_sent"]:
            await db.execute(f"DELETE FROM {table} WHERE user_id = ?", (user_id,))
        await db.execute("DELETE FROM contexts WHERE user_id = ?", (user_id,))
        await db.commit()


async def get_goals_below_threshold(
    user_id: int, week_start: str, week_end: str, threshold: float = 0.30
) -> List[Tuple]:
    """Returns [(ctx_name, color, target_hours, actual_minutes, pct), ...] for goals below threshold."""
    rows = await get_goals_with_progress(user_id, week_start, week_end)
    result = []
    for ctx_name, color, target_h, ctx_id, actual_m in rows:
        if target_h <= 0:
            continue
        pct = actual_m / 60 / target_h
        if pct < threshold:
            result.append((ctx_name, color, target_h, actual_m, pct))
    return result
