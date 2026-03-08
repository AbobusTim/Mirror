import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

DB_PATH = Path("data/bridge.db")


@dataclass(frozen=True)
class BridgeEntry:
    id: int
    user_id: int
    source_id: int
    source_type: str  # 'channel', 'chat', 'forum', 'topic'
    source_title: str
    target_id: int
    target_type: str  # 'channel', 'chat', 'forum', 'topic'
    target_title: str
    keywords: str
    is_active: bool
    session_id: int  # Which session is used for this bridge
    source_thread_id: int = 0  # Forum topic ID (if source is topic)
    target_thread_id: int = 0  # Forum topic ID (if target is topic)
    created_at: str = ""  # Optional field


@dataclass(frozen=True)
class UserSession:
    session_id: int
    user_id: int
    api_id: int
    api_hash: str
    session_string: str
    phone: str
    label: str  # User-friendly name for the session


def init_db() -> None:
    DB_PATH.parent.mkdir(exist_ok=True)
    with _get_connection() as conn:
        # Bridges table with session reference and forum support
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bridges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                source_id INTEGER NOT NULL,
                source_type TEXT NOT NULL CHECK(source_type IN ('channel', 'chat', 'forum', 'topic')),
                source_title TEXT NOT NULL,
                target_id INTEGER NOT NULL,
                target_type TEXT NOT NULL CHECK(target_type IN ('channel', 'chat', 'forum', 'topic')),
                target_title TEXT NOT NULL,
                keywords TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT 1,
                session_id INTEGER DEFAULT 0,
                source_thread_id INTEGER DEFAULT 0,
                target_thread_id INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # Migration: add session_id column if not exists
        try:
            conn.execute("SELECT session_id FROM bridges LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE bridges ADD COLUMN session_id INTEGER DEFAULT 0")
            conn.commit()
        # Migration: add forum topic columns if not exists
        try:
            conn.execute("SELECT source_thread_id FROM bridges LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE bridges ADD COLUMN source_thread_id INTEGER DEFAULT 0")
            conn.commit()
        try:
            conn.execute("SELECT target_thread_id FROM bridges LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE bridges ADD COLUMN target_thread_id INTEGER DEFAULT 0")
            conn.commit()
        # Sessions table - multiple per user
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                api_id INTEGER NOT NULL,
                api_hash TEXT NOT NULL,
                session_string TEXT NOT NULL,
                phone TEXT NOT NULL,
                label TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # Migrate old data if exists (legacy users table)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                api_id INTEGER,
                api_hash TEXT,
                session_string TEXT,
                phone TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()


@contextmanager
def _get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


# Session management

def create_session(
    user_id: int,
    api_id: int,
    api_hash: str,
    session_string: str,
    phone: str,
    label: str = "",
) -> int:
    """Create a new session for user. Returns session_id."""
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO sessions (user_id, api_id, api_hash, session_string, phone, label)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, api_id, api_hash, session_string, phone, label),
        )
        conn.commit()
        return cursor.lastrowid


def get_user_sessions(user_id: int) -> List[UserSession]:
    """Get all sessions for user."""
    with _get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM sessions WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
        return [
            UserSession(
                session_id=row["session_id"],
                user_id=row["user_id"],
                api_id=row["api_id"],
                api_hash=row["api_hash"],
                session_string=row["session_string"],
                phone=row["phone"],
                label=row["label"] or f"Account +{row['phone']}",
            )
            for row in rows
        ]


def get_session(session_id: int) -> Optional[UserSession]:
    """Get specific session by ID."""
    with _get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if row:
            return UserSession(
                session_id=row["session_id"],
                user_id=row["user_id"],
                api_id=row["api_id"],
                api_hash=row["api_hash"],
                session_string=row["session_string"],
                phone=row["phone"],
                label=row["label"] or f"Account +{row['phone']}",
            )
        return None


def delete_session(session_id: int, user_id: int) -> bool:
    """Delete user's session."""
    with _get_connection() as conn:
        # First check if session has active bridges
        bridges = conn.execute(
            "SELECT COUNT(*) as count FROM bridges WHERE session_id = ? AND is_active = 1",
            (session_id,),
        ).fetchone()
        if bridges["count"] > 0:
            return False  # Cannot delete session with active bridges
        
        cursor = conn.execute(
            "DELETE FROM sessions WHERE session_id = ? AND user_id = ?",
            (session_id, user_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def has_any_session(user_id: int) -> bool:
    """Check if user has at least one session."""
    with _get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as count FROM sessions WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return row["count"] > 0


def get_first_session(user_id: int) -> Optional[UserSession]:
    """Get user's first available session."""
    sessions = get_user_sessions(user_id)
    return sessions[0] if sessions else None


# Bridge management

def add_bridge(
    user_id: int,
    session_id: int,
    source_id: int,
    source_type: str,
    source_title: str,
    target_id: int,
    target_type: str,
    target_title: str,
    keywords: str = "",
    source_thread_id: int = 0,
    target_thread_id: int = 0,
) -> int:
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO bridges
            (user_id, session_id, source_id, source_type, source_title, target_id, target_type, target_title, keywords, source_thread_id, target_thread_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, session_id, source_id, source_type, source_title, target_id, target_type, target_title, keywords, source_thread_id, target_thread_id),
        )
        conn.commit()
        return cursor.lastrowid


def get_active_bridges() -> List[BridgeEntry]:
    with _get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM bridges WHERE is_active = 1"
        ).fetchall()
        return [BridgeEntry(**dict(row)) for row in rows]


def get_user_bridges(user_id: int) -> List[BridgeEntry]:
    with _get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM bridges WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
        return [BridgeEntry(**dict(row)) for row in rows]


def get_all_bridges() -> List[BridgeEntry]:
    with _get_connection() as conn:
        rows = conn.execute("SELECT * FROM bridges").fetchall()
        return [BridgeEntry(**dict(row)) for row in rows]


def get_bridge_by_source(source_id: int) -> Optional[BridgeEntry]:
    with _get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM bridges WHERE source_id = ? AND is_active = 1",
            (source_id,),
        ).fetchone()
        return BridgeEntry(**dict(row)) if row else None


def delete_bridge(bridge_id: int) -> bool:
    with _get_connection() as conn:
        cursor = conn.execute("DELETE FROM bridges WHERE id = ?", (bridge_id,))
        conn.commit()
        return cursor.rowcount > 0


def toggle_bridge(bridge_id: int, is_active: bool) -> bool:
    with _get_connection() as conn:
        cursor = conn.execute(
            "UPDATE bridges SET is_active = ? WHERE id = ?",
            (is_active, bridge_id),
        )
        conn.commit()
        return cursor.rowcount > 0


# Legacy migration
def migrate_old_user_data(user_id: int) -> Optional[int]:
    """Migrate old single-session data to new multi-session format."""
    with _get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE user_id = ? AND session_string IS NOT NULL",
            (user_id,),
        ).fetchone()
        if not row:
            return None
        
        # Check if already migrated
        existing = conn.execute(
            "SELECT session_id FROM sessions WHERE user_id = ? AND phone = ?",
            (user_id, row["phone"]),
        ).fetchone()
        if existing:
            return existing["session_id"]
        
        # Migrate to new sessions table
        cursor = conn.execute(
            """
            INSERT INTO sessions (user_id, api_id, api_hash, session_string, phone, label)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, row["api_id"], row["api_hash"], row["session_string"], row["phone"], "Основной аккаунт"),
        )
        conn.commit()
        return cursor.lastrowid
