import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

DB_PATH = Path("data/bridge.db")


@dataclass(frozen=True)
class BridgeEntry:
    id: int
    source_id: int
    source_type: str  # 'channel' or 'chat'
    source_title: str
    target_id: int
    target_type: str  # 'channel' or 'chat'
    target_title: str
    keywords: str
    is_active: bool


@dataclass(frozen=True)
class UserCredentials:
    user_id: int
    api_id: int
    api_hash: str
    session_string: str
    phone: str


def init_db() -> None:
    DB_PATH.parent.mkdir(exist_ok=True)
    with _get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bridges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                source_id INTEGER NOT NULL,
                source_type TEXT NOT NULL CHECK(source_type IN ('channel', 'chat')),
                source_title TEXT NOT NULL,
                target_id INTEGER NOT NULL,
                target_type TEXT NOT NULL CHECK(target_type IN ('channel', 'chat')),
                target_title TEXT NOT NULL,
                keywords TEXT DEFAULT '',
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
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


def add_bridge(
    user_id: int,
    source_id: int,
    source_type: str,
    source_title: str,
    target_id: int,
    target_type: str,
    target_title: str,
    keywords: str = "",
) -> int:
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO bridges
            (user_id, source_id, source_type, source_title, target_id, target_type, target_title, keywords)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, source_id, source_type, source_title, target_id, target_type, target_title, keywords),
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


# User credentials functions

def save_user_credentials(user_id: int, api_id: int, api_hash: str, phone: str) -> None:
    with _get_connection() as conn:
        conn.execute(
            """
            INSERT INTO users (user_id, api_id, api_hash, phone)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                api_id = excluded.api_id,
                api_hash = excluded.api_hash,
                phone = excluded.phone
            """,
            (user_id, api_id, api_hash, phone),
        )
        conn.commit()


def save_session_string(user_id: int, session_string: str) -> None:
    with _get_connection() as conn:
        conn.execute(
            "UPDATE users SET session_string = ? WHERE user_id = ?",
            (session_string, user_id),
        )
        conn.commit()


def get_user_credentials(user_id: int) -> Optional[UserCredentials]:
    with _get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if row:
            return UserCredentials(
                user_id=row["user_id"],
                api_id=row["api_id"],
                api_hash=row["api_hash"],
                session_string=row["session_string"] or "",
                phone=row["phone"] or "",
            )
        return None


def has_user_session(user_id: int) -> bool:
    creds = get_user_credentials(user_id)
    return creds is not None and bool(creds.session_string)
