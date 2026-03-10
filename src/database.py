import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

DB_PATH = Path("data/bridge.db")
ROUTE_RELOAD_PATH = Path("data/route_reload.signal")


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


@dataclass(frozen=True)
class TopicRule:
    id: int
    bridge_id: int
    source_chat_id: int
    source_type: str
    source_thread_id: int
    source_title: str
    target_chat_id: int
    target_thread_id: int
    target_title: str
    is_active: bool
    is_external: bool
    header_enabled: bool = True
    created_at: str = ""


@dataclass(frozen=True)
class TopicProposal:
    id: int
    bridge_id: int
    user_id: int
    session_id: int
    source_chat_id: int
    source_thread_id: int
    source_title: str
    bridge_source_id: int
    bridge_source_title: str
    bridge_target_id: int
    bridge_target_title: str
    status: str
    notified_at: str | None = None
    created_at: str = ""
    updated_at: str = ""


def init_db() -> None:
    DB_PATH.parent.mkdir(exist_ok=True)
    ROUTE_RELOAD_PATH.parent.mkdir(exist_ok=True)
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
        # Topic mapping table for forum bridges
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topic_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bridge_id INTEGER NOT NULL,
                source_thread_id INTEGER NOT NULL,
                target_thread_id INTEGER NOT NULL,
                topic_title TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(bridge_id, source_thread_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topic_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bridge_id INTEGER NOT NULL,
                source_chat_id INTEGER NOT NULL,
                source_type TEXT NOT NULL CHECK(source_type IN ('channel', 'chat', 'forum', 'topic')),
                source_thread_id INTEGER DEFAULT 0,
                source_title TEXT NOT NULL,
                target_chat_id INTEGER NOT NULL,
                target_thread_id INTEGER NOT NULL,
                target_title TEXT NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                is_external BOOLEAN DEFAULT 0,
                header_enabled BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(bridge_id, source_chat_id, source_thread_id, target_thread_id)
            )
            """
        )
        try:
            conn.execute("SELECT header_enabled FROM topic_rules LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE topic_rules ADD COLUMN header_enabled BOOLEAN DEFAULT 1")
            conn.commit()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS topic_proposals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bridge_id INTEGER NOT NULL,
                source_chat_id INTEGER NOT NULL,
                source_thread_id INTEGER NOT NULL,
                source_title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'accepted', 'dismissed')),
                notified_at TIMESTAMP NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(bridge_id, source_chat_id, source_thread_id)
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO topic_rules (
                bridge_id,
                source_chat_id,
                source_type,
                source_thread_id,
                source_title,
                target_chat_id,
                target_thread_id,
                target_title,
                is_active,
                is_external,
                header_enabled
            )
            SELECT
                tm.bridge_id,
                b.source_id,
                'topic',
                tm.source_thread_id,
                COALESCE(NULLIF(tm.topic_title, ''), 'Topic ' || tm.source_thread_id),
                b.target_id,
                tm.target_thread_id,
                COALESCE(NULLIF(tm.topic_title, ''), 'Topic ' || tm.source_thread_id),
                1,
                0,
                1
            FROM topic_mappings tm
            JOIN bridges b ON b.id = tm.bridge_id
            """
        )
        conn.commit()


def notify_route_reload() -> None:
    ROUTE_RELOAD_PATH.touch()


def get_route_reload_token() -> float:
    try:
        return ROUTE_RELOAD_PATH.stat().st_mtime
    except FileNotFoundError:
        return 0.0


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
        notify_route_reload()
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
        conn.execute("DELETE FROM topic_proposals WHERE bridge_id = ?", (bridge_id,))
        conn.execute("DELETE FROM topic_rules WHERE bridge_id = ?", (bridge_id,))
        conn.execute("DELETE FROM topic_mappings WHERE bridge_id = ?", (bridge_id,))
        cursor = conn.execute("DELETE FROM bridges WHERE id = ?", (bridge_id,))
        conn.commit()
        notify_route_reload()
        return cursor.rowcount > 0


def toggle_bridge(bridge_id: int, is_active: bool) -> bool:
    with _get_connection() as conn:
        cursor = conn.execute(
            "UPDATE bridges SET is_active = ? WHERE id = ?",
            (is_active, bridge_id),
        )
        conn.commit()
        notify_route_reload()
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


# Topic mapping for forum bridges

def get_topic_mapping(bridge_id: int, source_thread_id: int) -> Optional[tuple[int, str]]:
    """Get target thread ID and title for a source topic. Returns (target_thread_id, topic_title) or None."""
    with _get_connection() as conn:
        row = conn.execute(
            "SELECT target_thread_id, topic_title FROM topic_mappings WHERE bridge_id = ? AND source_thread_id = ?",
            (bridge_id, source_thread_id),
        ).fetchone()
        return (row["target_thread_id"], row["topic_title"]) if row else None


def create_topic_mapping(
    bridge_id: int, source_thread_id: int, target_thread_id: int, topic_title: str = ""
) -> None:
    """Create or update a topic mapping."""
    with _get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO topic_mappings (bridge_id, source_thread_id, target_thread_id, topic_title)
            VALUES (?, ?, ?, ?)
            """,
            (bridge_id, source_thread_id, target_thread_id, topic_title),
        )
        conn.commit()
        notify_route_reload()


def get_all_topic_mappings(bridge_id: int) -> List[tuple[int, int, str]]:
    """Get all topic mappings for a bridge. Returns list of (source_thread_id, target_thread_id, topic_title)."""
    with _get_connection() as conn:
        rows = conn.execute(
            "SELECT source_thread_id, target_thread_id, topic_title FROM topic_mappings WHERE bridge_id = ?",
            (bridge_id,),
        ).fetchall()
        return [(row["source_thread_id"], row["target_thread_id"], row["topic_title"]) for row in rows]


def add_topic_rule(
    bridge_id: int,
    source_chat_id: int,
    source_type: str,
    source_thread_id: int,
    source_title: str,
    target_chat_id: int,
    target_thread_id: int,
    target_title: str,
    is_external: bool = False,
) -> int:
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT OR REPLACE INTO topic_rules (
                bridge_id,
                source_chat_id,
                source_type,
                source_thread_id,
                source_title,
                target_chat_id,
                target_thread_id,
                target_title,
                is_active,
                is_external,
                header_enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 1)
            """,
            (
                bridge_id,
                source_chat_id,
                source_type,
                source_thread_id,
                source_title,
                target_chat_id,
                target_thread_id,
                target_title,
                int(is_external),
            ),
        )
        conn.commit()
        notify_route_reload()
        return cursor.lastrowid


def get_topic_rule(rule_id: int) -> Optional[TopicRule]:
    with _get_connection() as conn:
        row = conn.execute("SELECT * FROM topic_rules WHERE id = ?", (rule_id,)).fetchone()
        return TopicRule(**dict(row)) if row else None


def get_topic_rules_for_bridge(bridge_id: int, active_only: bool = False) -> List[TopicRule]:
    with _get_connection() as conn:
        query = "SELECT * FROM topic_rules WHERE bridge_id = ?"
        params: tuple[int, ...] = (bridge_id,)
        if active_only:
            query += " AND is_active = 1"
        query += " ORDER BY created_at ASC"
        rows = conn.execute(query, params).fetchall()
        return [TopicRule(**dict(row)) for row in rows]


def get_active_topic_rules() -> List[TopicRule]:
    with _get_connection() as conn:
        rows = conn.execute(
            """
            SELECT tr.*
            FROM topic_rules tr
            JOIN bridges b ON b.id = tr.bridge_id
            WHERE tr.is_active = 1 AND b.is_active = 1
            ORDER BY tr.created_at ASC
            """
        ).fetchall()
        return [TopicRule(**dict(row)) for row in rows]


def get_topic_rule_by_source(
    bridge_id: int, source_chat_id: int, source_thread_id: int
) -> Optional[TopicRule]:
    with _get_connection() as conn:
        row = conn.execute(
            """
            SELECT * FROM topic_rules
            WHERE bridge_id = ? AND source_chat_id = ? AND source_thread_id = ?
            """,
            (bridge_id, source_chat_id, source_thread_id),
        ).fetchone()
        return TopicRule(**dict(row)) if row else None


def toggle_topic_rule(rule_id: int, is_active: bool) -> bool:
    with _get_connection() as conn:
        cursor = conn.execute(
            "UPDATE topic_rules SET is_active = ? WHERE id = ?",
            (is_active, rule_id),
        )
        conn.commit()
        notify_route_reload()
        return cursor.rowcount > 0


def toggle_topic_rule_header(rule_id: int, header_enabled: bool) -> bool:
    with _get_connection() as conn:
        cursor = conn.execute(
            "UPDATE topic_rules SET header_enabled = ? WHERE id = ?",
            (header_enabled, rule_id),
        )
        conn.commit()
        notify_route_reload()
        return cursor.rowcount > 0


def delete_topic_rule(rule_id: int) -> bool:
    with _get_connection() as conn:
        cursor = conn.execute("DELETE FROM topic_rules WHERE id = ?", (rule_id,))
        conn.commit()
        notify_route_reload()
        return cursor.rowcount > 0


def _topic_proposal_from_row(row) -> TopicProposal:
    return TopicProposal(
        id=row["id"],
        bridge_id=row["bridge_id"],
        user_id=row["user_id"],
        session_id=row["session_id"],
        source_chat_id=row["source_chat_id"],
        source_thread_id=row["source_thread_id"],
        source_title=row["source_title"],
        bridge_source_id=row["bridge_source_id"],
        bridge_source_title=row["bridge_source_title"],
        bridge_target_id=row["bridge_target_id"],
        bridge_target_title=row["bridge_target_title"],
        status=row["status"],
        notified_at=row["notified_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def get_topic_proposal(proposal_id: int) -> Optional[TopicProposal]:
    with _get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                tp.*,
                b.user_id,
                b.session_id,
                b.source_id AS bridge_source_id,
                b.source_title AS bridge_source_title,
                b.target_id AS bridge_target_id,
                b.target_title AS bridge_target_title
            FROM topic_proposals tp
            JOIN bridges b ON b.id = tp.bridge_id
            WHERE tp.id = ?
            """,
            (proposal_id,),
        ).fetchone()
        return _topic_proposal_from_row(row) if row else None


def get_topic_proposal_by_source(
    bridge_id: int,
    source_chat_id: int,
    source_thread_id: int,
) -> Optional[TopicProposal]:
    with _get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                tp.*,
                b.user_id,
                b.session_id,
                b.source_id AS bridge_source_id,
                b.source_title AS bridge_source_title,
                b.target_id AS bridge_target_id,
                b.target_title AS bridge_target_title
            FROM topic_proposals tp
            JOIN bridges b ON b.id = tp.bridge_id
            WHERE tp.bridge_id = ? AND tp.source_chat_id = ? AND tp.source_thread_id = ?
            """,
            (bridge_id, source_chat_id, source_thread_id),
        ).fetchone()
        return _topic_proposal_from_row(row) if row else None


def create_topic_proposal(
    bridge_id: int,
    source_chat_id: int,
    source_thread_id: int,
    source_title: str,
) -> tuple[TopicProposal, bool]:
    with _get_connection() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO topic_proposals (
                bridge_id,
                source_chat_id,
                source_thread_id,
                source_title
            )
            VALUES (?, ?, ?, ?)
            """,
            (bridge_id, source_chat_id, source_thread_id, source_title),
        )
        conn.commit()
        row = conn.execute("SELECT changes() AS changes").fetchone()
        proposal_row = conn.execute(
            """
            SELECT
                tp.*,
                b.user_id,
                b.session_id,
                b.source_id AS bridge_source_id,
                b.source_title AS bridge_source_title,
                b.target_id AS bridge_target_id,
                b.target_title AS bridge_target_title
            FROM topic_proposals tp
            JOIN bridges b ON b.id = tp.bridge_id
            WHERE tp.bridge_id = ? AND tp.source_chat_id = ? AND tp.source_thread_id = ?
            """,
            (bridge_id, source_chat_id, source_thread_id),
        ).fetchone()
        proposal = _topic_proposal_from_row(proposal_row) if proposal_row else None
        if not proposal:
            raise RuntimeError("Failed to load topic proposal after insert")
        return proposal, bool(row["changes"])


def mark_topic_proposal_notified(proposal_id: int) -> bool:
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE topic_proposals
            SET notified_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND status = 'pending'
            """,
            (proposal_id,),
        )
        conn.commit()
        return cursor.rowcount > 0


def update_topic_proposal_status(proposal_id: int, status: str) -> bool:
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE topic_proposals
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (status, proposal_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def get_pending_topic_proposals_for_user(user_id: int) -> List[TopicProposal]:
    with _get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                tp.*,
                b.user_id,
                b.session_id,
                b.source_id AS bridge_source_id,
                b.source_title AS bridge_source_title,
                b.target_id AS bridge_target_id,
                b.target_title AS bridge_target_title
            FROM topic_proposals tp
            JOIN bridges b ON b.id = tp.bridge_id
            WHERE b.user_id = ? AND tp.status = 'pending'
            ORDER BY tp.created_at ASC
            """,
            (user_id,),
        ).fetchall()
        return [_topic_proposal_from_row(row) for row in rows]
