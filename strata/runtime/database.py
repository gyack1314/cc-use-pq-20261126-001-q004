"""保存目录并提供接收、修复和回收所用的事务边界。"""

from contextlib import contextmanager
from pathlib import Path
import sqlite3
import time
from collections.abc import Callable, Iterator

from strata.runtime.errors import unavailable

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS collections(
        name TEXT PRIMARY KEY,
        retention_seconds INTEGER NOT NULL CHECK(retention_seconds > 0),
        head INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS deposits(
        id TEXT PRIMARY KEY,
        collection TEXT NOT NULL REFERENCES collections(name),
        token TEXT NOT NULL,
        parent INTEGER NOT NULL,
        manifest TEXT NOT NULL,
        manifest_hash TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('open','published','canceled','expired')),
        revision INTEGER,
        UNIQUE(collection,token),
        CHECK((status = 'published') = (revision IS NOT NULL))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS expected_parts(
        deposit TEXT NOT NULL REFERENCES deposits(id),
        digest TEXT NOT NULL,
        size INTEGER NOT NULL,
        PRIMARY KEY(deposit,digest)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS objects(
        digest TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        added_at INTEGER NOT NULL,
        verified_at INTEGER NOT NULL,
        sweep_state TEXT NOT NULL DEFAULT 'live' CHECK(sweep_state IN ('live','marked')),
        marked_at INTEGER,
        CHECK((sweep_state = 'marked') = (marked_at IS NOT NULL))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS received_parts(
        deposit TEXT NOT NULL,
        digest TEXT NOT NULL REFERENCES objects(digest),
        received_at INTEGER NOT NULL,
        PRIMARY KEY(deposit,digest),
        FOREIGN KEY(deposit,digest) REFERENCES expected_parts(deposit,digest)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS revisions(
        collection TEXT NOT NULL REFERENCES collections(name),
        number INTEGER NOT NULL,
        deposit TEXT NOT NULL UNIQUE REFERENCES deposits(id),
        parent INTEGER NOT NULL,
        manifest_hash TEXT NOT NULL,
        manifest TEXT NOT NULL,
        published_at INTEGER NOT NULL,
        retain_until INTEGER NOT NULL,
        retired_at INTEGER,
        PRIMARY KEY(collection,number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS revision_parts(
        collection TEXT NOT NULL,
        number INTEGER NOT NULL,
        digest TEXT NOT NULL,
        PRIMARY KEY(collection,number,digest),
        FOREIGN KEY(collection,number) REFERENCES revisions(collection,number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS holds(
        collection TEXT NOT NULL,
        number INTEGER NOT NULL,
        reference TEXT NOT NULL,
        reason TEXT NOT NULL,
        placed_at INTEGER NOT NULL,
        released_at INTEGER,
        PRIMARY KEY(collection,number,reference),
        FOREIGN KEY(collection,number) REFERENCES revisions(collection,number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS incidents(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        digest TEXT NOT NULL,
        opened_at INTEGER NOT NULL,
        observed_at INTEGER NOT NULL,
        reason TEXT NOT NULL,
        resolved_at INTEGER,
        resolution TEXT
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS active_incident ON incidents(digest) WHERE resolved_at IS NULL",
    "CREATE INDEX IF NOT EXISTS revisions_by_digest ON revision_parts(digest,collection,number)",
    "CREATE INDEX IF NOT EXISTS received_by_digest ON received_parts(digest,deposit)",
    "CREATE INDEX IF NOT EXISTS deposit_expiry ON deposits(status,expires_at,id)",
    "CREATE INDEX IF NOT EXISTS sweep_age ON objects(sweep_state,added_at,digest)",
    """
    CREATE TABLE IF NOT EXISTS events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT REFERENCES collections(name),
        kind TEXT NOT NULL,
        entity TEXT NOT NULL,
        at INTEGER NOT NULL,
        payload TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS events_by_collection ON events(collection,id)",
    """
    CREATE TABLE IF NOT EXISTS relay_points(
        point TEXT PRIMARY KEY,
        label TEXT,
        created_at INTEGER NOT NULL,
        paused_at INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS relay_handoffs(
        id TEXT PRIMARY KEY,
        collection TEXT NOT NULL REFERENCES collections(name),
        deposit TEXT NOT NULL REFERENCES deposits(id),
        token TEXT NOT NULL,
        point TEXT NOT NULL,
        parent INTEGER NOT NULL,
        manifest_hash TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS relay_handoff_parts(
        handoff TEXT NOT NULL REFERENCES relay_handoffs(id),
        digest TEXT NOT NULL,
        size INTEGER NOT NULL,
        declared_at INTEGER NOT NULL,
        PRIMARY KEY(handoff,digest)
    )
    """,
    "CREATE INDEX IF NOT EXISTS relay_handoffs_by_deposit ON relay_handoffs(deposit,id)",
    "CREATE INDEX IF NOT EXISTS relay_handoffs_by_batch ON relay_handoffs(collection,token,id)",
    "CREATE INDEX IF NOT EXISTS relay_handoff_parts_by_digest ON relay_handoff_parts(digest,handoff)",
)

# 代收点本地台账使用的表，随代收点目录独立建库。
AGENT_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS agent_state(
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_receipts(
        id TEXT PRIMARY KEY,
        point TEXT NOT NULL,
        collection TEXT NOT NULL,
        token TEXT NOT NULL,
        parent INTEGER NOT NULL,
        manifest_hash TEXT NOT NULL,
        manifest TEXT,
        lifetime_seconds INTEGER,
        status TEXT NOT NULL DEFAULT 'valid',
        created_at INTEGER NOT NULL,
        UNIQUE(point,collection,token,manifest_hash)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_parts(
        collection TEXT NOT NULL,
        token TEXT NOT NULL,
        digest TEXT NOT NULL,
        size INTEGER NOT NULL,
        receipt TEXT REFERENCES agent_receipts(id),
        promised INTEGER NOT NULL DEFAULT 1,
        stored_at INTEGER NOT NULL,
        confirmed_at INTEGER,
        handoff_id TEXT,
        released_at INTEGER,
        PRIMARY KEY(collection,token,digest)
    )
    """,
    "CREATE INDEX IF NOT EXISTS agent_parts_by_digest ON agent_parts(digest,released_at)",
    "CREATE INDEX IF NOT EXISTS agent_parts_due ON agent_parts(stored_at,released_at)",
    """
    CREATE TABLE IF NOT EXISTS agent_peer_copies(
        collection TEXT NOT NULL,
        token TEXT NOT NULL,
        digest TEXT NOT NULL,
        point TEXT NOT NULL,
        handoff_id TEXT,
        available INTEGER NOT NULL DEFAULT 1,
        observed_at INTEGER NOT NULL,
        PRIMARY KEY(collection,token,digest,point)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS agent_handoffs(
        collection TEXT NOT NULL,
        token TEXT NOT NULL,
        handoff_id TEXT NOT NULL,
        deposit TEXT,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY(collection,token)
    )
    """,
)


class Database:
    def __init__(self, path: Path, clock: Callable[[], int] | None = None):
        self.path = path
        self.clock = clock or (lambda: int(time.time()))
        path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise RuntimeError(f"Unsupported vault catalog version {version}")
            if version == 0:
                for statement in SCHEMA:
                    connection.execute(statement)
                connection.execute("PRAGMA user_version=1")
            else:
                # 旧版本库上幂等补建代收转交表。
                for statement in SCHEMA:
                    if statement.lstrip().startswith("CREATE TABLE IF NOT EXISTS relay_") or \
                            statement.lstrip().startswith("CREATE INDEX IF NOT EXISTS relay_"):
                        connection.execute(statement)

    def now(self) -> int:
        return self.clock()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def transaction(self, *, write: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield connection
            connection.commit()
        except sqlite3.OperationalError as error:
            connection.rollback()
            if getattr(error, "sqlite_errorcode", 0) & 255 in (
                sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED
            ):
                raise unavailable("Archive is busy, retry the operation") from error
            raise
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
