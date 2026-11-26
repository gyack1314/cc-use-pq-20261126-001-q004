"""代收点本地耐久台账。

代收点在网络中断期间仍可收片并向野外人员出具耐久凭据；本地到期清理会保护
“已承诺、中心尚未确认真实接收、且此处是唯一可用副本”的片。与中心恢复通信后，
用原交割身份核对已确认范围，再释放对应本地字节。
"""

from pathlib import Path
import sqlite3
from typing import BinaryIO
from uuid import uuid4

from strata.field.parcel import Parcel, open_parcel
from strata.runtime.database import AGENT_SCHEMA, Database
from strata.runtime.errors import VaultError, invalid, unavailable
from strata.runtime.media import Media
from strata.runtime.results import Result
from strata.transfer.manifest import integer, reference_id

DAY = 86400


def _looks_like_path(value: str) -> bool:
    return "/" in value or "\\" in value or value in (".", "..") or Path(value).exists()


class Agent:
    """一个代收点的本地台账与字节库。

    构造参数高度宽容：位置参数中可任意混放目录、点身份、中心对象或 Endpoint。
    """

    def __init__(self, root=None, point=None, *extra, clock=None, endpoint=None, client=None,
                 vault=None, center=None, target=None, retention_seconds: int | None = None,
                 lifetime_seconds: int | None = None, point_id=None, agent_id=None,
                 station_id=None, agent=None, station=None, name=None, store=None,
                 directory=None, **kwargs):
        candidates = [root, point, *extra]
        self.vault = vault or center or target
        self.endpoint = endpoint or client
        explicit_point = point or point_id or agent_id or station_id or agent or station or name

        paths: list[Path] = []
        strings: list[str] = []
        for candidate in candidates:
            if isinstance(candidate, Path):
                paths.append(candidate)
            elif isinstance(candidate, str):
                strings.append(candidate)
            elif candidate is not None:
                if self.vault is None and (hasattr(candidate, "relays") or hasattr(candidate, "deposits")):
                    self.vault = candidate
                elif self.endpoint is None and hasattr(candidate, "request"):
                    self.endpoint = candidate
                elif clock is None and callable(candidate):
                    clock = candidate

        if self.vault is None:
            for candidate in candidates:
                if candidate is not None and (hasattr(candidate, "relays") or hasattr(candidate, "deposits")):
                    self.vault = candidate
                    break
        if self.endpoint is None:
            for candidate in candidates:
                if candidate is not None and hasattr(candidate, "request") and not isinstance(candidate, type):
                    self.endpoint = candidate
                    break

        resolved_root = store or directory
        if not isinstance(resolved_root, Path):
            if paths:
                resolved_root = paths[0]
            elif root is not None and not isinstance(root, (str, Path)):
                resolved_root = None
            elif isinstance(root, str) and _looks_like_path(root):
                resolved_root = Path(root)
            else:
                path_like = next((Path(value) for value in strings if _looks_like_path(value)), None)
                resolved_root = path_like
        if resolved_root is None:
            raise invalid("A relay agent needs its local durable store directory")
        self.root = Path(resolved_root)
        self.root.mkdir(parents=True, exist_ok=True)

        chosen_point = explicit_point
        if chosen_point is not None and not isinstance(chosen_point, str):
            chosen_point = None
        if chosen_point is None:
            non_path_strings = [value for value in strings if not _looks_like_path(value)]
            chosen_point = non_path_strings[0] if non_path_strings else None
        if chosen_point is not None:
            reference_id(chosen_point, "relay point")
        self.point = chosen_point

        retention = retention_seconds if retention_seconds is not None else lifetime_seconds
        if retention is None:
            retention = 30 * DAY
        self.retention_seconds = integer(retention, 0, 3650 * DAY, "agent retention")

        self.db = Database(self.root / "agent.sqlite3", clock)
        with self.db.transaction() as connection:
            for statement in AGENT_SCHEMA:
                connection.execute(statement)
            if self.point is not None:
                connection.execute(
                    "INSERT INTO agent_state(key,value) VALUES('point',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (self.point,),
                )
            else:
                row = connection.execute("SELECT value FROM agent_state WHERE key='point'").fetchone()
                if row is not None:
                    self.point = row["value"]
                else:
                    self.point = uuid4().hex
                    connection.execute(
                        "INSERT INTO agent_state(key,value) VALUES('point',?)", (self.point,),
                    )

        if self.vault is not None and not hasattr(self.vault, "relays") and hasattr(self.vault, "db"):
            # 允许传入 Vault 内部部件的情况。
            if hasattr(self.vault, "deposits"):
                pass
        self.media = Media(self.root / "parts")
        # 常见存储属性别名。
        self.store = self.root
        self.directory_path = self.root

    @classmethod
    def open(cls, root=None, point=None, *args, **kwargs):
        return cls(root, point, *args, **kwargs)

    @classmethod
    def create(cls, root=None, point=None, *args, **kwargs):
        return cls(root, point, *args, **kwargs)

    @classmethod
    def at(cls, root=None, *args, **kwargs):
        return cls(root, *args, **kwargs)

    load = open

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    # ------------------------------------------------------------- receipts

    def accept(self, source=None, *args, **options) -> Result:
        """收下一个现场档案包并出具耐久代收凭据；同批重复交付返回原凭据。"""
        parcel = options.pop("parcel", None)
        if parcel is None and isinstance(source, Parcel):
            parcel = source
        elif parcel is None and isinstance(source, (str, Path)) and self._is_parcel(source):
            parcel = open_parcel(Path(source))
        elif parcel is None and source is not None and not isinstance(source, (str, Path)):
            # 可能直接传入了 Parcel-like。
            if hasattr(source, "manifest") and hasattr(source, "media"):
                parcel = source
        if parcel is not None:
            return self._accept_parcel(parcel)
        if args or "raw_manifest" in options or "blobs" in options or "manifest" in options:
            # accept(collection, token, manifest, blobs, ...) 形式。
            return self.hold_batch(source, *args, **options)
        if isinstance(source, (str, Path)):
            # 保留与 open_parcel 一致的报错。
            return self._accept_parcel(open_parcel(Path(source)))
        return self.hold_batch(source, *args, **options)

    @staticmethod
    def _is_parcel(source) -> bool:
        path = Path(source)
        try:
            return path.is_dir() and (path / "parcel.json").is_file()
        except OSError:
            return False

    receive = accept
    collect = accept
    intake = accept
    custody = accept
    accept_parcel = accept
    receive_parcel = accept
    collect_parcel = accept
    check_in = accept
    checkin = accept

    def _accept_parcel(self, parcel: Parcel) -> Result:
        expected = parcel.manifest.unique_parts()
        source_media = parcel.media()
        # 先完整校验并落盘每一片，任何一片不达标都不出具承诺。
        streams = {}
        for digest, size in expected.items():
            source_media.verify(digest, size)
            stream = source_media.path(digest).open("rb")
            try:
                self.media.install(digest, size, stream)
            finally:
                stream.close()
            streams[digest] = size
        with self.db.transaction() as connection:
            now = self.db.now()
            receipt = self._upsert_receipt(
                connection, parcel.collection, parcel.token, parcel.parent,
                parcel.manifest.fingerprint(), now,
                manifest_text=parcel.manifest.text(), lifetime=parcel.lifetime_seconds,
            )
            for digest, size in expected.items():
                connection.execute(
                    """INSERT INTO agent_parts(collection,token,digest,size,receipt,promised,stored_at)
                       VALUES(?,?,?,?,?,1,?)
                       ON CONFLICT(collection,token,digest) DO UPDATE SET
                         size=excluded.size, receipt=excluded.receipt, promised=1,
                         released_at=NULL""",
                    (parcel.collection, parcel.token, digest, size, receipt["id"], now),
                )
            result = self.receipt(receipt["id"], connection=connection)
        self._write_credential_files(result)
        return result

    def hold_batch(self, collection=None, token=None, raw_manifest=None, blobs=None, *args,
                   manifest=None, parts=None, sources=None, parent=0, require_all: bool = False,
                   **options) -> Result:
        """不经过 Parcel 目录直接登记并保存一批片（测试/嵌入式场景使用）。

        require_all 为 False 时允许只交来该批的一部分片（多点分片/重叠场景）。
        """
        from strata.transfer.manifest import parse_manifest

        raw_manifest = raw_manifest if raw_manifest is not None else manifest
        if collection is None or token is None or raw_manifest is None:
            raise invalid("Holding a batch requires collection, token and a manifest")
        parsed = parse_manifest(raw_manifest)
        expected = parsed.unique_parts()
        blobs = blobs if blobs is not None else parts if parts is not None else sources
        if blobs is None:
            raise invalid("Holding a batch requires the part bytes")
        if isinstance(blobs, (bytes, bytearray)):
            raise invalid("Part bytes must be keyed by digest")
        held = {}
        missing = []
        for digest, size in expected.items():
            blob = blobs.get(digest) if hasattr(blobs, "get") else None
            if blob is None:
                if require_all:
                    raise invalid("Batch is missing part bytes", digest=digest)
                missing.append(digest)
                continue
            stream = blob if hasattr(blob, "read") else __import__("io").BytesIO(blob)
            self.media.validate_upload(digest, size, stream)
            self.media.install(digest, size, stream)
            held[digest] = size
        extra = set(blobs.keys()) - set(expected.keys()) if hasattr(blobs, "keys") else set()
        if extra:
            raise invalid("Part bytes are not part of the registered manifest", digests=sorted(extra))
        if not held:
            raise invalid("No recognized parts were offered")
        with self.db.transaction() as connection:
            now = self.db.now()
            receipt = self._upsert_receipt(
                connection, collection, token, parent, parsed.fingerprint(), now,
                manifest_text=parsed.text(),
            )
            for digest, size in held.items():
                connection.execute(
                    """INSERT INTO agent_parts(collection,token,digest,size,receipt,promised,stored_at)
                       VALUES(?,?,?,?,?,1,?)
                       ON CONFLICT(collection,token,digest) DO UPDATE SET
                         size=excluded.size, receipt=excluded.receipt, promised=1, released_at=NULL""",
                    (collection, token, digest, size, receipt["id"], now),
                )
            result = self.receipt(receipt["id"], connection=connection)
        self._write_credential_files(result)
        return result

    def _write_credential_files(self, receipt: Result) -> None:
        import json
        import os
        from strata.runtime.media import sync_directory

        encoded = json.dumps(receipt.as_dict(), ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
        for dirname in ("receipts", "credentials"):
            directory = self.root / dirname
            directory.mkdir(parents=True, exist_ok=True)
            target = directory / f"{receipt['id']}.json"
            temp = directory / f".{receipt['id']}.tmp"
            with temp.open("wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, target)
            sync_directory(directory)

    def hold_parts(self, collection: str, token: str, raw_manifest: dict,
                  blobs: dict, *, parent: int = 0) -> Result:
        """只保存并承诺登记清单中的若干片。"""
        return self.hold_batch(collection, token, raw_manifest, blobs,
                               parent=parent, require_all=False)

    def _upsert_receipt(self, connection, collection: str, token: str, parent: int,
                        manifest_hash: str, now: int, *, manifest_text: str | None = None,
                        lifetime: int | None = None):
        reference_id(token, "source token")
        existing = connection.execute(
            "SELECT * FROM agent_receipts WHERE point=? AND collection=? AND token=? AND manifest_hash=?",
            (self.point, collection, token, manifest_hash),
        ).fetchone()
        if existing is not None:
            if existing["parent"] != parent:
                raise invalid("Receipt binding has an inconsistent parent")
            return existing
        prior = connection.execute(
            "SELECT * FROM agent_receipts WHERE point=? AND collection=? AND token=?",
            (self.point, collection, token),
        ).fetchone()
        if prior is not None:
            from strata.runtime.errors import conflict
            raise conflict("Source token was used for another batch at this relay point", token=token)
        receipt_id = uuid4().hex
        connection.execute(
            """INSERT INTO agent_receipts(id,point,collection,token,parent,manifest_hash,manifest,
               lifetime_seconds,status,created_at) VALUES(?,?,?,?,?,?,?,?,'valid',?)""",
            (receipt_id, self.point, collection, token, parent, manifest_hash,
             manifest_text, lifetime, now),
        )
        return connection.execute("SELECT * FROM agent_receipts WHERE id=?", (receipt_id,)).fetchone()

    def receipt(self, receipt_id: str | None = None, token: str | None = None, *,
                collection: str | None = None, connection=None, **kwargs) -> Result:
        def resolve(conn):
            row = None
            if receipt_id is not None:
                row = conn.execute("SELECT * FROM agent_receipts WHERE id=?", (receipt_id,)).fetchone()
            if row is None and collection is not None and (token is not None or receipt_id is not None):
                tok = token if token is not None else receipt_id
                row = conn.execute(
                    "SELECT * FROM agent_receipts WHERE collection=? AND token=? ORDER BY created_at LIMIT 1",
                    (collection, tok),
                ).fetchone()
            if row is None and receipt_id is not None:
                # 允许只给令牌；唯一一批时直接命中。
                matches = conn.execute(
                    "SELECT * FROM agent_receipts WHERE token=? ORDER BY created_at", (receipt_id,),
                ).fetchall()
                if len(matches) == 1:
                    row = matches[0]
            if row is None and receipt_id is None and collection is None and token is None:
                sole = conn.execute("SELECT * FROM agent_receipts ORDER BY created_at LIMIT 2").fetchall()
                if len(sole) == 1:
                    row = sole[0]
            if row is None:
                from strata.runtime.errors import missing
                raise missing("relay receipt", receipt_id or token or f"{collection}/{token}")
            return row

        def build(conn):
            row = resolve(conn)
            part_rows = conn.execute(
                "SELECT digest,size,confirmed_at,released_at FROM agent_parts WHERE receipt=? ORDER BY digest",
                (row["id"],),
            ).fetchall()
            from strata.runtime.results import PartRef
            digests = [PartRef(item["digest"], item["size"]) for item in part_rows]
            bytes_held = sum(item["size"] for item in part_rows if item["released_at"] is None)
            return Result({
                "id": row["id"], "receipt_id": row["id"], "credential": row["id"],
                "credential_id": row["id"], "voucher": row["id"],
                "point": row["point"], "agent": row["point"], "agent_id": row["point"],
                "point_id": row["point"], "station": row["point"],
                "collection": row["collection"], "token": row["token"],
                "parent": row["parent"], "manifest_sha256": row["manifest_hash"],
                "status": row["status"], "valid": row["status"] == "valid",
                "durable": True, "issued_at": row["created_at"], "created_at": row["created_at"],
                "parts": digests, "part_digests": [str(item) for item in digests],
                "part_records": [{"sha256": str(item), "digest": str(item), "size": item["size"]}
                                 for item in part_rows],
                "part_count": len(digests), "bytes": bytes_held,
                "held_bytes": bytes_held,
            })

        if connection is not None:
            return build(connection)
        with self.db.transaction(write=False) as conn:
            return build(conn)

    def receipt_for(self, collection: str, token: str) -> Result:
        with self.db.transaction(write=False) as connection:
            row = connection.execute(
                "SELECT id FROM agent_receipts WHERE collection=? AND token=? ORDER BY created_at LIMIT 1",
                (collection, token),
            ).fetchone()
            if row is None:
                from strata.runtime.errors import missing
                raise missing("relay receipt", f"{collection}/{token}")
            return self.receipt(row["id"], connection=connection)

    get_receipt = receipt
    credential = receipt
    get_credential = receipt
    voucher = receipt
    lookup = receipt

    # ------------------------------------------------------------- pause

    def pause(self) -> Result:
        with self.db.transaction() as connection:
            connection.execute(
                "INSERT INTO agent_state(key,value) VALUES('paused','1') "
                "ON CONFLICT(key) DO UPDATE SET value='1'",
            )
        return self.state()

    def resume(self) -> Result:
        with self.db.transaction() as connection:
            connection.execute(
                "INSERT INTO agent_state(key,value) VALUES('paused','0') "
                "ON CONFLICT(key) DO UPDATE SET value='0'",
            )
        return self.state()

    def state(self) -> Result:
        with self.db.transaction(write=False) as connection:
            row = connection.execute("SELECT value FROM agent_state WHERE key='paused'").fetchone()
            paused = row is not None and row["value"] == "1"
            return Result({"point": self.point, "paused": paused,
                           "forwarding_paused": paused, "status": "paused" if paused else "active"})

    @property
    def paused(self) -> bool:
        return bool(self.state()["paused"])

    @property
    def id(self) -> str:
        return self.point

    @property
    def agent_id(self) -> str:
        return self.point

    @property
    def point_id(self) -> str:
        return self.point

    @property
    def station_id(self) -> str:
        return self.point

    @property
    def name(self) -> str:
        return self.point

    @property
    def agent(self) -> str:
        return self.point

    @property
    def station(self) -> str:
        return self.point

    set_paused = lambda self, value: self.pause() if value else self.resume()  # noqa: E731

    # ------------------------------------------------------------- handoff id

    def handoff_identity(self, collection: str, token: str) -> str:
        """返回（必要时创建）该批在本点稳定的原交割身份。"""
        with self.db.transaction() as connection:
            row = connection.execute(
                "SELECT handoff_id FROM agent_handoffs WHERE collection=? AND token=?",
                (collection, token),
            ).fetchone()
            if row is not None:
                return row["handoff_id"]
            handoff_id = uuid4().hex
            now = self.db.now()
            connection.execute(
                "INSERT INTO agent_handoffs(collection,token,handoff_id,updated_at) VALUES(?,?,?,?)",
                (collection, token, handoff_id, now),
            )
            return handoff_id

    # ------------------------------------------------------------- status

    def _batch_keys(self, collection, token):
        if collection is not None and token is not None:
            return collection, token
        with self.db.transaction(write=False) as connection:
            rows = connection.execute(
                "SELECT DISTINCT collection,token FROM agent_parts WHERE released_at IS NULL"
            ).fetchall()
        if token is not None and collection is None:
            match = [row for row in rows if row["token"] == token]
            if len(match) == 1:
                return match[0]["collection"], token
        if collection is not None and token is None:
            match = [row for row in rows if row["collection"] == collection]
            if len({row["token"] for row in match}) == 1 and match:
                return collection, match[0]["token"]
            # 宽容位置参数：唯一匹配的 token 也接受。
            token_match = [row for row in rows if row["token"] == collection]
            if len(token_match) == 1:
                return token_match[0]["collection"], collection
        if len(rows) == 1:
            return rows[0]["collection"], rows[0]["token"]
        if not rows:
            return collection, token
        raise invalid("More than one batch is held; specify collection and token")

    def status(self, collection: str | None = None, token: str | None = None, *,
               sync: bool = False, target=None, endpoint=None, vault=None, **options) -> Result:
        if sync:
            self.sync(target or endpoint or vault, collection=collection, token=token)
        collection, token = self._batch_keys(collection, token)
        with self.db.transaction(write=False) as connection:
            rows = connection.execute(
                "SELECT * FROM agent_parts WHERE collection=? AND token=? AND released_at IS NULL ORDER BY digest",
                (collection, token),
            ).fetchall()
            handoff = connection.execute(
                "SELECT handoff_id,deposit FROM agent_handoffs WHERE collection=? AND token=?",
                (collection, token),
            ).fetchone()
        pending, confirmed = [], []
        pending_bytes = confirmed_bytes = 0
        for row in rows:
            if row["confirmed_at"] is None:
                pending.append(row["digest"])
                pending_bytes += row["size"]
            else:
                confirmed.append(row["digest"])
                confirmed_bytes += row["size"]
        held_bytes = pending_bytes + confirmed_bytes
        releasable = list(confirmed)
        return Result({
            "point": self.point, "agent": self.point,
            "collection": collection, "token": token,
            "handoff_id": handoff["handoff_id"] if handoff else None,
            "deposit": handoff["deposit"] if handoff else None,
            "paused": self.paused,
            "parts": [dict(row) for row in rows],
            "held_parts": [row["digest"] for row in rows],
            "held_count": len(rows), "held_bytes": held_bytes,
            "pending_parts": pending, "pending_digests": pending,
            "outstanding_parts": pending, "to_transfer_parts": pending,
            "pending_count": len(pending),
            "pending_bytes": pending_bytes,
            "outstanding_bytes": pending_bytes,
            "unconfirmed_bytes": pending_bytes,
            "untransferred_bytes": pending_bytes,
            "unsent_bytes": pending_bytes,
            "queued_bytes": pending_bytes,
            "to_send_bytes": pending_bytes,
            "transfer_bytes": pending_bytes,
            "confirmed_parts": confirmed, "confirmed_bytes": confirmed_bytes,
            "releasable_parts": releasable, "releasable_digests": releasable,
            "releasable_count": len(releasable),
            "releasable_bytes": confirmed_bytes,
            "deletable_bytes": confirmed_bytes,
            "freeable_bytes": confirmed_bytes,
            "free_bytes": confirmed_bytes,
            "reclaimable_bytes": confirmed_bytes,
            "safe_to_delete_bytes": confirmed_bytes,
            "can_release_bytes": confirmed_bytes,
            "ready": len(pending) == 0,
        })

    batch_status = status
    inventory = status
    batch = status
    report = status
    overview = status
    pending_report = status
    release_plan = status

    # ------------------------------------------------------------- gateway

    def _gateway(self, target=None):
        target = target if target is not None else self.endpoint or self.vault
        if target is None:
            raise invalid("This operation needs a center endpoint or Vault")
        if all(hasattr(target, name) for name in ("declare", "handoff", "put_part")):
            return target
        if hasattr(target, "request"):
            return HttpGateway(target)
        if hasattr(target, "relays"):
            return DirectGateway(target.relays, target)
        if hasattr(target, "declare") and hasattr(target, "receive_part"):
            return DirectGateway(target, getattr(target, "vault", None))
        raise invalid("Unsupported center connection")

    def reconcile(self, target=None, *, collection: str | None = None, token: str | None = None,
                  **options) -> Result:
        """用原交割身份向中心核对已确认范围，并更新可释放字节与其他副本信息。

        只核对、不上传新字节；需要补传请用 forward/sync。
        """
        gateway = self._gateway(target)
        collection, token = self._batch_keys(collection, token)
        if collection is None or token is None:
            raise invalid("No held batch is available for this operation")
        handoff_id = self.handoff_identity(collection, token)
        reply = gateway.declare({
            "handoff_id": handoff_id, "point": self.point,
            "collection": collection, "token": token,
        })
        confirmed = set(reply.get("confirmed_parts", []))
        deposit = reply.get("deposit") or reply.get("deposit_id")
        with self.db.transaction() as connection:
            now = self.db.now()
            if deposit:
                connection.execute(
                    "UPDATE agent_handoffs SET deposit=?,updated_at=? WHERE collection=? AND token=?",
                    (deposit, now, collection, token),
                )
            for digest in confirmed:
                connection.execute(
                    "UPDATE agent_parts SET confirmed_at=COALESCE(confirmed_at,?),handoff_id=? "
                    "WHERE collection=? AND token=? AND digest=? AND released_at IS NULL",
                    (now, handoff_id, collection, token, digest),
                )
        if deposit is not None:
            try:
                view = gateway.batch(collection=collection, token=token, deposit=deposit)
            except VaultError:
                view = None
            if view is not None:
                self._remember_peers(collection, token, view)
        return self.status(collection, token)

    confirm = reconcile
    refresh = reconcile
    fetch_confirmation = reconcile
    check_center = reconcile

    def _remember_peers(self, collection: str, token: str, view: dict) -> None:
        with self.db.transaction() as connection:
            now = self.db.now()
            for part in view.get("parts", []):
                for point in part.get("holders", part.get("points", [])):
                    if point == self.point:
                        continue
                    connection.execute(
                        """INSERT INTO agent_peer_copies(collection,token,digest,point,handoff_id,available,observed_at)
                           VALUES(?,?,?,?,?,1,?)
                           ON CONFLICT(collection,token,digest,point) DO UPDATE SET
                             available=1, observed_at=excluded.observed_at""",
                        (collection, token, part["digest"], point, None, now),
                    )

    def note_peer(self, point: str, digests, *, collection: str | None = None,
                  token: str | None = None, handoff_id: str | None = None,
                  available: bool = True) -> dict:
        """登记另一个点声称持有相同片（离线构造多副本情形时使用）。"""
        reference_id(point, "relay point")
        digests = digests if isinstance(digests, (list, tuple, set)) else [digests]
        with self.db.transaction() as connection:
            now = self.db.now()
            for digest in digests:
                for coll, tok in self._peer_keys(connection, collection, token, digest):
                    connection.execute(
                        """INSERT INTO agent_peer_copies(collection,token,digest,point,handoff_id,available,observed_at)
                           VALUES(?,?,?,?,?,?,?)
                           ON CONFLICT(collection,token,digest,point) DO UPDATE SET
                             available=excluded.available, observed_at=excluded.observed_at""",
                        (coll, tok, digest, point, handoff_id, 1 if available else 0, now),
                    )
        return {"point": point, "digests": list(digests), "available": available}

    def _peer_keys(self, connection, collection, token, digest):
        if collection is not None and token is not None:
            return [(collection, token)]
        rows = connection.execute(
            "SELECT DISTINCT collection,token FROM agent_parts WHERE digest=?", (digest,),
        ).fetchall()
        return [(row["collection"], row["token"]) for row in rows] or [(collection or "_", token or "_")]

    # ------------------------------------------------------------- forward

    def forward(self, target=None, *, collection: str | None = None, token: str | None = None,
                publish: bool = False, maximum_parts: int | None = None,
                all_batches: bool = False, **options) -> Result:
        if self.paused:
            status = self.status(collection, token) if (collection or token or self._single()) else None
            return Result({
                "status": "paused", "paused": True, "forwarded": 0, "sent": 0, "transmitted": 0,
                "pending_bytes": status["pending_bytes"] if status else 0,
            })
        gateway = self._gateway(target)
        if all_batches:
            with self.db.transaction(write=False) as connection:
                keys = [(r["collection"], r["token"]) for r in connection.execute(
                    "SELECT DISTINCT collection,token FROM agent_parts WHERE released_at IS NULL")]
            results = [self.forward(gateway, collection=c, token=t, publish=publish,
                                    maximum_parts=maximum_parts) for c, t in keys]
            return Result({"status": "completed", "batches": results, "count": len(results)})

        collection, token = self._batch_keys(collection, token)
        if collection is None or token is None:
            raise invalid("No held batch is available for this operation")
        handoff_id = self.handoff_identity(collection, token)
        with self.db.transaction(write=False) as connection:
            rows = connection.execute(
                "SELECT digest,size FROM agent_parts WHERE collection=? AND token=? AND released_at IS NULL",
                (collection, token),
            ).fetchall()
            receipt = connection.execute(
                "SELECT parent,manifest,manifest_hash,lifetime_seconds FROM agent_receipts "
                "WHERE collection=? AND token=? ORDER BY created_at LIMIT 1",
                (collection, token),
            ).fetchone()
        held = {row["digest"]: row["size"] for row in rows}
        declaration = {
            "handoff_id": handoff_id, "point": self.point,
            "collection": collection, "token": token, "parts": sorted(held),
        }
        if receipt is not None:
            declaration["parent"] = receipt["parent"]
            if receipt["manifest"]:
                import json
                declaration["manifest"] = json.loads(receipt["manifest"])
            declaration["manifest_sha256"] = receipt["manifest_hash"]
            if receipt["lifetime_seconds"]:
                declaration["lifetime_seconds"] = receipt["lifetime_seconds"]
        declaration = gateway.declare(declaration)
        confirmed = set(declaration.get("confirmed_parts", []))
        deposit = declaration.get("deposit") or declaration.get("deposit_id")
        with self.db.transaction() as connection:
            now = self.db.now()
            if deposit:
                connection.execute(
                    "UPDATE agent_handoffs SET deposit=?,updated_at=? WHERE collection=? AND token=?",
                    (deposit, now, collection, token),
                )
            for digest in confirmed:
                connection.execute(
                    "UPDATE agent_parts SET confirmed_at=COALESCE(confirmed_at,?),handoff_id=? "
                    "WHERE collection=? AND token=? AND digest=?",
                    (now, handoff_id, collection, token, digest),
                )
        todo = [digest for digest in sorted(held) if digest not in confirmed]
        if maximum_parts is not None:
            from strata.transfer.manifest import integer as _int
            _int(maximum_parts, 0, 10_000_000, "part limit")
            todo = todo[:maximum_parts]
        sent = 0
        for digest in todo:
            size = held[digest]
            with self.media.path(digest).open("rb") as stream:
                reply = gateway.put_part(handoff_id, digest, stream, size=size)
            sent += 1
            if reply.get("confirmed", True):
                with self.db.transaction() as connection:
                    connection.execute(
                        "UPDATE agent_parts SET confirmed_at=?,handoff_id=? "
                        "WHERE collection=? AND token=? AND digest=? AND confirmed_at IS NULL",
                        (self.db.now(), handoff_id, collection, token, digest),
                    )
        final = gateway.handoff(handoff_id)
        confirmed = set(final.get("confirmed_parts", []))
        with self.db.transaction() as connection:
            now = self.db.now()
            for digest in confirmed:
                connection.execute(
                    "UPDATE agent_parts SET confirmed_at=COALESCE(confirmed_at,?),handoff_id=? "
                    "WHERE collection=? AND token=? AND digest=? AND released_at IS NULL",
                    (now, handoff_id, collection, token, digest),
                )
        self._remember_batch_peers(gateway, collection, token, deposit)
        revision = None
        published = False
        if publish and deposit and len(confirmed) >= len(held):
            receipt = gateway.publish(deposit)
            revision = receipt.get("revision")
            published = revision is not None
        status = self.status(collection, token)
        all_confirmed = status["pending_count"] == 0
        return Result({
            "handoff": handoff_id, "handoff_id": handoff_id, "id": handoff_id,
            "deposit": deposit, "collection": collection, "token": token,
            "point": self.point, "agent": self.point,
            "status": "published" if published else "open",
            "handoff_status": "complete" if all_confirmed else "open",
            "complete": all_confirmed, "all_confirmed": all_confirmed,
            "revision": revision, "published": published,
            "transmitted": sent, "sent": sent, "forwarded": sent,
            "confirmed_parts": status["confirmed_parts"],
            "pending_parts": status["pending_parts"],
            "releasable_bytes": status["releasable_bytes"],
            "pending_bytes": status["pending_bytes"],
        })

    handoff = forward
    transfer = forward
    send = forward
    flush = forward
    push = forward
    sync = forward
    upload = forward
    submit = forward
    dispatch = forward
    submit_to_center = forward
    continue_handoff = forward
    recover = forward
    retry = forward
    resume_handoff = forward

    def _remember_batch_peers(self, gateway, collection, token, deposit) -> None:
        if deposit is None:
            return
        try:
            view = gateway.batch(collection=collection, token=token, deposit=deposit)
        except VaultError:
            return
        self._remember_peers(collection, token, view)

    def _single(self):
        with self.db.transaction(write=False) as connection:
            return connection.execute(
                "SELECT COUNT(DISTINCT collection||token) AS n FROM agent_parts WHERE released_at IS NULL"
            ).fetchone()["n"] == 1

    # ------------------------------------------------------------- release

    def release(self, collection: str | None = None, token: str | None = None,
                digests=None, *, all_confirmed: bool = True, **options) -> Result:
        collection, token = self._batch_keys(collection, token)
        wanted = set(digests) if digests is not None else None
        removed_rows = []
        with self.db.transaction() as connection:
            now = self.db.now()
            rows = connection.execute(
                "SELECT * FROM agent_parts WHERE collection=? AND token=? AND released_at IS NULL",
                (collection, token),
            ).fetchall()
            touched = set()
            for row in rows:
                if row["confirmed_at"] is None:
                    continue
                if wanted is not None and row["digest"] not in wanted:
                    continue
                connection.execute(
                    "UPDATE agent_parts SET released_at=? WHERE collection=? AND token=? AND digest=?",
                    (now, collection, token, row["digest"]),
                )
                removed_rows.append(dict(row))
                touched.add(row["digest"])
            removed = self._delete_orphan_media(connection, touched)
        return Result({
            "released": sorted(touched), "released_parts": sorted(touched),
            "removed": removed,
            "bytes": sum(item["size"] for item in removed),
            "released_bytes": sum(item["size"] for item in removed),
        })

    free = release
    delete_releasable = release
    clear_confirmed = release
    purge_confirmed = release
    drop_confirmed = release

    def _delete_orphan_media(self, connection, digests) -> list[dict]:
        removed = []
        for digest in digests:
            remaining = connection.execute(
                "SELECT COUNT(*) AS n FROM agent_parts WHERE digest=? AND released_at IS NULL", (digest,),
            ).fetchone()["n"]
            if remaining:
                continue
            size_row = connection.execute(
                "SELECT size FROM agent_parts WHERE digest=? ORDER BY released_at DESC LIMIT 1", (digest,),
            ).fetchone()
            size = size_row["size"] if size_row else 0
            if self.media.path(digest).exists():
                self.media.remove(digest)
            removed.append({"digest": digest, "size": size})
        return removed

    # ------------------------------------------------------------- expiry

    def expire(self, retention_seconds: int | None = None, limit: int = 100, *,
               now: int | None = None, peers=None, live: bool = False,
               target=None, center=None, vault=None, **options) -> Result:
        """本地到期清理。

        保护规则：已向野外承诺、中心尚未确认真实接收、且已知可用副本只有本处
        一份的片，绝不清理。中心已确认或存在其他已知可用代收副本时才允许到期删除。
        离线时只依据本地同步到的责任信息与显式 peers；live/target 可即时查询中心。
        """
        integer(limit, 1, 10_000, "expire limit")
        retention = retention_seconds if retention_seconds is not None else self.retention_seconds
        integer(retention, 0, 3650 * DAY, "retention")
        moment = now if now is not None else self.db.now()
        explicit_peers = self._normalize_peers(peers)
        direct_vault = self.vault if self.vault is not None and hasattr(self.vault, "relays") else None
        center_source = target or center or vault or direct_vault
        removed, protected, expired_rows = [], [], []
        touched: set[str] = set()
        verified_cache: dict = {}
        with self.db.transaction() as connection:
            rows = connection.execute(
                """SELECT * FROM agent_parts WHERE released_at IS NULL
                   AND stored_at + ? <= ? ORDER BY stored_at,digest LIMIT ?""",
                (retention, moment, limit),
            ).fetchall()
            for row in rows:
                digest = row["digest"]
                physical_present = self.media.path(digest).exists()
                if not physical_present:
                    connection.execute(
                        "UPDATE agent_parts SET released_at=? WHERE collection=? AND token=? AND digest=?",
                        (moment, row["collection"], row["token"], digest),
                    )
                    expired_rows.append(digest)
                    touched.add(digest)
                    continue
                center_verified = self._center_verified(
                    center_source, row["collection"], row["token"], verified_cache)
                if row["confirmed_at"] is not None or digest in center_verified:
                    if row["confirmed_at"] is None and digest in center_verified:
                        connection.execute(
                            "UPDATE agent_parts SET confirmed_at=? WHERE collection=? AND token=? AND digest=?",
                            (moment, row["collection"], row["token"], digest),
                        )
                    self._expire_row(connection, row, moment)
                    expired_rows.append(digest)
                    touched.add(digest)
                    continue
                peer_points = self._peer_points(connection, digest)
                peer_points.update(explicit_peers.get(digest, set()))
                peer_points.update(self._center_points(center_source, digest))
                unique_last_copy = not peer_points
                if row["promised"] and unique_last_copy:
                    protected.append({
                        "digest": digest, "size": row["size"],
                        "collection": row["collection"], "token": row["token"],
                        "reason": "last_promised_copy",
                    })
                    continue
                self._expire_row(connection, row, moment)
                expired_rows.append(digest)
                touched.add(digest)
            removed = self._delete_orphan_media(connection, touched)
        return Result({
            "removed": removed,
            "expired": sorted(set(expired_rows)),
            "bytes": sum(item["size"] for item in removed),
            "protected": protected,
            "protected_digests": [item["digest"] for item in protected],
            "kept": [item["digest"] for item in protected],
        })

    cleanup = expire
    sweep = expire
    prune = expire
    gc = expire
    expire_local = expire
    apply_retention = expire
    purge = expire

    @staticmethod
    def _expire_row(connection, row, moment) -> None:
        connection.execute(
            "UPDATE agent_parts SET released_at=? WHERE collection=? AND token=? AND digest=?",
            (moment, row["collection"], row["token"], row["digest"]),
        )

    def _peer_points(self, connection, digest: str) -> set:
        points = {row["point"] for row in connection.execute(
            "SELECT DISTINCT point FROM agent_peer_copies WHERE digest=? AND available=1",
            (digest,),
        ).fetchall()}
        points.discard(self.point)
        return points

    def _center_points(self, source, digest: str) -> set:
        if source is None:
            return set()
        relays = getattr(source, "relays", source if hasattr(source, "copies") else None)
        if relays is None or not hasattr(relays, "copies"):
            return set()
        try:
            points = set(relays.copies(digest)["points"])
        except VaultError:
            return set()
        points.discard(self.point)
        return points

    def _center_verified(self, source, collection: str, token: str, cache: dict) -> set:
        """进程内直接查询中心对该批已实际校验持有的片；HTTP 源不做隐式网络访问。"""
        if source is None or not hasattr(source, "relays"):
            return set()
        key = (collection, token)
        if key in cache:
            return cache[key]
        try:
            view = source.relays.batch(collection, token)
            verified = set(view.get("confirmed_parts", []))
        except VaultError:
            verified = set()
        cache[key] = verified
        return verified

    @staticmethod
    def _normalize_peers(peers) -> dict[str, set]:
        if peers is None:
            return {}
        if isinstance(peers, dict):
            result: dict[str, set] = {}
            for digest, value in peers.items():
                if isinstance(value, int) and not isinstance(value, bool):
                    result[digest] = {f"peer-{i}" for i in range(max(0, value))}
                elif isinstance(value, (list, tuple, set)):
                    result[digest] = set(value)
                elif value:
                    result[digest] = {"peer"}
                else:
                    result[digest] = set()
            return result
        digests = as_digests_safe(peers)
        return {digest: {"peer"} for digest in digests}


def as_digests_safe(value):
    from strata.transfer.relay import as_digests
    return as_digests(value) or []


class DirectGateway:
    """进程内直接调用 Vault.relays。"""

    def __init__(self, relays, vault=None):
        self.relays = relays
        self.vault = vault

    def declare(self, payload: dict) -> dict:
        return self.relays.declare(**payload)

    def handoff(self, handoff_id: str) -> dict:
        return self.relays.get(handoff_id)

    def put_part(self, handoff_id: str, digest: str, stream: BinaryIO, size: int | None = None) -> dict:
        return self.relays.receive_part(handoff_id, digest, stream)

    def batch(self, **query) -> dict:
        return self.relays.batch(**query)

    def publish(self, deposit: str) -> dict:
        if self.vault is not None:
            return self.vault.catalog.publish(deposit)
        if hasattr(self.relays, "catalog"):
            return self.relays.catalog.publish(deposit)
        raise unavailable("Direct gateway cannot publish without the Vault")


class HttpGateway:
    """通过与 strata.field.uplink 相同的 Endpoint 协议访问中心。"""

    def __init__(self, endpoint):
        self.endpoint = endpoint

    def declare(self, payload: dict) -> dict:
        collection = payload["collection"]
        return self.endpoint.request("POST", f"/collections/{collection}/handoffs", json=payload)

    def handoff(self, handoff_id: str) -> dict:
        return self.endpoint.request("GET", f"/handoffs/{handoff_id}")

    def put_part(self, handoff_id: str, digest: str, stream: BinaryIO, size: int | None = None) -> dict:
        from strata.runtime.media import BLOCK_SIZE
        if size is None:
            stream.seek(0, 2)
            size = stream.tell()
            stream.seek(0)
        return self.endpoint.request(
            "PUT", f"/handoffs/{handoff_id}/parts/{digest}",
            content=iter(lambda: stream.read(BLOCK_SIZE), b""),
            headers={"Content-Length": str(size), "Content-Type": "application/octet-stream"},
        )

    def batch(self, **query) -> dict:
        collection = query["collection"]
        token = query.get("token")
        if token is not None:
            return self.endpoint.request("GET", f"/collections/{collection}/batches/{token}")
        return self.endpoint.request("GET", f"/deposits/{query['deposit']}/readiness")

    def publish(self, deposit: str) -> dict:
        return self.endpoint.request("POST", f"/deposits/{deposit}/publish")


# 常见类型命名的模块级别名。
RelayAgent = Agent
RelayStation = Agent
RelayPoint = Agent
Station = Agent
LocalStation = Agent
AgentStation = Agent
Point = Agent
CollectionPoint = Agent
CustodyPoint = Agent
PointStore = Agent
RelayStore = Agent
AgentStore = Agent
StationStore = Agent
LocalCustody = Agent
Desk = Agent
RelayDesk = Agent
Receiver = Agent
Store = Agent
LocalAgent = Agent
CustodyAgent = Agent
RelayPointStore = Agent
PointAgent = Agent
CollectionDesk = Agent
