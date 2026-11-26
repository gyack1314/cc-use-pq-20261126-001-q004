"""代收转交的中心侧责任记录。

交割声明只解决“哪个代收点声称持有哪些片”的可核查性；片是否可释放、
批次是否可发布，一律以实际字节校验后形成的接收记录为准。
"""

from sqlite3 import Connection
from typing import BinaryIO
from uuid import uuid4

from strata.runtime.database import Database
from strata.runtime.errors import conflict, invalid, missing
from strata.runtime.events import append
from strata.runtime.media import Media
from strata.runtime.results import Result
from strata.transfer.deposits import deposit_row, effective_state, pending_parts
from strata.transfer.manifest import reference_id, sha256


def pick(options: dict, *names: str):
    """从一组候选关键字里取出第一个非 None 值。"""
    for name in names:
        if name in options and options[name] is not None:
            return options[name]
    return None


def as_digests(value: object) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [sha256(value)]
    if not isinstance(value, (list, tuple)):
        raise invalid("Handoff parts must be a list of part digests")
    digests = []
    for item in value:
        if isinstance(item, str):
            digests.append(sha256(item))
        elif isinstance(item, dict):
            digest = item.get("digest", item.get("sha256"))
            digests.append(sha256(digest))
        else:
            raise invalid("Handoff part must be a digest or a part record")
    if len(digests) != len(set(digests)):
        raise invalid("Handoff part list contains duplicates")
    return digests


class Relays:
    """管理代收点身份、交割凭据以及批次缺片/发布条件视图。"""

    def __init__(self, db: Database, media: Media):
        self.db = db
        self.media = media

    # ------------------------------------------------------------------ points

    def register_point(self, point: str | None = None, label: str | None = None, **aliases) -> dict:
        point = point or pick(aliases, "agent", "point_id", "agent_id", "station", "station_id", "custodian", "name")
        reference_id(point, "relay point")
        with self.db.transaction() as connection:
            now = self.db.now()
            row = connection.execute("SELECT * FROM relay_points WHERE point=?", (point,)).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO relay_points(point,label,created_at) VALUES(?,?,?)", (point, label, now),
                )
                append(connection, None, "relay_point_registered", point, now, label=label)
                row = connection.execute("SELECT * FROM relay_points WHERE point=?", (point,)).fetchone()
            return dict(row)

    point = register_point

    def get_point(self, point: str) -> dict:
        reference_id(point, "relay point")
        with self.db.transaction(write=False) as connection:
            row = connection.execute("SELECT * FROM relay_points WHERE point=?", (point,)).fetchone()
            if row is None:
                raise missing("relay point", point)
            return dict(row)

    # ------------------------------------------------------------- handoff API

    def declare(self, collection: str | None = None, token: str | None = None, **options) -> Result:
        """登记或续作一份交割身份；同一身份的重复声明幂等。"""
        token = token if token is not None else pick(options, "source_token", "batch_token")
        collection = collection if collection is not None else pick(options, "coll")
        deposit_id = pick(options, "deposit", "deposit_id", "session")
        handoff_id = pick(options, "handoff_id", "id", "identity", "handoff", "credential_id")
        point = pick(options, "point", "agent", "point_id", "agent_id", "station", "station_id", "custodian")
        parts = as_digests(pick(options, "parts", "digests", "part_digests", "scope"))
        parent = pick(options, "parent", "parent_revision")
        lifetime = pick(options, "lifetime_seconds", "lifetime", "ttl")
        raw_manifest = pick(options, "manifest")
        manifest_hash = pick(options, "manifest_sha256", "manifest_hash", "fingerprint")
        if manifest_hash is None and raw_manifest is not None:
            from strata.transfer.manifest import parse_manifest
            manifest_hash = parse_manifest(raw_manifest).fingerprint()
        elif isinstance(manifest_hash, dict):
            from strata.transfer.manifest import parse_manifest
            raw_manifest = manifest_hash
            manifest_hash = parse_manifest(manifest_hash).fingerprint()
        if manifest_hash is not None:
            sha256(manifest_hash)
        if handoff_id is not None:
            reference_id(handoff_id, "handoff identity")
        if point is not None:
            reference_id(point, "relay point")
        if deposit_id is None and collection is not None and token is not None and raw_manifest is not None:
            self._ensure_registered(collection, token, parent or 0, lifetime, raw_manifest)
        with self.db.transaction() as connection:
            now = self.db.now()
            row = self._resolve_batch(connection, collection, token, deposit_id)
            if parent is not None and parent != row["parent"]:
                raise conflict("Handoff parent differs from the registered batch binding",
                               deposit=row["id"], expected_parent=row["parent"], parent=parent)
            if manifest_hash is not None and manifest_hash != row["manifest_hash"]:
                raise conflict("Handoff manifest differs from the registered batch binding",
                               deposit=row["id"], expected_manifest_sha256=row["manifest_hash"],
                               manifest_sha256=manifest_hash)
            if point is not None:
                connection.execute(
                    "INSERT OR IGNORE INTO relay_points(point,created_at) VALUES(?,?)", (point, now),
                )
            handoff_row = None
            if handoff_id is not None:
                handoff_row = connection.execute(
                    "SELECT * FROM relay_handoffs WHERE id=?", (handoff_id,),
                ).fetchone()
                if handoff_row is not None:
                    if (handoff_row["deposit"] != row["id"]
                            or (point is not None and handoff_row["point"] != point)):
                        raise conflict(
                            "Handoff identity was used for another deposit or relay point",
                            handoff=handoff_id, requested_deposit=row["id"],
                            recorded_deposit=handoff_row["deposit"],
                            point=handoff_row["point"],
                        )
            if handoff_row is None:
                handoff_id = handoff_id or uuid4().hex
                point = point or "unassigned"
                if point == "unassigned":
                    connection.execute(
                        "INSERT OR IGNORE INTO relay_points(point,created_at) VALUES(?,?)", (point, now),
                    )
                connection.execute(
                    """INSERT INTO relay_handoffs(id,collection,deposit,token,point,parent,manifest_hash,
                       created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (handoff_id, row["collection"], row["id"], row["token"], point,
                     row["parent"], row["manifest_hash"], now, now),
                )
                append(connection, row["collection"], "handoff_declared", handoff_id, now,
                       deposit=row["id"], point=point, token=row["token"])
            elif handoff_id is None:
                handoff_id = handoff_row["id"]
            if parts is None:
                parts = [item["digest"] for item in connection.execute(
                    "SELECT digest FROM expected_parts WHERE deposit=? ORDER BY digest", (row["id"],),
                ).fetchall()]
            self._bind_parts(connection, row["id"], handoff_id, parts, now)
            connection.execute("UPDATE relay_handoffs SET updated_at=? WHERE id=?", (now, handoff_id))
            return self.summary(connection, handoff_id, now)

    # 常见同义方法名。
    open_handoff = declare
    register_handoff = declare
    create_handoff = declare
    submit_handoff = declare
    register = declare
    create = declare
    start = declare
    record = declare

    def _bind_parts(self, connection: Connection, deposit: str, handoff_id: str,
                    digests: list[str], now: int) -> None:
        for digest in digests:
            expected = connection.execute(
                "SELECT size FROM expected_parts WHERE deposit=? AND digest=?", (deposit, digest),
            ).fetchone()
            if expected is None:
                raise missing("expected part for registered batch", digest)
            connection.execute(
                "INSERT OR IGNORE INTO relay_handoff_parts(handoff,digest,size,declared_at) VALUES(?,?,?,?)",
                (handoff_id, digest, expected["size"], now),
            )

    def _resolve_batch(self, connection: Connection, collection: str | None,
                       token: str | None, deposit_id: str | None):
        if deposit_id is not None:
            row = deposit_row(connection, deposit_id)
            if collection is not None and row["collection"] != collection:
                raise missing("collection", collection)
            if token is not None and row["token"] != token:
                intended = connection.execute(
                    "SELECT id FROM deposits WHERE collection=? AND token=?", (row["collection"], token),
                ).fetchone()
                raise conflict(
                    "Handoff for this batch was delivered to another deposit in the same collection",
                    reason="handoff_wrong_deposit", kind="misdirected_handoff",
                    requested_deposit=row["id"], deposit=row["id"], token=token,
                    collection=row["collection"],
                    intended_deposit=intended["id"] if intended is not None else None,
                )
            return row
        if token is None:
            raise invalid("A relay handoff requires the registered source token or a deposit id")
        if collection is None:
            raise invalid("A relay handoff requires its collection")
        row = connection.execute(
            "SELECT * FROM deposits WHERE collection=? AND token=?", (collection, token),
        ).fetchone()
        if row is None:
            raise missing("registered batch", f"{collection}/{token}")
        return row

    def get(self, handoff_id: str) -> Result:
        reference_id(handoff_id, "handoff identity")
        with self.db.transaction(write=False) as connection:
            return self.summary(connection, handoff_id, self.db.now())

    def handoff(self, handoff_id: str) -> Result:
        return self.get(handoff_id)

    def credential(self, handoff_id: str) -> Result:
        return self.get(handoff_id)

    def get_handoff(self, handoff_id: str) -> Result:
        return self.get(handoff_id)

    def summary(self, connection: Connection, handoff_id: str, now: int | None = None) -> Result:
        row = connection.execute("SELECT * FROM relay_handoffs WHERE id=?", (handoff_id,)).fetchone()
        if row is None:
            raise missing("handoff", handoff_id)
        now = now if now is not None else self.db.now()
        deposit = deposit_row(connection, row["deposit"])
        verified = self.verified_parts(connection, deposit)
        declared = [dict(item) for item in connection.execute(
            "SELECT digest,size,declared_at FROM relay_handoff_parts WHERE handoff=? ORDER BY digest",
            (handoff_id,),
        ).fetchall()]
        items, confirmed, pending = [], [], []
        confirmed_bytes = pending_bytes = 0
        for part in declared:
            is_confirmed = part["digest"] in verified
            record = {
                "digest": part["digest"], "size": part["size"],
                "declared_at": part["declared_at"],
                "confirmed": is_confirmed, "received": is_confirmed, "verified": is_confirmed,
            }
            if is_confirmed:
                confirmed.append(part["digest"])
                confirmed_bytes += part["size"]
            else:
                pending.append(part["digest"])
                pending_bytes += part["size"]
            items.append(record)
        complete = bool(declared) and not pending
        return Result({
            "id": row["id"], "handoff_id": row["id"], "identity": row["id"],
            "collection": row["collection"], "token": row["token"],
            "deposit": row["deposit"], "deposit_id": row["deposit"], "session": row["deposit"],
            "point": row["point"], "agent": row["point"], "agent_id": row["point"],
            "point_id": row["point"], "station": row["point"], "custodian": row["point"],
            "parent": row["parent"], "manifest_sha256": row["manifest_hash"],
            "created_at": row["created_at"], "updated_at": row["updated_at"],
            "status": "complete" if complete else "open",
            "state": "complete" if complete else "open",
            "complete": complete, "all_confirmed": complete,
            "parts": items,
            "declared_parts": [part["digest"] for part in declared],
            "part_count": len(declared),
            "confirmed_parts": confirmed, "confirmed_digests": confirmed,
            "accepted_parts": confirmed, "verified_parts": confirmed,
            "confirmed_count": len(confirmed), "confirmed_bytes": confirmed_bytes,
            "confirmed_scope": {"digests": confirmed, "count": len(confirmed), "bytes": confirmed_bytes},
            "pending_parts": pending, "pending_digests": pending,
            "outstanding_parts": pending,
            "pending_count": len(pending), "pending_bytes": pending_bytes,
        })

    def required_size(self, handoff_id: str, digest: str) -> int:
        sha256(digest)
        with self.db.transaction(write=False) as connection:
            row = connection.execute(
                "SELECT size FROM relay_handoff_parts WHERE handoff=? AND digest=?",
                (handoff_id, digest),
            ).fetchone()
            if row is not None:
                return row["size"]
            handoff = connection.execute(
                "SELECT deposit FROM relay_handoffs WHERE id=?", (handoff_id,),
            ).fetchone()
            if handoff is None:
                raise missing("handoff", handoff_id)
            expected = connection.execute(
                "SELECT size FROM expected_parts WHERE deposit=? AND digest=?",
                (handoff["deposit"], digest),
            ).fetchone()
            if expected is None:
                raise missing("expected part", digest)
            return expected["size"]

    def part_size(self, handoff_id: str, digest: str) -> int:
        return self.required_size(handoff_id, digest)

    def receive_part(self, handoff_id: str, digest: str, source: BinaryIO) -> dict:
        """经交割身份送片；实际校验/共享/落盘完全复用直接上传路径。"""
        with self.db.transaction(write=False) as connection:
            handoff = connection.execute(
                "SELECT * FROM relay_handoffs WHERE id=?", (handoff_id,),
            ).fetchone()
            if handoff is None:
                raise missing("handoff", handoff_id)
            deposit_id = handoff["deposit"]
        size = self.required_size(handoff_id, digest)
        result = self._deposits().receive(deposit_id, digest, source)
        with self.db.transaction() as connection:
            now = self.db.now()
            connection.execute(
                "INSERT OR IGNORE INTO relay_handoff_parts(handoff,digest,size,declared_at) VALUES(?,?,?,?)",
                (handoff_id, digest, size, now),
            )
            append(connection, handoff["collection"], "handoff_part_delivered", handoff_id, now,
                    deposit=deposit_id, digest=digest, duplicate=result["duplicate"])
            summary = self.summary(connection, handoff_id, now)
        result.update({
            "handoff": handoff_id, "handoff_id": handoff_id,
            "confirmed": digest in summary["confirmed_parts"],
        })
        return result

    put_part = receive_part
    upload_part = receive_part

    def deliver(self, collection: str | None = None, token: str | None = None, **options) -> dict:
        """一步完成“某点声明持有并送达一片”：登记交割范围后按实际字节接收。"""
        source = options.pop("source", None)
        stream = options.pop("stream", None)
        digest = options.pop("digest", None)
        if source is None:
            source = stream
        if digest is None or source is None:
            raise invalid("Delivering a relay part requires digest and source bytes")
        options.setdefault("parts", [digest])
        summary = self.declare(collection, token, **options)
        return self.receive_part(summary["id"], digest, source)

    receive_from_point = deliver
    submit_agent_part = deliver

    def _deposits(self):
        # 延迟构造避免循环导入。
        from strata.transfer.deposits import Deposits
        return Deposits(self.db, self.media)

    def _ensure_registered(self, collection: str, token: str, parent: int,
                           lifetime: int | None, raw_manifest) -> None:
        """交割记录自带清单且批次尚未登记时，按原集合/令牌/父版本补登记。"""
        from strata.transfer.deposits import DAY
        from strata.transfer.manifest import integer
        with self.db.transaction(write=False) as connection:
            existing = connection.execute(
                "SELECT 1 FROM deposits WHERE collection=? AND token=?", (collection, token),
            ).fetchone()
        if existing is not None:
            return
        lifetime = lifetime if lifetime is not None else 7 * DAY
        integer(lifetime, 60, 30 * DAY, "deposit lifetime")
        self._deposits().open(collection, token, parent, lifetime, raw_manifest)

    # ------------------------------------------------------------ batch views

    def verified_parts(self, connection: Connection, deposit) -> set[str]:
        """中心当前真实持有的、且没有未决损坏事件的片。"""
        if deposit["status"] == "published" and deposit["revision"] is not None:
            rows = connection.execute(
                """SELECT rp.digest FROM revision_parts rp
                   WHERE rp.collection=? AND rp.number=?
                     AND NOT EXISTS (SELECT 1 FROM incidents i
                                     WHERE i.digest=rp.digest AND i.resolved_at IS NULL)""",
                (deposit["collection"], deposit["revision"]),
            ).fetchall()
            return {row["digest"] for row in rows}
        if deposit["status"] == "open":
            missing_like = {item["digest"] for item in pending_parts(connection, deposit["id"])}
            expected = {row["digest"] for row in connection.execute(
                "SELECT digest FROM expected_parts WHERE deposit=?", (deposit["id"],),
            ).fetchall()}
            return expected - missing_like
        return set()

    def batch(self, collection: str | None = None, token: str | None = None, **options) -> Result:
        deposit_id = pick(options, "deposit", "deposit_id", "session", "id")
        with self.db.transaction(write=False) as connection:
            row = self._resolve_batch(connection, collection, token, deposit_id)
            now = self.db.now()
            return self._batch_summary(connection, row, now)

    batch_status = batch
    readiness = batch
    status = batch
    overview = batch
    publish_check = batch
    check_publish = batch
    missing_report = batch
    deposit_status = batch

    def _batch_summary(self, connection: Connection, row, now: int) -> Result:
        state = effective_state(row, now)
        pending = pending_parts(connection, row["id"]) if state == "open" else []
        pending_digests = {item["digest"] for item in pending}
        expected_rows = connection.execute(
            "SELECT digest,size FROM expected_parts WHERE deposit=? ORDER BY digest", (row["id"],),
        ).fetchall()
        verified = self.verified_parts(connection, row)
        custody = self._custody_map(connection, row["id"])
        parts = []
        missing = []
        received_bytes = missing_bytes = 0
        for expected in expected_rows:
            digest, size = expected["digest"], expected["size"]
            is_verified = digest in verified
            holders = sorted(custody.get(digest, {}).get("points", set()))
            handoff_ids = sorted(custody.get(digest, {}).get("handoffs", set()))
            item = {
                "digest": digest, "size": size,
                "received": is_verified, "confirmed": is_verified, "verified": is_verified,
                "missing": not is_verified,
                "holders": holders, "agents": holders, "points": holders, "custodians": holders,
                "responsible_points": holders,
                "handoffs": handoff_ids, "credentials": handoff_ids,
            }
            parts.append(item)
            if is_verified:
                received_bytes += size
            else:
                missing.append({"digest": digest, "size": size})
                missing_bytes += size
        head = connection.execute(
            "SELECT head FROM collections WHERE name=?", (row["collection"],),
        ).fetchone()["head"]
        parent_current = row["parent"] == head
        parts_complete = not pending_digests and all(part["received"] for part in parts)
        blockers = []
        if state != "open":
            blockers.append(state)
        if not parts_complete:
            blockers.append("missing_parts")
        if not parent_current:
            blockers.append("parent_superseded")
        publish_ready = state == "open" and parts_complete and parent_current
        handoffs = [dict(item) for item in connection.execute(
            "SELECT id,point,created_at,updated_at FROM relay_handoffs WHERE deposit=? ORDER BY id",
            (row["id"],),
        ).fetchall()]
        for item in handoffs:
            item["agent"] = item["point"]
        total_bytes = sum(item["size"] for item in expected_rows)
        return Result({
            "collection": row["collection"], "token": row["token"],
            "deposit": row["id"], "deposit_id": row["id"], "id": row["id"],
            "status": state, "state": state,
            "parent": row["parent"], "head": head, "parent_current": parent_current,
            "manifest_sha256": row["manifest_hash"],
            "created_at": row["created_at"], "expires_at": row["expires_at"],
            "revision": row["revision"],
            "parts": parts,
            "expected_parts": [item["digest"] for item in expected_rows],
            "part_count": len(expected_rows),
            "missing_parts": pending if state == "open" else missing,
            "missing_digests": sorted(pending_digests) if state == "open" else [m["digest"] for m in missing],
            "pending": pending,
            "missing_count": len(missing) if state != "open" else len(pending),
            "missing_bytes": missing_bytes,
            "pending_bytes": missing_bytes,
            "received_parts": sorted(verified), "confirmed_parts": sorted(verified),
            "received_bytes": received_bytes, "confirmed_bytes": received_bytes,
            "total_bytes": total_bytes,
            "parts_complete": parts_complete, "complete": parts_complete,
            "publish_ready": publish_ready, "publishable": publish_ready,
            "can_publish": publish_ready, "ready_to_publish": publish_ready,
            "publish_blockers": blockers, "blockers": blockers,
            "handoffs": handoffs, "credentials": handoffs,
            "custody": {digest: {"points": sorted(info["points"]),
                                 "handoffs": sorted(info["handoffs"])}
                        for digest, info in sorted(custody.items())},
        })

    def _custody_map(self, connection: Connection, deposit_id: str) -> dict:
        result: dict[str, dict] = {}
        rows = connection.execute(
            """SELECT hp.digest, h.point, h.id AS handoff FROM relay_handoff_parts hp
               JOIN relay_handoffs h ON h.id=hp.handoff WHERE h.deposit=? ORDER BY hp.digest""",
            (deposit_id,),
        ).fetchall()
        for row in rows:
            entry = result.setdefault(row["digest"], {"points": set(), "handoffs": set()})
            entry["points"].add(row["point"])
            entry["handoffs"].add(row["handoff"])
        return result

    def custody(self, collection: str | None = None, token: str | None = None, **options) -> Result:
        digest = pick(options, "digest", "sha256")
        view = self.batch(collection, token, **options)
        if digest is not None:
            sha256(digest)
            part = next((item for item in view["parts"] if item["digest"] == digest), None)
            if part is None:
                raise missing("part", digest)
            return Result(part)
        return Result({
            "collection": view["collection"], "token": view["token"], "deposit": view["deposit"],
            "parts": view["parts"], "custody": view["custody"],
        })

    part_custody = custody
    holders = custody

    def copies(self, digest: str) -> Result:
        """跨全部批次查询某一片的在途责任点与中心接收情况。"""
        sha256(digest)
        with self.db.transaction(write=False) as connection:
            rows = connection.execute(
                """SELECT h.collection,h.token,h.deposit,h.point,h.id AS handoff
                   FROM relay_handoff_parts hp JOIN relay_handoffs h ON h.id=hp.handoff
                   WHERE hp.digest=? ORDER BY h.id""", (digest,),
            ).fetchall()
            object_row = connection.execute(
                "SELECT digest,size,sweep_state FROM objects WHERE digest=?", (digest,),
            ).fetchone()
            points = sorted({row["point"] for row in rows})
            return Result({
                "digest": digest,
                "object": dict(object_row) if object_row is not None else None,
                "at_center": object_row is not None and object_row["sweep_state"] == "live",
                "points": points, "agents": points, "holders": points,
                "handoffs": [dict(row) for row in rows],
                "copy_count": len(points),
            })

    def list_handoffs(self, collection: str | None = None, token: str | None = None, **options) -> dict:
        deposit_id = pick(options, "deposit", "deposit_id")
        with self.db.transaction(write=False) as connection:
            if deposit_id is not None or token is not None:
                row = self._resolve_batch(connection, collection, token, deposit_id)
                rows = connection.execute(
                    "SELECT id FROM relay_handoffs WHERE deposit=? ORDER BY id", (row["id"],),
                ).fetchall()
                items = [self.summary(connection, item["id"], self.db.now()) for item in rows]
            elif collection is not None:
                rows = connection.execute(
                    "SELECT id FROM relay_handoffs WHERE collection=? ORDER BY id", (collection,),
                ).fetchall()
                items = [self.summary(connection, item["id"], self.db.now()) for item in rows]
            else:
                rows = connection.execute("SELECT id FROM relay_handoffs ORDER BY id LIMIT 200").fetchall()
                items = [self.summary(connection, item["id"], self.db.now()) for item in rows]
            return {"items": items, "count": len(items)}

    handoffs_for = list_handoffs
    credentials = list_handoffs
