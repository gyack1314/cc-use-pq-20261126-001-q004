"""代收转交能力的行为核验：多点分片、交割身份幂等、最后副本保护与现场查询。"""

from io import BytesIO

from strata.field.agent import Agent
from strata.field.parcel import build_parcel, open_parcel
from strata.runtime.errors import VaultError
from support import Scenario, manifest, sha


ENTRIES = {"survey/a": [b"aaaa", b"bbbb"], "survey/b": [b"cccc"]}
D_A = sha(b"aaaa")
D_B = sha(b"bbbb")
D_C = sha(b"cccc")
BLOBS = {D_A: b"aaaa", D_B: b"bbbb", D_C: b"cccc"}


class FlakyGateway:
    """包装直连网关，在片已落中心后制造一次“确认丢失”。"""

    def __init__(self, vault, fail_puts=1):
        self.inner = vault
        self.fail_puts = fail_puts
        self.calls = 0

    def declare(self, payload):
        return self.inner.relays.declare(**payload)

    def handoff(self, hid):
        return self.inner.relays.get(hid)

    def put_part(self, hid, digest, stream, size=None):
        result = self.inner.relays.receive_part(hid, digest, stream)
        self.calls += 1
        if self.fail_puts > 0:
            self.fail_puts -= 1
            from strata.runtime.errors import unavailable
            raise unavailable("confirmation lost after commit")
        return result

    def batch(self, **query):
        return self.inner.relays.batch(**query)

    def publish(self, deposit):
        return self.inner.catalog.publish(deposit)


class CenterRelayRules(Scenario):
    def test_multiple_points_bring_different_and_overlapping_parts(self):
        opened = self.open("batch-x", ENTRIES)
        hid_n = self.vault.relays.declare(
            "ridge", "batch-x", point="north", handoff_id="h-north", parts=[D_A, D_B])
        hid_s = self.vault.relays.declare(
            "ridge", "batch-x", point="south", handoff_id="h-south", parts=[D_B, D_C])
        self.assertEqual(hid_n.pending_bytes, 8)
        self.assertEqual(hid_s.pending_bytes, 8)

        view = self.vault.relays.batch("ridge", "batch-x")
        holders = {item["digest"]: item["holders"] for item in view["parts"]}
        self.assertEqual(holders[D_A], ["north"])
        self.assertEqual(holders[D_B], ["north", "south"])
        self.assertEqual(holders[D_C], ["south"])
        self.assertFalse(view["publish_ready"])

        self.vault.relays.receive_part("h-north", D_A, BytesIO(b"aaaa"))
        self.vault.relays.receive_part("h-north", D_B, BytesIO(b"bbbb"))
        # 南点送重叠片时中心已有字节，重复收据不影响其交割范围确认。
        dup = self.vault.relays.receive_part("h-south", D_B, BytesIO(b"bbbb"))
        self.assertTrue(dup["duplicate"])
        self.vault.relays.receive_part("h-south", D_C, BytesIO(b"cccc"))

        north = self.vault.relays.get("h-north")
        south = self.vault.relays.get("h-south")
        self.assertEqual(north["confirmed_parts"], [D_A, D_B])
        self.assertEqual(sorted(south["confirmed_parts"]), [D_B, D_C])
        self.assertTrue(north["complete"] and south["complete"])
        ready = self.vault.relays.batch("ridge", "batch-x")
        self.assertTrue(ready["publish_ready"])
        published = self.vault.catalog.publish(opened["id"])
        self.assertEqual(published["revision"], 1)
        # 发布后交割范围仍可核查为已确认。
        self.assertTrue(self.vault.relays.get("h-north")["complete"])

    def test_two_credentials_for_same_parts_are_independently_queryable(self):
        self.open("dup-parts", {"f": [b"same-bytes"]})
        d = sha(b"same-bytes")
        a = self.vault.relays.declare("ridge", "dup-parts", point="a", handoff_id="cred-a", parts=[d])
        b = self.vault.relays.declare("ridge", "dup-parts", point="b", handoff_id="cred-b", parts=[d])
        self.assertNotEqual(a["id"], b["id"])
        self.assertEqual(self.vault.relays.get("cred-a")["point"], "a")
        self.assertEqual(self.vault.relays.get("cred-b")["point"], "b")
        copies = self.vault.relays.copies(d)
        self.assertEqual(copies["points"], ["a", "b"])
        self.assertEqual(len(copies["handoffs"]), 2)

    def test_duplicate_declaration_is_idempotent_and_binding_conflicts(self):
        self.open("idem", ENTRIES)
        first = self.vault.relays.declare("ridge", "idem", point="n", handoff_id="hid-1")
        again = self.vault.relays.declare("ridge", "idem", point="n", handoff_id="hid-1")
        self.assertEqual(first["id"], again["id"])
        with self.assertRaisesRegex(VaultError, "another deposit or relay point"):
            self.vault.relays.declare("ridge", "idem", point="other", handoff_id="hid-1")
        with self.assertRaisesRegex(VaultError, "manifest differs"):
            self.vault.relays.declare(
                "ridge", "idem", point="n", handoff_id="hid-2",
                manifest_sha256=sha(b"other-manifest"))

    def test_misdirected_handoff_is_rejected_by_the_other_deposit(self):
        one = self.open("right", {"a": [b"1"]})
        two = self.open("wrong", {"b": [b"22"]})
        with self.assertRaisesRegex(VaultError, "another deposit") as caught:
            self.vault.relays.declare(deposit=two["id"], token="right", point="n")
        self.assertEqual(caught.exception.details["intended_deposit"], one["id"])
        # 误送不得在错误提交上留下任何交割记录。
        self.assertEqual(self.vault.relays.list_handoffs(deposit=two["id"])["items"], [])
        ok = self.vault.relays.declare("ridge", "right", point="n", handoff_id="right-h")
        self.assertEqual(ok["deposit"], one["id"])

    def test_late_handoff_after_direct_upload_reports_full_scope(self):
        opened = self.open("late", ENTRIES)
        for blob in (b"aaaa", b"bbbb", b"cccc"):
            self.vault.deposits.receive(opened["id"], sha(blob), BytesIO(blob))
        late = self.vault.relays.declare("ridge", "late", point="late-point", handoff_id="late-h")
        self.assertTrue(late["complete"])
        self.assertEqual(set(late["confirmed_parts"]), {D_A, D_B, D_C})
        view = self.vault.relays.batch("ridge", "late")
        self.assertTrue(view["publish_ready"])

    def test_center_state_survives_restart(self):
        self.open("persist", ENTRIES)
        self.vault.relays.declare("ridge", "persist", point="n", handoff_id="persist-h", parts=[D_A])
        self.restart()
        row = self.vault.relays.get("persist-h")
        self.assertEqual(row["point"], "n")
        self.assertEqual(row["declared_parts"], [D_A])


class FieldAgentRules(Scenario):
    def _agent(self, name="north", **kw):
        return Agent(self.root / name, name, vault=self.vault, clock=self.clock, **kw)

    def _hold(self, agent, token="batch", blobs=None):
        raw = manifest(ENTRIES)
        return agent.hold_parts("ridge", token, raw, blobs or BLOBS)

    def test_durable_receipt_survives_restart_and_pause(self):
        agent = self._agent("durable")
        receipt = self._hold(agent, "durable-batch", {D_A: b"aaaa"})
        self.assertEqual(receipt["valid"], True)
        agent.pause()
        reopened = Agent(self.root / "durable", clock=self.clock)
        self.assertTrue(reopened.paused)
        again = reopened.receipt(receipt["id"])
        self.assertTrue(again["valid"] and again["durable"])
        # 暂停期间转交不产生任何中心交割记录。
        result = reopened.forward(self.vault, collection="ridge", token="durable-batch")
        self.assertEqual(result["status"], "paused")
        self.assertEqual(self.vault.relays.list_handoffs(collection="ridge")["count"], 0)
        reopened.resume()
        self.assertFalse(Agent(self.root / "durable", clock=self.clock).paused)

    def test_pending_and_releasable_byte_counts(self):
        agent = self._agent("counts")
        self._hold(agent, "counts-batch", {D_A: b"aaaa", D_B: b"bbbb"})
        status = agent.status("ridge", "counts-batch")
        self.assertEqual(status["pending_bytes"], 8)
        self.assertEqual(status["releasable_bytes"], 0)
        agent.forward(self.vault, collection="ridge", token="counts-batch")
        status = agent.status("ridge", "counts-batch")
        self.assertEqual(status["pending_bytes"], 0)
        self.assertEqual(status["releasable_bytes"], 8)
        released = agent.release("ridge", "counts-batch")
        self.assertEqual(released["bytes"], 8)
        self.assertEqual(agent.status("ridge", "counts-batch")["releasable_bytes"], 0)
        self.assertFalse(agent.media.path(D_A).exists())

    def test_lost_confirmation_is_recovered_with_original_identity(self):
        agent = self._agent("lost")
        self._hold(agent, "lost-batch", {D_A: b"aaaa", D_B: b"bbbb"})
        handoff_id = agent.handoff_identity("ridge", "lost-batch")
        flaky = FlakyGateway(self.vault, fail_puts=1)
        with self.assertRaisesRegex(VaultError, "lost after commit"):
            agent.forward(flaky, collection="ridge", token="lost-batch")
        # 模拟代收点重启：本地仍保存原交割身份与原点身份。
        agent2 = Agent(self.root / "lost", vault=self.vault, clock=self.clock)
        self.assertEqual(agent2.handoff_identity("ridge", "lost-batch"), handoff_id)
        result = agent2.forward(self.vault, collection="ridge", token="lost-batch")
        self.assertEqual(result["handoff_id"], handoff_id)
        self.assertEqual(result["transmitted"], 1)  # 只剩未确认的那一片
        status = agent2.status("ridge", "lost-batch")
        self.assertEqual(status["releasable_bytes"], 8)
        center = self.vault.relays.get(handoff_id)
        self.assertTrue(center["complete"])

    def test_last_promised_copy_is_protected_from_expiry(self):
        agent = self._agent("sole")
        self._hold(agent, "sole-batch", {D_A: b"aaaa"})
        self.clock.advance(10_000_000)
        result = agent.expire(retention_seconds=60)
        self.assertEqual(result["bytes"], 0)
        self.assertEqual(result["protected_digests"], [D_A])
        self.assertTrue(agent.media.path(D_A).exists())
        # 中心一旦确认真实接收，到期即可清理。批次清单与代收点持有的一致。
        opened = self.open("sole-batch", ENTRIES)
        self.vault.deposits.receive(opened["id"], D_A, BytesIO(b"aaaa"))
        agent.sync(self.vault, collection="ridge", token="sole-batch")
        result = agent.expire(retention_seconds=60)
        self.assertEqual(result["bytes"], 4)
        self.assertFalse(agent.media.path(D_A).exists())

    def test_second_available_copy_allows_local_expiry(self):
        self.open("two-copies", {"f": [b"aaaa"]})
        agent_a = self._agent("copy-a")
        agent_b = self._agent("copy-b")
        raw = manifest({"f": [b"aaaa"]})
        agent_a.hold_parts("ridge", "two-copies", raw, {D_A: b"aaaa"})
        agent_b.hold_parts("ridge", "two-copies", raw, {D_A: b"aaaa"})
        self.clock.advance(10_000_000)
        # 中心还没有任何责任记录：两处都把自己当作最后副本，必须保护。
        self.assertEqual(agent_a.expire(retention_seconds=60)["protected_digests"], [D_A])
        # 两点都向中心声明了相同片，但都还没交字节（在途凭据分别可查）。
        id_a = agent_a.handoff_identity("ridge", "two-copies")
        id_b = agent_b.handoff_identity("ridge", "two-copies")
        self.vault.relays.declare("ridge", "two-copies", point="copy-a",
                                  handoff_id=id_a, parts=[D_A])
        self.vault.relays.declare("ridge", "two-copies", point="copy-b",
                                  handoff_id=id_b, parts=[D_A])
        # 直连中心时可看到另一份在途副本，A 的本地到期即可清。
        result_a = agent_a.expire(retention_seconds=60)
        self.assertEqual(result_a["bytes"], 4)
        # 离线代收点（不接 vault）未同步前仍只知道自己，继续保护。
        offline = Agent(self.root / "copy-b", clock=self.clock)
        result_b = offline.expire(retention_seconds=60)
        self.assertEqual(result_b["protected_digests"], [D_A])
        # 同步到另一副本信息或显式给出 peer 后才允许清理。
        offline.note_peer("copy-a", [D_A])
        result_b2 = offline.expire(retention_seconds=60)
        self.assertEqual(result_b2["bytes"], 4)

    def test_unpromised_or_explicit_peer_expiry(self):
        agent = self._agent("peer-note")
        raw = manifest({"f": [b"aaaa"]})
        agent.hold_parts("ridge", "peer-batch", raw, {D_A: b"aaaa"})
        self.clock.advance(10_000_000)
        agent.note_peer("south-desk", [D_A])
        result = agent.expire(retention_seconds=60)
        self.assertEqual(result["bytes"], 4)

    def test_parcel_accept_and_http_gateway_flow(self):
        source = self.root / "field"
        source.mkdir()
        (source / "scan.bin").write_bytes(b"aaaa" + b"bbbb")
        parcel_path = self.root / "parcel"
        build_parcel(source, parcel_path, collection="ridge", token="http-batch",
                     parent=0, lifetime=300, part_size=4)
        agent = Agent(self.root / "http-point", "http-north", clock=self.clock)
        receipt = agent.accept(open_parcel(parcel_path))
        self.assertEqual(receipt["part_count"], 2)

        class Endpoint:
            def __init__(self, http):
                self.http = http

            def request(self, method, path, **kwargs):
                reply = self.http.request(method, path, **kwargs)
                value = reply.json()
                if reply.is_error:
                    error = value["error"]
                    raise VaultError(error["code"], error["message"], reply.status_code,
                                     **error["details"])
                return value

        result = agent.forward(Endpoint(self.http))
        self.assertEqual(result["status"], "open")
        self.assertTrue(result["complete"])
        self.assertEqual(result["transmitted"], 2)
        self.assertEqual(agent.status()["releasable_bytes"], 8)
        center = self.http.get("/collections/ridge/batches/http-batch").json()
        self.assertTrue(center["publish_ready"])
        published = self.http.post(f"/deposits/{center['deposit']}/publish")
        self.assertEqual(published.status_code, 200)

    def test_direct_upload_and_relay_share_content_objects(self):
        # 另一个提交已经收过同样的字节并发布；新批打开时即按内容认领。
        self.edition("earlier", {"f": [b"aaaa"]})
        opened = self.open("shared-batch", {"g": [b"aaaa", b"cccc"]}, parent=1)
        self.assertEqual(len(opened["pending"]), 1)  # 只剩 cccc
        view = self.vault.relays.batch(deposit=opened["id"])
        self.assertIn(D_A, view["confirmed_parts"])
        self.vault.relays.declare("ridge", "shared-batch", point="n", handoff_id="shared-h")
        self.vault.relays.receive_part("shared-h", D_C, BytesIO(b"cccc"))
        self.assertTrue(self.vault.relays.batch(deposit=opened["id"])["publish_ready"])
