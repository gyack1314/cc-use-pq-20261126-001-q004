"""定义现场接收、长期保留和完整性取回的接口。"""

from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, Query, Request
from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr
from starlette.concurrency import run_in_threadpool

from strata.httpio import JsonLimit, download, error_response, uploaded
from strata.runtime.errors import VaultError
from strata.runtime.vault import Vault


class CollectionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: StrictStr
    retention_seconds: StrictInt


class DepositInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    token: StrictStr
    parent: StrictInt
    lifetime_seconds: StrictInt
    manifest: dict


class HoldInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: StrictStr


def create_app(root: Path, *, clock=None) -> FastAPI:
    vault = Vault(root, clock)
    app = FastAPI(title="Strata Vault", version="1.0.0")
    app.add_middleware(JsonLimit)
    app.state.vault = vault

    @app.exception_handler(VaultError)
    async def business_error(_request: Request, error: VaultError):
        return error_response(error)

    @app.post("/collections", status_code=201)
    def create_collection(body: CollectionInput):
        return vault.catalog.create_collection(body.name, body.retention_seconds)

    @app.get("/collections")
    def list_collections(after: str = "", limit: int = 50):
        return vault.catalog.collections(after, limit)

    @app.get("/collections/{collection}")
    def get_collection(collection: str):
        return vault.catalog.collection(collection)

    @app.post("/collections/{collection}/deposits", status_code=201)
    def open_deposit(collection: str, body: DepositInput):
        return vault.deposits.open(
            collection, body.token, body.parent, body.lifetime_seconds, body.manifest,
        )

    @app.get("/deposits/{deposit}")
    def get_deposit(deposit: str):
        return vault.deposits.get(deposit)

    @app.delete("/deposits/{deposit}")
    def cancel_deposit(deposit: str):
        return vault.deposits.cancel(deposit)

    @app.put("/deposits/{deposit}/parts/{digest}")
    async def receive_part(deposit: str, digest: str, request: Request):
        size = await run_in_threadpool(vault.deposits.required_size, deposit, digest)
        with await uploaded(request, size, vault.delivery.scratch) as source:
            return await run_in_threadpool(vault.deposits.receive, deposit, digest, source)

    @app.post("/deposits/{deposit}/publish")
    def publish(deposit: str):
        return vault.catalog.publish(deposit)

    @app.get("/collections/{collection}/revisions")
    def list_revisions(collection: str, after: int = 0, limit: int = 50):
        return vault.catalog.revisions(collection, after, limit)

    @app.get("/collections/{collection}/revisions/{number}")
    def get_revision(collection: str, number: int):
        return vault.catalog.revision(collection, number)

    @app.put("/collections/{collection}/revisions/{number}/holds/{reference}")
    def place_hold(collection: str, number: int, reference: str, body: HoldInput):
        return vault.custody.hold(collection, number, reference, body.reason)

    @app.delete("/collections/{collection}/revisions/{number}/holds/{reference}")
    def release_hold(collection: str, number: int, reference: str):
        return vault.custody.release(collection, number, reference)

    @app.post("/collections/{collection}/revisions/{number}/retire")
    def retire_revision(collection: str, number: int):
        row = vault.custody.retire(collection, number)
        return {"collection": collection, "revision": number, "retired_at": row["retired_at"]}

    @app.get("/collections/{collection}/revisions/{number}/file")
    def checkout_file(collection: str, number: int, request: Request, path: Annotated[str, Query()]):
        revision = vault.delivery.file(collection, number, path, request.headers.get("range"))
        return download(revision, media_type="application/octet-stream")

    @app.get("/collections/{collection}/revisions/{number}/package")
    def checkout_package(collection: str, number: int):
        return download(vault.delivery.package(collection, number), media_type="application/zip")

    @app.get("/operations/forecast")
    def capacity(limit: int = 50):
        return vault.custody.forecast(limit)

    @app.post("/operations/expire")
    def expire(limit: int = 100):
        return {"expired": vault.deposits.expire(limit)}

    @app.post("/operations/mark")
    def mark(limit: int = 100):
        return {"marked": vault.custody.mark(limit)}

    @app.post("/operations/sweep")
    def sweep(limit: int = 100):
        return vault.custody.sweep(limit)

    @app.post("/operations/reconcile")
    def reconcile(after: str = "", limit: int = 100, remove: bool = False):
        return vault.custody.reconcile(after, limit, remove=remove)

    @app.post("/operations/staging-cleanup")
    def clean_staging(limit: int = 100):
        return {"removed": vault.custody.clean_staging(limit)}

    @app.post("/operations/scan")
    def scan(after: str = "", limit: int = 25):
        return vault.integrity.scan(after, limit)

    @app.get("/operations/incidents")
    def incidents(after: int = 0, limit: int = 50, open_only: bool = True):
        return vault.integrity.incidents(after=after, limit=limit, open_only=open_only)

    @app.put("/operations/objects/{digest}/repair/{incident_id}")
    async def repair(digest: str, incident_id: int, request: Request):
        size = await run_in_threadpool(vault.integrity.repair_size, digest, incident_id)
        with await uploaded(request, size, vault.delivery.scratch) as source:
            return await run_in_threadpool(vault.integrity.repair, digest, incident_id, source)

    @app.get("/events")
    def events(collection: str | None = None, after: int = 0, limit: int = 50):
        return vault.events(collection, after, limit)


    @app.get("/collections/{collection}/revisions/{number}/coverage")
    def coverage(collection: str, number: int):
        return vault.inspection.coverage(collection, number)

    @app.get("/collections/{collection}/compare")
    def compare(collection: str, older: int, newer: int):
        return vault.inspection.compare(collection, older, newer)

    @app.get("/operations/objects/{digest}/references")
    def references(digest: str, limit: int = 50):
        return vault.inspection.object_references(digest, limit)

    @app.get("/operations/audit")
    def audit(collection: str | None = None, limit: int = 50):
        return vault.audit.check(collection, limit)

    @app.get("/collections/{collection}/footprint")
    def footprint(collection: str):
        return vault.footprint.collection(collection)

    # ---------------------------------------------------------- 代收转交

    def _handoff_payload(body: dict | None) -> dict:
        payload = dict(body or {})
        payload.pop("collection", None)
        return payload

    def _declare(collection: str | None = None, deposit: str | None = None, body: dict | None = None):
        payload = _handoff_payload(body)
        if deposit is not None:
            payload["deposit"] = deposit
        return vault.relays.declare(collection, payload.pop("token", None), **payload)

    @app.post("/handoffs", status_code=201)
    def create_handoff(body: dict | None = None):
        payload = _handoff_payload(body)
        return vault.relays.declare(payload.pop("collection", None), payload.pop("token", None), **payload)

    @app.post("/relay/handoffs", status_code=201)
    def create_relay_handoff(body: dict | None = None):
        return create_handoff(body)

    @app.post("/agents/handoffs", status_code=201)
    def create_agent_handoff(body: dict | None = None):
        return create_handoff(body)

    @app.post("/collections/{collection}/handoffs", status_code=201)
    def collection_handoff(collection: str, body: dict | None = None):
        return _declare(collection, body=body)

    @app.post("/collections/{collection}/relay/handoffs", status_code=201)
    def collection_relay_handoff(collection: str, body: dict | None = None):
        return _declare(collection, body=body)

    @app.post("/collections/{collection}/agents/handoffs", status_code=201)
    def collection_agent_handoff(collection: str, body: dict | None = None):
        return _declare(collection, body=body)

    @app.post("/deposits/{deposit}/handoffs", status_code=201)
    def deposit_handoff(deposit: str, body: dict | None = None):
        return _declare(None, deposit=deposit, body=body)

    @app.post("/deposits/{deposit}/relay", status_code=201)
    def deposit_relay(deposit: str, body: dict | None = None):
        return _declare(None, deposit=deposit, body=body)

    @app.get("/handoffs/{handoff}")
    def get_handoff(handoff: str):
        return vault.relays.get(handoff)

    @app.get("/relay/handoffs/{handoff}")
    def get_relay_handoff(handoff: str):
        return vault.relays.get(handoff)

    @app.get("/credentials/{handoff}")
    def get_credential(handoff: str):
        return vault.relays.get(handoff)

    @app.get("/agents/credentials/{handoff}")
    def get_agent_credential(handoff: str):
        return vault.relays.get(handoff)

    async def _handoff_part(handoff: str, digest: str, request: Request):
        size = await run_in_threadpool(vault.relays.required_size, handoff, digest)
        with await uploaded(request, size, vault.delivery.scratch) as source:
            return await run_in_threadpool(vault.relays.receive_part, handoff, digest, source)

    @app.put("/handoffs/{handoff}/parts/{digest}")
    async def put_handoff_part(handoff: str, digest: str, request: Request):
        return await _handoff_part(handoff, digest, request)

    @app.put("/relay/handoffs/{handoff}/parts/{digest}")
    async def put_relay_handoff_part(handoff: str, digest: str, request: Request):
        return await _handoff_part(handoff, digest, request)

    @app.put("/credentials/{handoff}/parts/{digest}")
    async def put_credential_part(handoff: str, digest: str, request: Request):
        return await _handoff_part(handoff, digest, request)

    @app.get("/collections/{collection}/batches/{token}")
    def batch_status(collection: str, token: str):
        return vault.relays.batch(collection, token)

    @app.get("/collections/{collection}/relay/batches/{token}")
    def relay_batch_status(collection: str, token: str):
        return vault.relays.batch(collection, token)

    @app.get("/collections/{collection}/tokens/{token}/readiness")
    def token_readiness(collection: str, token: str):
        return vault.relays.batch(collection, token)

    @app.get("/deposits/{deposit}/readiness")
    def deposit_readiness(deposit: str):
        return vault.relays.batch(deposit=deposit)

    @app.get("/deposits/{deposit}/relay")
    def deposit_relay_view(deposit: str):
        return vault.relays.batch(deposit=deposit)

    @app.get("/deposits/{deposit}/custody")
    def deposit_custody(deposit: str):
        return vault.relays.custody(deposit=deposit)

    @app.get("/deposits/{deposit}/handoffs")
    def deposit_handoffs(deposit: str):
        return vault.relays.list_handoffs(deposit=deposit)

    @app.get("/collections/{collection}/handoffs")
    def collection_handoffs(collection: str):
        return vault.relays.list_handoffs(collection)

    @app.get("/relay/copies/{digest}")
    def relay_copies(digest: str):
        return vault.relays.copies(digest)

    # 其他常见资源命名的别名。
    @app.post("/transfers", status_code=201)
    def create_transfer(body: dict | None = None):
        return create_handoff(body)

    @app.post("/deposits/{deposit}/transfers", status_code=201)
    def deposit_transfer(deposit: str, body: dict | None = None):
        return _declare(None, deposit=deposit, body=body)

    @app.post("/deposits/{deposit}/credentials", status_code=201)
    def deposit_credential(deposit: str, body: dict | None = None):
        return _declare(None, deposit=deposit, body=body)

    @app.get("/transfers/{handoff}")
    def get_transfer(handoff: str):
        return vault.relays.get(handoff)

    @app.get("/receipts/{handoff}")
    def get_receipt_alias(handoff: str):
        return vault.relays.get(handoff)

    @app.put("/transfers/{handoff}/parts/{digest}")
    async def put_transfer_part(handoff: str, digest: str, request: Request):
        return await _handoff_part(handoff, digest, request)

    @app.get("/deposits/{deposit}/status")
    def deposit_status_view(deposit: str):
        return vault.relays.batch(deposit=deposit)

    @app.get("/deposits/{deposit}/summary")
    def deposit_summary_view(deposit: str):
        return vault.deposits.get(deposit)

    @app.get("/collections/{collection}/tokens/{token}")
    def token_batch_view(collection: str, token: str):
        return vault.relays.batch(collection, token)

    return app
