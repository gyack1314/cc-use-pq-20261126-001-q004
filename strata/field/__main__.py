"""提供离线捕获、续传与恢复验证命令。"""

import argparse
import json
from pathlib import Path

from strata.field.check import check_parcel
from strata.field.parcel import DEFAULT_PART, build_parcel, open_parcel
from strata.field.receive import verify_package
from strata.field.uplink import Client, Uplink
from strata.runtime.errors import VaultError, invalid


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(description=__doc__)
    commands = cli.add_subparsers(dest="command", required=True)

    build = commands.add_parser("capture", help="Capture the bytes of a local field directory")
    build.add_argument("source", type=Path)
    build.add_argument("parcel", type=Path)
    build.add_argument("--collection", required=True)
    build.add_argument("--token", required=True)
    build.add_argument("--parent", required=True, type=int)
    build.add_argument("--lifetime", type=int, default=7 * 86400)
    build.add_argument("--part-size", type=int, default=DEFAULT_PART)

    inspect = commands.add_parser("inspect", help="Read a parcel identity and the logical contents")
    inspect.add_argument("parcel", type=Path)

    check = commands.add_parser("check", help="Verify a field parcel before transit")
    check.add_argument("parcel", type=Path)
    check.add_argument("--limit", type=int, default=50)

    submit = commands.add_parser("submit", help="Send missing parts and optionally close the deposit")
    submit.add_argument("parcel", type=Path)
    submit.add_argument("--vault-url", required=True)
    submit.add_argument("--maximum-parts", type=int)
    submit.add_argument("--keep-open", action="store_true")

    receive = commands.add_parser("verify", help="Check a checkout against a known archival reference")
    receive.add_argument("package", type=Path)
    receive.add_argument("--collection", required=True)
    receive.add_argument("--revision", required=True, type=int)
    receive.add_argument("--manifest-sha256", required=True)
    receive.add_argument("--extract", type=Path)

    agent = commands.add_parser("agent", help="Operate a local relay (collection-on-behalf) point")
    agent.add_argument("action", choices=(
        "receive", "status", "forward", "sync", "release", "expire",
        "pause", "resume", "receipt",
    ))
    agent.add_argument("parcel", type=Path, nargs="?")
    agent.add_argument("--store", type=Path, required=True)
    agent.add_argument("--point", "--agent", dest="point")
    agent.add_argument("--vault-url")
    agent.add_argument("--collection")
    agent.add_argument("--token")
    agent.add_argument("--receipt-id")
    agent.add_argument("--publish", action="store_true")
    agent.add_argument("--maximum-parts", type=int)
    agent.add_argument("--retention", type=int)
    agent.add_argument("--sync", action="store_true")
    return cli


def execute(options) -> dict:
    if options.command == "capture":
        parcel = build_parcel(
            options.source, options.parcel, collection=options.collection, token=options.token,
            parent=options.parent, lifetime=options.lifetime, part_size=options.part_size,
        )
        return parcel.descriptor()
    if options.command == "inspect":
        return open_parcel(options.parcel).descriptor()
    if options.command == "check":
        return check_parcel(open_parcel(options.parcel), options.limit).as_dict()
    if options.command == "submit":
        parcel = open_parcel(options.parcel)
        with Client(options.vault_url) as endpoint:
            return Uplink(endpoint).send(parcel, publish=not options.keep_open,
                                         maximum_parts=options.maximum_parts).as_dict()
    if options.command == "agent":
        from strata.field.agent import Agent

        point = Agent(options.store, options.point)
        action = options.action
        if action == "receive":
            if options.parcel is None:
                raise invalid("agent receive requires a parcel path")
            return point.accept(options.parcel).as_dict()
        if action == "receipt":
            return point.receipt(options.receipt_id or options.token).as_dict()
        if action in ("pause", "resume"):
            return (point.pause() if action == "pause" else point.resume()).as_dict()
        if action == "expire":
            return point.expire(options.retention).as_dict()
        if action == "release":
            return point.release(options.collection, options.token).as_dict()
        if action == "status":
            if options.vault_url and options.sync:
                with Client(options.vault_url) as endpoint:
                    return point.status(options.collection, options.token,
                                        target=endpoint, sync=True).as_dict()
            return point.status(options.collection, options.token).as_dict()
        with Client(options.vault_url) as endpoint:
            if action == "sync":
                return point.sync(endpoint, collection=options.collection,
                                  token=options.token).as_dict()
            return point.forward(endpoint, collection=options.collection, token=options.token,
                                 publish=options.publish,
                                 maximum_parts=options.maximum_parts).as_dict()
    return verify_package(
        options.package, expected_manifest=options.manifest_sha256,
        expected_collection=options.collection, expected_revision=options.revision,
        destination=options.extract,
    ).as_dict()


def main() -> None:
    try:
        result = execute(parser().parse_args())
    except VaultError as error:
        print(json.dumps({"error": error.as_dict()}, ensure_ascii=False))
        raise SystemExit(2) from None
    except OSError as error:
        print(json.dumps({"error": {"code": "local_io", "message": str(error)}}, ensure_ascii=False))
        raise SystemExit(2) from None
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result.get("valid") is False:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
