#!/usr/bin/env python
"""Manual local smoke test for aioabrp — NOT part of the package.

Exercises the live ABRP API against a real account so you can eyeball the
library's surface before merging. Organised as subcommands:

    garage              List the vehicles in the account's garage.
    display [TYPECODE]  Display metadata for the given typecodes (or, with
                        none, every garage vehicle's typecode).
    snapshot            One-shot telemetry snapshot per vehicle.
    stream              Stream live telemetry to stdout until Ctrl+C
                        (the full 26-metric surface).

Credentials come from flags, the environment, or a .env file (never stored in
this script):

    --api-key / ABRP_API_KEY      Iternio *partner* API key.
    --token   / ABRP_ACCESS_TOKEN Per-user ABRP access token.

The API key is the Iternio partner key, not a per-user credential — see the
project README "Get your own API key". The simplest setup is a .env file in
the repo root (auto-loaded; copy .env.example):

    # .env
    ABRP_API_KEY=your-iternio-partner-api-key
    ABRP_ACCESS_TOKEN=your-abrp-access-token

Real environment variables take precedence over .env; use --env-file to point
at a different path. Run from the repo root so it resolves the project venv:

    uv run python scripts/test_cli.py garage
    uv run python scripts/test_cli.py display
    uv run python scripts/test_cli.py display rivian:r2:26:ncma91:rwd:w21
    uv run python scripts/test_cli.py snapshot --vehicle 12345
    uv run python scripts/test_cli.py stream --no-snapshot
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
from datetime import datetime

import aiohttp

from aioabrp import (
    AbrpClient,
    AbrpError,
    ChargingState,
    ConnectionEvent,
    DrivingState,
    Location,
    MapInfo,
    StaticAuth,
    Telemetry,
    TelemetryStream,
    VehicleModelDisplay,
)

# ---------- rendering ---------------------------------------------------------


def _fmt_value(value: object) -> str:
    """Render a MetricValue.value compactly for one stdout line."""
    if isinstance(value, Location):
        return f"{value.lat:.5f},{value.lon:.5f}"
    if isinstance(value, MapInfo):
        parts = [
            f"region={value.region.value if value.region else None}",
            f"country={value.country_3}",
            f"address={value.address!r}",
            f"speed_limit_ms={value.speed_limit_ms}",
            f"free_speed={value.is_free_speed_zone}",
        ]
        return "{" + ", ".join(parts) + "}"
    if isinstance(value, (ChargingState, DrivingState)):
        return value.value
    return repr(value)


def _stamp() -> str:
    """Local wall-clock receipt time, for ordering the stdout stream."""
    return datetime.now().astimezone().strftime("%H:%M:%S")


def _print_telemetry(vehicle_id: int, telemetry: Telemetry) -> None:
    for metric, mv in telemetry.items():
        wire_time = mv.time.isoformat() if mv.time else "-"
        provider = mv.provider or "-"
        print(
            f"[{_stamp()}] vehicle={vehicle_id} {metric.value:<22} "
            f"= {_fmt_value(mv.value):<40} (time={wire_time} provider={provider})",
            flush=True,
        )


def _print_display(typecode: str, display: VehicleModelDisplay) -> None:
    print(
        f"[{_stamp()}] {typecode:<36} "
        f"{display.manufacturer} {display.model} — {display.title} "
        f"(years={display.years!r} start={display.start_year} end={display.end_year})",
        flush=True,
    )


def _on_update(vehicle_id: int, telemetry: Telemetry) -> None:
    _print_telemetry(vehicle_id, telemetry)


def _on_connection_change(event: ConnectionEvent) -> None:
    reason = f" ({event.reason})" if event.reason else ""
    print(f"[{_stamp()}] -- connection: {event.state.name}{reason}", flush=True)


# ---------- shared helpers ----------------------------------------------------


async def _print_garage(client: AbrpClient) -> list[int]:
    """Fetch and print the garage; return the vehicle ids in order."""
    print(f"[{_stamp()}] fetching garage ...", flush=True)
    garage = await client.async_get_vehicles()
    for v in garage:
        print(
            f"[{_stamp()}] vehicle {v.vehicle_id}: "
            f"{v.name or '(no name)'} [{v.vehicle_model}]",
            flush=True,
        )
    return [v.vehicle_id for v in garage]


async def _resolve_vehicle_ids(
    client: AbrpClient, requested: list[int] | None
) -> list[int]:
    """Use the explicitly requested ids, else fall back to the whole garage."""
    if requested:
        return requested
    return await _print_garage(client)


# ---------- subcommands -------------------------------------------------------


async def _cmd_garage(client: AbrpClient) -> None:
    await _print_garage(client)


async def _cmd_display(client: AbrpClient, typecodes: list[str]) -> None:
    if typecodes:
        targets = typecodes
    else:
        print(f"[{_stamp()}] fetching garage to resolve typecodes ...", flush=True)
        garage = await client.async_get_vehicles()
        # The garage's vehicle_model field is the catalog typecode; dedupe
        # while preserving order so shared typecodes aren't fetched twice.
        targets = list(dict.fromkeys(v.vehicle_model for v in garage))

    if not targets:
        print("No typecodes to look up — exiting.", flush=True)
        return

    for typecode in targets:
        try:
            _print_display(
                typecode, await client.async_get_vehicle_model_display(typecode)
            )
        except AbrpError as err:
            print(f"[{_stamp()}] display failed for {typecode}: {err}", flush=True)


async def _cmd_snapshot(client: AbrpClient, vehicles: list[int] | None) -> None:
    vehicle_ids = await _resolve_vehicle_ids(client, vehicles)
    if not vehicle_ids:
        print("No vehicles to snapshot — exiting.", flush=True)
        return
    print(f"[{_stamp()}] one-shot snapshot per vehicle:", flush=True)
    for vid in vehicle_ids:
        try:
            _print_telemetry(vid, await client.async_get_current_telemetry(vid))
        except AbrpError as err:
            print(f"[{_stamp()}] snapshot failed for {vid}: {err}", flush=True)


async def _cmd_stream(
    session: aiohttp.ClientSession,
    client: AbrpClient,
    auth: StaticAuth,
    api_key: str,
    vehicles: list[int] | None,
    snapshot: bool,
) -> None:
    vehicle_ids = await _resolve_vehicle_ids(client, vehicles)
    if not vehicle_ids:
        print("No vehicles to stream — exiting.", flush=True)
        return
    print(f"[{_stamp()}] streaming vehicles: {vehicle_ids}", flush=True)

    if snapshot:
        await _cmd_snapshot(client, vehicle_ids)

    stream = TelemetryStream(
        session,
        api_key,
        auth,
        vehicle_ids=vehicle_ids,
        on_update=_on_update,
        on_connection_change=_on_connection_change,
        name="stream-test",
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    await stream.start()
    print(f"[{_stamp()}] streaming — Ctrl+C to stop.", flush=True)
    try:
        await stop.wait()
    finally:
        print(f"\n[{_stamp()}] stopping ...", flush=True)
        await stream.stop()


# ---------- entrypoint --------------------------------------------------------


def _load_env_file(path: str) -> None:
    """Populate os.environ from a simple ``KEY=VALUE`` .env file, if present.

    Zero-dependency and intentionally minimal: blank lines and ``#`` comments
    are skipped, an optional leading ``export`` and surrounding single/double
    quotes are stripped. Real environment variables already set take
    precedence (this only fills gaps), and a missing file is a no-op.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            lines = handle.readlines()
    except FileNotFoundError:
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.removeprefix("export ").lstrip()
        key, sep, value = line.partition("=")
        if not sep:
            continue
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


async def _run(args: argparse.Namespace) -> None:
    auth = StaticAuth(args.token)
    async with aiohttp.ClientSession() as session:
        client = AbrpClient(session, args.api_key, auth)
        if args.command == "garage":
            await _cmd_garage(client)
        elif args.command == "display":
            await _cmd_display(client, args.typecodes)
        elif args.command == "snapshot":
            await _cmd_snapshot(client, args.vehicles)
        elif args.command == "stream":
            await _cmd_stream(
                session, client, auth, args.api_key, args.vehicles, args.snapshot
            )


def _build_parser() -> argparse.ArgumentParser:
    # Credentials + logging are shared by every subcommand, so they live on a
    # parent parser (add_help=False) that each subparser inherits.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--env-file",
        default=".env",
        metavar="PATH",
        help="Load credentials from this KEY=VALUE file if present (default: .env). "
        "Real environment variables take precedence.",
    )
    common.add_argument(
        "--api-key",
        default=os.environ.get("ABRP_API_KEY"),
        help="Iternio partner API key (or env ABRP_API_KEY).",
    )
    common.add_argument(
        "--token",
        default=os.environ.get("ABRP_ACCESS_TOKEN"),
        help="ABRP access token (or env ABRP_ACCESS_TOKEN).",
    )
    common.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help=(
            "aioabrp logger level. INFO shows connect/disconnect/reconnect + "
            "backoff; DEBUG adds per-frame detail. Default: WARNING."
        ),
    )

    parser = argparse.ArgumentParser(
        description="Manual smoke test for aioabrp against a live ABRP account.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    sub.add_parser(
        "garage",
        parents=[common],
        help="List the vehicles in the garage.",
    )

    p_display = sub.add_parser(
        "display",
        parents=[common],
        help="Show vehicle-model display metadata for typecodes.",
    )
    p_display.add_argument(
        "typecodes",
        nargs="*",
        metavar="TYPECODE",
        help="Typecode(s) to look up. Default: every garage vehicle's typecode.",
    )

    p_snapshot = sub.add_parser(
        "snapshot",
        parents=[common],
        help="One-shot telemetry snapshot per vehicle.",
    )
    p_snapshot.add_argument(
        "--vehicle",
        type=int,
        action="append",
        dest="vehicles",
        metavar="ID",
        help="Vehicle id (repeatable). Default: all in the garage.",
    )

    p_stream = sub.add_parser(
        "stream",
        parents=[common],
        help="Stream live telemetry to stdout until Ctrl+C.",
    )
    p_stream.add_argument(
        "--vehicle",
        type=int,
        action="append",
        dest="vehicles",
        metavar="ID",
        help="Vehicle id to stream (repeatable). Default: all in the garage.",
    )
    p_stream.add_argument(
        "--no-snapshot",
        action="store_false",
        dest="snapshot",
        help="Skip the initial one-shot telemetry snapshot.",
    )

    return parser


def main() -> None:
    """Parse arguments, configure logging, and dispatch the subcommand."""
    # Resolve --env-file first (before the main parser computes its credential
    # defaults from os.environ) so a .env file can supply ABRP_API_KEY /
    # ABRP_ACCESS_TOKEN. Real environment variables still win.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--env-file", default=".env")
    pre_args, _ = pre.parse_known_args()
    _load_env_file(pre_args.env_file)

    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("aioabrp").setLevel(args.log_level)

    if not args.api_key:
        parser.error("missing API key: pass --api-key or set ABRP_API_KEY")
    if not args.token:
        parser.error("missing access token: pass --token or set ABRP_ACCESS_TOKEN")

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_run(args))


if __name__ == "__main__":
    main()
