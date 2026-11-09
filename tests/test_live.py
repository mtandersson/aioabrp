"""Live tests against the real ABRP API (opt-in).

Deselected by default (``addopts = -m "not live and not live_write"``), so
plain ``uv run pytest`` and CI never touch the network. Run the read-only
ones explicitly::

    uv run pytest -m live

``pytest tests/test_live.py`` alone still deselects them; ``-m live`` is
what opts in. The write test runs only with exactly ``-m live_write`` and
needs ``ABRP_WRITE_VEHICLE_ID``; it sends the vehicle's current SoC back.

Credentials: ``ABRP_API_KEY`` (v2 partner key) and ``ABRP_ACCESS_TOKEN``
(per-user session token), from the environment or the repo-root ``.env``
(see ``.env.example``); the real environment wins. A missing value fails
the run. The access token expires after about 15 minutes; refresh it
before a run with the ABRP auth-cli (``go run . get-access-token``).

Requirements on the account: at least 1 vehicle in the garage, and at
least 2 for the ``set_vehicle_ids`` test (skipped otherwise), whose first
garage vehicle must be reporting telemetry. Assertions check shapes and
timing bounds, never values. Every expected failure goes through ``pytest.fail(...,
pytrace=False)`` with a message that names vehicles by garage index, so no
traceback, locals or rewritten assert values can print the token, ids or
positions.

The two unmarked tests at the bottom check the opt-in and credential rules
in the normal suite, without network.
"""

import asyncio
import dataclasses
import os
import re
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

import aiohttp
import pytest
from conftest import CallbackRecorder

from aioabrp import (
    AbrpApiError,
    AbrpAuthError,
    AbrpClient,
    AbrpVehicle,
    ConnectionState,
    Provider,
    StaticAuth,
    Telemetry,
    TelemetryPoint,
    TelemetryStream,
)
from aioabrp.stream import VEHICLES_CHANGED_REASON

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = REPO_ROOT / ".env"
REQUIRED_VARS = ("ABRP_API_KEY", "ABRP_ACCESS_TOKEN")
AUTH_HINT = (
    "ABRP rejected the credentials (access token expired/revoked or API key "
    "invalid). Refresh ABRP_ACCESS_TOKEN with the ABRP auth-cli: "
    "go run . get-access-token"
)
WRITE_VEHICLE_VAR = "ABRP_WRITE_VEHICLE_ID"
LIVE_WRITE_TESTS = {"test_send_current_soc_back"}
LIVE_TESTS = {
    "test_catalog_is_not_empty",
    "test_garage_display_matches_the_display_endpoint",
    "test_snapshot_for_every_vehicle",
    "test_stream_whole_garage_connects_and_updates",
    "test_set_vehicle_ids_grow_noop_empty_narrow",
}

live = pytest.mark.live
live_write = pytest.mark.live_write

LiveStreamFactory = Callable[[list[int], CallbackRecorder], TelemetryStream]


class _Garage(list[AbrpVehicle]):
    """The garage, with a repr that cannot print ids or nicknames."""

    def __repr__(self) -> str:
        return f"<garage: {len(self)} vehicles>"


def _check(condition: object, message: str) -> None:
    """Assert without a traceback (no locals, no rewritten values)."""
    __tracebackhide__ = True
    if not condition:
        pytest.fail(message, pytrace=False)


def _read_env_file(path: Path) -> dict[str, str]:
    """Parse a simple ``KEY=VALUE`` file (same rules as scripts/test_cli.py)."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return {}
    values: dict[str, str] = {}
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.removeprefix("export ").lstrip().partition("=")
        if sep:
            values[key.strip()] = value.strip().strip("\"'")
    return values


def load_credentials(
    environ: Mapping[str, str], env_file: Path
) -> tuple[str, StaticAuth]:
    """Return ``(api_key, auth)``; the environment wins over ``env_file``.

    Empty or whitespace values count as missing. Never writes os.environ.
    """
    __tracebackhide__ = True
    merged = _read_env_file(env_file)
    merged.update(
        {k: v for k, v in environ.items() if k in REQUIRED_VARS and v.strip()}
    )
    present = {k: merged.get(k, "").strip() for k in REQUIRED_VARS}
    missing = [k for k, v in present.items() if not v]
    if missing:
        pytest.fail(
            f"live tests need {', '.join(missing)} (environment or "
            f"{env_file.name}); see the tests/test_live.py docstring",
            pytrace=False,
        )
    return present["ABRP_API_KEY"], StaticAuth(present["ABRP_ACCESS_TOKEN"])


def load_write_vehicle_id(environ: Mapping[str, str], env_file: Path) -> int:
    """Return the vehicle the write test may post to; the environment wins."""
    __tracebackhide__ = True
    raw = (
        environ.get(WRITE_VEHICLE_VAR, "").strip()
        or _read_env_file(env_file).get(WRITE_VEHICLE_VAR, "").strip()
    )
    if not raw:
        pytest.fail(
            f"the live write test needs {WRITE_VEHICLE_VAR} (environment or "
            f"{env_file.name}): the id of a vehicle it may post to",
            pytrace=False,
        )
    try:
        return int(raw)
    except ValueError:
        pytest.fail(f"{WRITE_VEHICLE_VAR} must be an integer vehicle id", pytrace=False)


@contextmanager
def api_errors(what: str, *, redact: str = "") -> Iterator[None]:
    """Turn ABRP errors into one-line failures; ``redact`` is masked out."""
    __tracebackhide__ = True
    try:
        yield
    except AbrpAuthError as err:
        detail = str(err).replace(redact, "<redacted>") if redact else str(err)
        pytest.fail(f"{what}: {AUTH_HINT} ({detail})", pytrace=False)
    except AbrpApiError as err:
        # aiohttp error text can carry the request URL (and a vehicle id).
        detail = re.sub(r"url='[^']*'", "url=<redacted>", str(err))
        if redact:
            detail = detail.replace(redact, "<redacted>")
        pytest.fail(f"{what}: {type(err).__name__}: {detail}", pytrace=False)


async def _wait(
    recorder: CallbackRecorder,
    predicate: Callable[[], bool],
    within: float,
    what: str,
) -> None:
    """Wait for ``predicate``; fail fast on AUTH_FAILED, fail on timeout."""
    __tracebackhide__ = True

    def auth_failed() -> bool:
        return ConnectionState.AUTH_FAILED in recorder.states

    try:
        await recorder.wait_until(lambda: auth_failed() or predicate(), within)
    except TimeoutError:
        pytest.fail(f"timed out after {within:g} s waiting for {what}", pytrace=False)
    if auth_failed():
        pytest.fail(f"stream: {AUTH_HINT}", pytrace=False)


@pytest.fixture
async def client(websession: aiohttp.ClientSession) -> AbrpClient:
    return AbrpClient(websession, *load_credentials(os.environ, ENV_FILE))


@pytest.fixture
async def garage(client: AbrpClient) -> list[AbrpVehicle]:
    with api_errors("garage"):
        vehicles = _Garage(await client.async_get_vehicles())
    if not vehicles:
        pytest.fail("the garage is empty; live tests need >= 1 vehicle", pytrace=False)
    return vehicles


@pytest.fixture
async def live_stream(
    websession: aiohttp.ClientSession,
) -> AsyncIterator[LiveStreamFactory]:
    """Build real streams; every one is stopped on teardown."""
    api_key, auth = load_credentials(os.environ, ENV_FILE)
    created: list[TelemetryStream] = []

    def factory(vehicle_ids: list[int], recorder: CallbackRecorder) -> TelemetryStream:
        stream = TelemetryStream(
            websession,
            api_key,
            auth,
            vehicle_ids,
            recorder.on_update,
            recorder.on_connection_change,
            name="live",
        )
        created.append(stream)
        return stream

    yield factory
    for stream in created:
        await stream.stop()


@live
async def test_catalog_is_not_empty(client: AbrpClient) -> None:
    with api_errors("catalog"):
        catalog = await client.async_get_catalog()
    _check(catalog, "catalog returned no entries")


@live
async def test_garage_display_matches_the_display_endpoint(
    client: AbrpClient, garage: list[AbrpVehicle]
) -> None:
    for index, vehicle in enumerate(garage):
        _check(vehicle.relation is not None, f"garage vehicle #{index}: no relation")
    for typecode in sorted({vehicle.vehicle_model for vehicle in garage}):
        with api_errors(f"display for typecode {typecode!r}"):
            display = await client.async_get_vehicle_model_display(typecode)
        _check(display.title, f"empty display title for typecode {typecode!r}")
        _check(
            all(v.display == display for v in garage if v.vehicle_model == typecode),
            f"garage display differs from the display endpoint for {typecode!r}",
        )


@live
async def test_snapshot_for_every_vehicle(
    client: AbrpClient, garage: list[AbrpVehicle]
) -> None:
    metric_counts = []
    for index, vehicle in enumerate(garage):
        with api_errors(f"snapshot for garage vehicle #{index}"):
            telemetry = await client.async_get_current_telemetry(vehicle.vehicle_id)
        _check(
            isinstance(telemetry, Telemetry), f"garage vehicle #{index}: no Telemetry"
        )
        metric_counts.append(
            sum(
                getattr(telemetry, field.name) is not None
                for field in dataclasses.fields(telemetry)
            )
        )
    _check(max(metric_counts) >= 1, "no vehicle snapshot carried any metric")


@live
async def test_stream_whole_garage_connects_and_updates(
    garage: list[AbrpVehicle], live_stream: LiveStreamFactory
) -> None:
    recorder = CallbackRecorder()
    stream = live_stream([vehicle.vehicle_id for vehicle in garage], recorder)
    await stream.start()
    await _wait(
        recorder,
        lambda: ConnectionState.CONNECTED in recorder.states,
        10,
        "CONNECTED",
    )
    await _wait(recorder, lambda: bool(recorder.updates), 30, "a first on_update")


@live
async def test_set_vehicle_ids_grow_noop_empty_narrow(
    garage: list[AbrpVehicle], live_stream: LiveStreamFactory
) -> None:
    if len(garage) < 2:
        pytest.skip("set_vehicle_ids live test needs >= 2 vehicles in the garage")
    ids = [vehicle.vehicle_id for vehicle in garage]
    recorder = CallbackRecorder()
    stream = live_stream(ids[:1], recorder)
    await stream.start()
    await _wait(
        recorder,
        lambda: ConnectionState.CONNECTED in recorder.states,
        10,
        "the first CONNECTED",
    )

    # Grow: an immediate DISCONNECTED with the change reason, then CONNECTED.
    mark = len(recorder.events)
    stream.set_vehicle_ids(ids)
    await _wait(recorder, lambda: len(recorder.events) > mark, 1, "DISCONNECTED")
    event = recorder.events[mark]
    _check(
        event.state is ConnectionState.DISCONNECTED
        and event.reason == VEHICLES_CHANGED_REASON,
        f"grow: expected DISCONNECTED {VEHICLES_CHANGED_REASON!r}, got {event.state}",
    )
    await _wait(
        recorder,
        lambda: ConnectionState.CONNECTED in recorder.states[mark:],
        2,
        "CONNECTED after growing",
    )

    # Same set, reordered and duplicated: nothing happens.
    mark = len(recorder.events)
    stream.set_vehicle_ids([*reversed(ids), *ids])
    await asyncio.sleep(5)
    _check(not recorder.states[mark:], "no-op change emitted events")

    # Empty: exactly one DISCONNECTED, then the stream stays idle.
    mark = len(recorder.events)
    stream.set_vehicle_ids([])
    await _wait(recorder, lambda: len(recorder.events) > mark, 1, "DISCONNECTED")
    await asyncio.sleep(5)
    _check(
        recorder.states[mark:] == [ConnectionState.DISCONNECTED],
        f"empty: expected one DISCONNECTED, got {recorder.states[mark:]}",
    )

    # Narrow to one: reconnects, and only that vehicle is ever delivered.
    mark = len(recorder.events)
    update_mark = len(recorder.updates)
    stream.set_vehicle_ids(ids[:1])
    await _wait(
        recorder,
        lambda: ConnectionState.CONNECTED in recorder.states[mark:],
        10,
        "CONNECTED after narrowing",
    )
    await asyncio.sleep(15)
    others = [vid for vid, _ in recorder.updates[update_mark:] if vid != ids[0]]
    _check(not others, f"narrow: {len(others)} updates for unsubscribed vehicles")
    _check(
        recorder.states[mark:] == [ConnectionState.CONNECTED],
        f"narrow: expected only CONNECTED, got {recorder.states[mark:]}",
    )


@live_write
async def test_send_current_soc_back(client: AbrpClient) -> None:
    vehicle_id = load_write_vehicle_id(os.environ, ENV_FILE)
    with api_errors("snapshot for the write vehicle", redact=str(vehicle_id)):
        telemetry = await client.async_get_current_telemetry(vehicle_id)
    if telemetry.soc is None or telemetry.soc.time is None:
        pytest.skip("the write vehicle has no timed current SoC to send back")
    point = TelemetryPoint(
        provider=Provider.APP_OBD,
        time=telemetry.soc.time,
        soc_frac=telemetry.soc.value / 100,  # read side is percent
    )
    with api_errors("send telemetry", redact=str(vehicle_id)):
        await client.async_send_telemetry(vehicle_id, point)


# Normal suite (no network): the opt-in and the credential rules.


def _collect(*args: str) -> tuple[int, set[str]]:
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("ABRP_") and k != "PYTEST_ADDOPTS"
    }
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
            *args,
            str(Path(__file__).resolve()),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    names = {
        line.split("::")[-1] for line in result.stdout.splitlines() if "::" in line
    }
    return result.returncode, names


def test_live_tests_run_only_when_selected() -> None:
    _, default = _collect()
    code, selected = _collect("-m", "live")
    assert code == 0
    assert selected == LIVE_TESTS
    assert default
    assert not default & LIVE_TESTS, "live tests collected without -m live"
    assert not (default | selected) & LIVE_WRITE_TESTS, "write test selected"
    for broad in ("not live", ""):
        _, names = _collect("-m", broad)
        assert not names & LIVE_WRITE_TESTS, f"-m {broad!r} selects the write test"
    code, write = _collect("-m", "live_write")
    assert code == 0
    assert write == LIVE_WRITE_TESTS


async def test_credentials_env_wins_and_missing_vars_are_named(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        '# comment\nABRP_API_KEY=file-key\nexport ABRP_ACCESS_TOKEN="file-tok"\n'
    )
    api_key, auth = load_credentials(
        {"ABRP_API_KEY": "  ", "ABRP_ACCESS_TOKEN": "env-tok"}, env_file
    )
    assert api_key == "file-key"  # blank env value falls back to the file
    assert await auth.async_get_access_token() == "env-tok"
    _, auth = load_credentials({}, env_file)  # export prefix and quotes stripped
    assert await auth.async_get_access_token() == "file-tok"

    with pytest.raises(pytest.fail.Exception) as missing:
        load_credentials({}, tmp_path / "absent.env")
    assert "ABRP_API_KEY, ABRP_ACCESS_TOKEN" in str(missing.value)
    assert missing.value.pytrace is False  # no traceback, so no locals

    with (
        pytest.raises(pytest.fail.Exception, match="expired/revoked") as rejected,
        api_errors("garage"),
    ):
        raise AbrpAuthError("HTTP 401")
    assert rejected.value.pytrace is False


def test_write_vehicle_id_env_wins_and_bad_values_are_named(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(f"{WRITE_VEHICLE_VAR}=111\n")
    assert load_write_vehicle_id({}, env_file) == 111
    assert load_write_vehicle_id({WRITE_VEHICLE_VAR: "222"}, env_file) == 222

    with pytest.raises(pytest.fail.Exception, match=WRITE_VEHICLE_VAR) as missing:
        load_write_vehicle_id({WRITE_VEHICLE_VAR: " "}, tmp_path / "absent.env")
    assert missing.value.pytrace is False

    with pytest.raises(pytest.fail.Exception, match=WRITE_VEHICLE_VAR) as bad:
        load_write_vehicle_id({WRITE_VEHICLE_VAR: "car-42"}, env_file)
    assert "car-42" not in str(bad.value)
