"""API constants for the ABRP / Iternio telemetry API."""

API_BASE_V1 = "https://api.iternio.com/1"
# Probe-confirmed v2 base is ``/2`` (not ``/v2``); the swagger ``servers.url``
# agrees.
API_BASE_V2 = "https://api.iternio.com/2"
ENDPOINT_GET_TLM = "session/get_tlm"
ENDPOINT_TLM = "tlm"
ENDPOINT_VEHICLE_LIST = "vehicle/_list"

# v2 splits auth across two headers: the static partner key in ``X-API-KEY``
# and the per-user session (OAuth ``access_token``) in ``X-ABRP-SESSION``.
HEADER_API_KEY = "X-API-KEY"
HEADER_ABRP_SESSION = "X-ABRP-SESSION"

# One-shot GET (``/2/tlm/{vehicle_id}``) is best-effort: a hung response must
# not block the caller indefinitely. A telemetry stream backfills any missed
# metrics on its first frame, so 30 s is a generous upper bound that still
# bounds the request.
ONE_SHOT_TIMEOUT_SECONDS = 30

# Pre-headers handshake bounds for the long-lived SSE GET. The total budget
# stays ``None`` — the stream runs for the lifetime of the connection (target
# ~200s server-close cadence per the heartbeat probe) and a total-elapsed
# budget would tear it down. ``connect`` covers DNS + TCP + TLS as a whole;
# ``sock_connect`` bounds the bare TCP handshake inside that envelope so a
# wedged DNS/TLS step still surfaces within ``sock_connect`` rather than
# waiting out the full ``connect`` budget. Detection of in-stream stall is
# handled by the stream's wall-clock watchdog, NOT ``sock_read`` — see the
# SSE-loop docstring in stream.py.
SSE_CONNECT_TIMEOUT_SECONDS = 30
SSE_SOCK_CONNECT_TIMEOUT_SECONDS = 15

# Reconnect backoff ladder for the SSE stream. Resets to the first delay
# after at least one frame is received from a connection attempt.
DEFAULT_BACKOFF_SECONDS: tuple[float, ...] = (5.0, 10.0, 30.0, 60.0)

# Maximum wall-clock gap between consecutive SSE frames before the stream
# treats the connection as stalled and forces a reconnect. Probe-confirmed
# empirical: the ABRP server emits no keepalive comments on idle streams and
# unilaterally closes at ~200 s (deterministic). This threshold sits ~100 s
# above that natural cycle — large enough that the payload error from the
# legitimate 200 s server-close always fires first on healthy paths; small
# enough that a half-open-TCP stall (observed ≥16 h in production) is
# detected within ~5 minutes. Tunable upward if legitimate active streams
# turn out to go silent across this window.
DEFAULT_WATCHDOG_SECONDS = 300.0
