"""SSE byte-stream parsing internals.

Internal module: converts the raw ``text/event-stream`` bytes of the ABRP
v2 telemetry endpoint into raw SSE event blocks (:func:`iter_sse_events`)
and one event block into a JSON frame dict (:func:`parse_sse_event`).

PII contract: nothing in this module ever logs a frame body, header value,
or token — frame key names only.
"""

import json
import logging
from codecs import getincrementaldecoder
from collections.abc import AsyncIterator
from typing import Any

from aiohttp import StreamReader

from .exceptions import AbrpApiError

_LOGGER = logging.getLogger(__name__)


async def iter_sse_events(content: StreamReader) -> AsyncIterator[str]:
    r"""Yield raw ``\n\n``-terminated SSE event blocks from a byte stream.

    An incremental UTF-8 decoder preserves multi-byte sequences that span
    chunk boundaries — naive per-chunk decode would mojibake on Unicode
    vehicle names / location strings and the JSON parser would then reject
    the otherwise-valid frame.
    """
    decoder = getincrementaldecoder("utf-8")(errors="replace")
    buffer = ""
    async for chunk in content.iter_any():
        buffer += decoder.decode(chunk)
        # SSE spec accepts ``\n``, ``\r\n`` and ``\r`` as line
        # terminators. Normalize before splitting on ``\n\n``.
        # A trailing lone ``\r`` may be the first half of a ``\r\n``
        # pair that arrives in the next chunk — hold it back so the
        # pair-rewrite below sees the whole sequence and we don't
        # mistake a CR for an extra blank line. The re-appended ``\r``
        # stays un-normalized in the buffer until the next chunk's pass
        # — or the final flush — normalizes it, which is why the
        # flush-path normalization below is not redundant.
        if buffer.endswith("\r"):
            buffer, held_cr = buffer[:-1], "\r"
        else:
            held_cr = ""
        buffer = buffer.replace("\r\n", "\n").replace("\r", "\n")
        buffer += held_cr
        while "\n\n" in buffer:
            event, _, buffer = buffer.partition("\n\n")
            yield event
    # Flush the incremental decoder and any residual buffer in case the
    # server closed the stream gracefully without a trailing blank line.
    # Rare but observed on cold reconnects.
    buffer += decoder.decode(b"", final=True)
    buffer = buffer.replace("\r\n", "\n").replace("\r", "\n")
    if buffer.strip():
        yield buffer


def parse_sse_event(event: str) -> dict[str, Any] | None:
    r"""Parse one ``\n\n``-terminated SSE event block into a JSON frame dict.

    Returns ``None`` for events that carry only comments / keepalives, or
    for frames missing the required ``vehicleId`` key (probable upstream
    drift — drop quietly with a keys-only debug log rather than killing
    the SSE consumer). Raises :exc:`AbrpApiError` on malformed JSON or a
    decoded payload that is not a JSON object.
    """
    data_parts: list[str] = []
    for line in event.split("\n"):
        if not line or line.startswith(":"):
            # blank line (intra-event whitespace) or SSE comment (``: heartbeat``)
            continue
        if line.startswith("data:"):
            data_parts.append(line[5:].removeprefix(" "))
    if not data_parts:
        return None
    payload = "\n".join(data_parts)
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError as err:
        raise AbrpApiError(f"malformed SSE frame: {err}") from err
    if not isinstance(decoded, dict):
        raise AbrpApiError(f"unexpected SSE frame shape: {type(decoded).__name__}")
    if "vehicleId" not in decoded:
        # PII contract: key names only — never the frame body.
        _LOGGER.debug(
            "Dropping SSE frame missing 'vehicleId': keys=%s",
            sorted(decoded.keys()),
        )
        return None
    # Per-metric shape validation lives at the consumer (the extraction
    # layer) which tolerates missing/null keys.
    return decoded
