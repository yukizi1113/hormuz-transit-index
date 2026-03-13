from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import requests

from hormuz_index.config import Settings
from hormuz_index.models import PositionEvent, ensure_utc, parse_datetime


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _timestamp_from_row(row: dict[str, Any]) -> datetime:
    for key in ("TIMESTAMP", "LAST_PORT_TIME", "TIMESTAMP_SECONDS"):
        parsed = parse_datetime(str(row.get(key))) if row.get(key) else None
        if parsed:
            return parsed
    return datetime.now(timezone.utc)


def normalize_response(payload: dict[str, Any], provider: str = "marinetraffic") -> list[PositionEvent]:
    rows = payload.get("DATA", [])
    if isinstance(payload, list):
        rows = payload

    events: list[PositionEvent] = []
    for row in rows:
        mmsi = _safe_int(row.get("MMSI"))
        latitude = _safe_float(row.get("LAT"))
        longitude = _safe_float(row.get("LON"))
        if mmsi is None or latitude is None or longitude is None:
            continue

        speed = _safe_float(row.get("SPEED"))
        course = _safe_float(row.get("COURSE"))
        ship_type = _safe_int(row.get("SHIPTYPE"))
        vessel_name = (row.get("SHIPNAME") or "").strip() or None
        if speed is not None and speed > 100:
            speed = speed / 10.0

        events.append(
            PositionEvent(
                mmsi=mmsi,
                observed_at=ensure_utc(_timestamp_from_row(row)),
                latitude=latitude,
                longitude=longitude,
                sog=speed,
                cog=course,
                ship_type=ship_type,
                vessel_name=vessel_name,
                provider=provider,
                raw_payload=row,
            )
        )
    return events


def fetch_page(settings: Settings, cursor: str | None = None) -> dict[str, Any]:
    if not settings.marinetraffic_api_key:
        raise RuntimeError("MARINETRAFFIC_API_KEY is not configured.")

    params: dict[str, Any] = {
        "v": settings.marinetraffic_api_version,
        "protocol": "jsono",
        "timespan": settings.marinetraffic_timespan_min,
        "limit": settings.marinetraffic_limit,
    }
    if settings.marinetraffic_vesseltype_ids:
        params["vesseltypeid"] = settings.marinetraffic_vesseltype_ids
    if cursor:
        params["cursor"] = cursor

    response = requests.get(
        settings.marinetraffic_url,
        params=params,
        timeout=settings.marinetraffic_timeout_sec,
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, list):
        return {"DATA": payload, "METADATA": {}}
    return payload


async def poll_messages(settings: Settings) -> AsyncIterator[dict[str, Any]]:
    backoff = 1.0
    while True:
        try:
            cursor: str | None = None
            for _ in range(settings.marinetraffic_max_pages_per_poll):
                payload = await asyncio.to_thread(fetch_page, settings, cursor)
                yield payload
                cursor = ((payload.get("METADATA") or {}).get("CURSOR")) or None
                if not cursor:
                    break
            backoff = 1.0
            await asyncio.sleep(settings.marinetraffic_poll_sec)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, float(settings.marinetraffic_poll_sec))
