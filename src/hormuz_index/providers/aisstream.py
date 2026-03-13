from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import AsyncIterator

import websockets

from hormuz_index.config import Settings
from hormuz_index.models import PositionEvent, StaticEvent, ensure_utc, parse_datetime


def _coalesce_timestamp(payload: dict) -> datetime:
    for key in ("time_utc", "timestamp", "time"):
        meta = payload.get("MetaData", {})
        value = meta.get(key) or payload.get(key)
        parsed = parse_datetime(value)
        if parsed:
            return parsed
    return datetime.now(timezone.utc)


def _clean_name(value: str | None) -> str | None:
    if not value:
        return None
    stripped = value.strip()
    return stripped or None


def normalize_message(payload: dict, provider: str = "aisstream") -> list[PositionEvent | StaticEvent]:
    events: list[PositionEvent | StaticEvent] = []
    message_type = payload.get("MessageType")
    message = payload.get("Message", {})
    meta = payload.get("MetaData", {})
    observed_at = _coalesce_timestamp(payload)

    candidates: list[tuple[dict, str]] = []
    for key in (
        "PositionReport",
        "StandardClassBPositionReport",
        "ExtendedClassBPositionReport",
        "ShipStaticData",
        "StaticDataReport",
        "VoyageData",
    ):
        if key in message:
            candidates.append((message[key], key))

    if not candidates and isinstance(message, dict):
        for value in message.values():
            if isinstance(value, dict):
                candidates.append((value, message_type or "unknown"))

    for block, block_type in candidates:
        user_id = block.get("UserID") or block.get("MMSI") or meta.get("MMSI")
        if user_id is None:
            continue
        mmsi = int(user_id)
        vessel_name = _clean_name(block.get("Name") or meta.get("ShipName"))
        ship_type = block.get("Type") or block.get("ShipType") or meta.get("ShipType")
        ship_type = int(ship_type) if ship_type is not None else None

        lat = block.get("Latitude")
        lon = block.get("Longitude")
        if lat is not None and lon is not None:
            events.append(
                PositionEvent(
                    mmsi=mmsi,
                    observed_at=ensure_utc(observed_at),
                    latitude=float(lat),
                    longitude=float(lon),
                    sog=float(block.get("Sog")) if block.get("Sog") is not None else None,
                    cog=float(block.get("Cog")) if block.get("Cog") is not None else None,
                    ship_type=ship_type,
                    vessel_name=vessel_name,
                    provider=provider,
                    raw_payload=payload,
                )
            )
            continue

        events.append(
            StaticEvent(
                mmsi=mmsi,
                observed_at=ensure_utc(observed_at),
                ship_type=ship_type,
                vessel_name=vessel_name,
                provider=provider,
                raw_payload=payload,
            )
        )
    return events


async def stream_messages(settings: Settings) -> AsyncIterator[dict]:
    if not settings.aisstream_api_key:
        raise RuntimeError("AISSTREAM_API_KEY is not configured.")

    subscription = {
        "APIKey": settings.aisstream_api_key,
        "BoundingBoxes": settings.ais_bounding_box,
    }
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(settings.aisstream_ws_url, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps(subscription))
                backoff = 1.0
                async for raw_message in websocket:
                    yield json.loads(raw_message)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
