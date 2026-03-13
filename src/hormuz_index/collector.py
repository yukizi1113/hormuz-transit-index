from __future__ import annotations

from pathlib import Path

from hormuz_index.config import Settings
from hormuz_index.models import PositionEvent, StaticEvent
from hormuz_index.providers.aisstream import normalize_message, stream_messages
from hormuz_index.providers.marinetraffic import normalize_response as normalize_marinetraffic_response
from hormuz_index.providers.marinetraffic import poll_messages as poll_marinetraffic_messages
from hormuz_index.providers.replay import replay_messages
from hormuz_index.storage import Database
from hormuz_index.transit import TransitDetector


async def _process_payload(payload: dict, db: Database, detector: TransitDetector, provider: str) -> int:
    count = 0
    if provider == "marinetraffic":
        normalized = normalize_marinetraffic_response(payload, provider=provider)
    else:
        normalized = normalize_message(payload, provider=provider)
    for event in normalized:
        count += 1
        if isinstance(event, PositionEvent):
            db.insert_position(event)
            detector.process(event)
        elif isinstance(event, StaticEvent):
            db.insert_static(event)
    return count


async def run_live_collector(settings: Settings, db: Database) -> int:
    if settings.marinetraffic_api_key:
        return await run_marinetraffic_collector(settings, db)
    return await run_aisstream_collector(settings, db)


async def run_marinetraffic_collector(settings: Settings, db: Database) -> int:
    detector = TransitDetector(settings, db)
    processed = 0
    async for payload in poll_marinetraffic_messages(settings):
        processed += await _process_payload(payload, db, detector, "marinetraffic")
    return processed


async def run_aisstream_collector(settings: Settings, db: Database) -> int:
    detector = TransitDetector(settings, db)
    processed = 0
    async for payload in stream_messages(settings):
        processed += await _process_payload(payload, db, detector, "aisstream")
    return processed


async def run_replay_collector(settings: Settings, db: Database, path: Path | None = None, sleep_sec: float = 0.0) -> int:
    detector = TransitDetector(settings, db)
    processed = 0
    replay_path = path or settings.ais_replay_file
    async for payload in replay_messages(replay_path, sleep_sec=sleep_sec):
        processed += await _process_payload(payload, db, detector, "replay")
    return processed
