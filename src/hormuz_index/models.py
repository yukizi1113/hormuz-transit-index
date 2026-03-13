from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


class Zone(StrEnum):
    OUTSIDE = "outside"
    WEST = "west"
    MIDDLE = "middle"
    EAST = "east"


@dataclass(slots=True)
class PositionEvent:
    mmsi: int
    observed_at: datetime
    latitude: float
    longitude: float
    provider: str
    sog: float | None = None
    cog: float | None = None
    ship_type: int | None = None
    vessel_name: str | None = None
    raw_payload: dict[str, Any] | None = None

    @property
    def observed_at_ts(self) -> int:
        return int(self.observed_at.timestamp())


@dataclass(slots=True)
class StaticEvent:
    mmsi: int
    observed_at: datetime
    provider: str
    ship_type: int | None = None
    vessel_name: str | None = None
    raw_payload: dict[str, Any] | None = None

    @property
    def observed_at_ts(self) -> int:
        return int(self.observed_at.timestamp())


@dataclass(slots=True)
class TransitEvent:
    mmsi: int
    direction: str
    started_at: datetime
    completed_at: datetime
    provider: str
    ship_type: int | None = None
    vessel_name: str | None = None

    @property
    def started_at_ts(self) -> int:
        return int(self.started_at.timestamp())

    @property
    def completed_at_ts(self) -> int:
        return int(self.completed_at.timestamp())


@dataclass(slots=True)
class VesselState:
    mmsi: int
    last_zone: Zone = Zone.OUTSIDE
    last_seen_at: datetime | None = None
    west_seen_at: datetime | None = None
    middle_seen_at: datetime | None = None
    east_seen_at: datetime | None = None
    last_transit_at: datetime | None = None
    vessel_name: str | None = None
    ship_type: int | None = None


@dataclass(slots=True)
class IndexPoint:
    bucket_start: datetime
    count_1h: int
    count_24h: int
    baseline_24h_median: float | None
    index_24h: float | None
    generated_at: datetime
    hourly_baseline_median: float | None = None

    @property
    def bucket_start_ts(self) -> int:
        return int(self.bucket_start.timestamp())

    @property
    def generated_at_ts(self) -> int:
        return int(self.generated_at.timestamp())


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return ensure_utc(parsed)
