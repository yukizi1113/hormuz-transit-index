from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from hormuz_index.config import Settings
from hormuz_index.models import IndexPoint, PositionEvent, StaticEvent, TransitEvent, VesselState, Zone, parse_datetime


class Database:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.path = settings.database_path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS raw_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mmsi INTEGER NOT NULL,
                    event_kind TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    observed_at_ts INTEGER NOT NULL,
                    latitude REAL,
                    longitude REAL,
                    sog REAL,
                    cog REAL,
                    ship_type INTEGER,
                    vessel_name TEXT,
                    provider TEXT NOT NULL,
                    payload_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_raw_events_ts ON raw_events(observed_at_ts);
                CREATE INDEX IF NOT EXISTS idx_raw_events_mmsi_ts ON raw_events(mmsi, observed_at_ts);

                CREATE TABLE IF NOT EXISTS vessel_registry (
                    mmsi INTEGER PRIMARY KEY,
                    vessel_name TEXT,
                    ship_type INTEGER,
                    last_seen_at TEXT,
                    last_seen_at_ts INTEGER,
                    provider TEXT
                );

                CREATE TABLE IF NOT EXISTS vessel_state (
                    mmsi INTEGER PRIMARY KEY,
                    last_zone TEXT NOT NULL,
                    last_seen_at TEXT,
                    last_seen_at_ts INTEGER,
                    west_seen_at TEXT,
                    west_seen_at_ts INTEGER,
                    middle_seen_at TEXT,
                    middle_seen_at_ts INTEGER,
                    east_seen_at TEXT,
                    east_seen_at_ts INTEGER,
                    last_transit_at TEXT,
                    last_transit_at_ts INTEGER,
                    vessel_name TEXT,
                    ship_type INTEGER
                );

                CREATE TABLE IF NOT EXISTS transits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mmsi INTEGER NOT NULL,
                    direction TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    started_at_ts INTEGER NOT NULL,
                    completed_at TEXT NOT NULL,
                    completed_at_ts INTEGER NOT NULL,
                    ship_type INTEGER,
                    vessel_name TEXT,
                    provider TEXT NOT NULL,
                    UNIQUE(mmsi, direction, completed_at_ts)
                );
                CREATE INDEX IF NOT EXISTS idx_transits_completed_ts ON transits(completed_at_ts);

                CREATE TABLE IF NOT EXISTS index_points (
                    bucket_start TEXT PRIMARY KEY,
                    bucket_start_ts INTEGER UNIQUE NOT NULL,
                    count_1h INTEGER NOT NULL,
                    count_24h INTEGER NOT NULL,
                    baseline_24h_median REAL,
                    hourly_baseline_median REAL,
                    index_24h REAL,
                    generated_at TEXT NOT NULL,
                    generated_at_ts INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_index_points_ts ON index_points(bucket_start_ts);

                CREATE TABLE IF NOT EXISTS alert_history (
                    alert_key TEXT PRIMARY KEY,
                    last_sent_at TEXT NOT NULL,
                    last_sent_at_ts INTEGER NOT NULL,
                    payload_json TEXT
                );
                """
            )

    def insert_position(self, event: PositionEvent) -> None:
        with self._connect() as conn:
            if self._raw_event_exists(
                conn,
                mmsi=event.mmsi,
                event_kind="position",
                observed_at_ts=event.observed_at_ts,
                provider=event.provider,
                latitude=event.latitude,
                longitude=event.longitude,
            ):
                self._upsert_registry(conn, event.mmsi, event.vessel_name, event.ship_type, event.observed_at, event.provider)
                return
            conn.execute(
                """
                INSERT INTO raw_events (
                    mmsi, event_kind, observed_at, observed_at_ts, latitude, longitude,
                    sog, cog, ship_type, vessel_name, provider, payload_json
                ) VALUES (?, 'position', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.mmsi,
                    event.observed_at.isoformat(),
                    event.observed_at_ts,
                    event.latitude,
                    event.longitude,
                    event.sog,
                    event.cog,
                    event.ship_type,
                    event.vessel_name,
                    event.provider,
                    json.dumps(event.raw_payload or {}, ensure_ascii=True),
                ),
            )
            self._upsert_registry(conn, event.mmsi, event.vessel_name, event.ship_type, event.observed_at, event.provider)

    def insert_static(self, event: StaticEvent) -> None:
        with self._connect() as conn:
            if self._raw_event_exists(
                conn,
                mmsi=event.mmsi,
                event_kind="static",
                observed_at_ts=event.observed_at_ts,
                provider=event.provider,
            ):
                self._upsert_registry(conn, event.mmsi, event.vessel_name, event.ship_type, event.observed_at, event.provider)
                return
            conn.execute(
                """
                INSERT INTO raw_events (
                    mmsi, event_kind, observed_at, observed_at_ts,
                    ship_type, vessel_name, provider, payload_json
                ) VALUES (?, 'static', ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.mmsi,
                    event.observed_at.isoformat(),
                    event.observed_at_ts,
                    event.ship_type,
                    event.vessel_name,
                    event.provider,
                    json.dumps(event.raw_payload or {}, ensure_ascii=True),
                ),
            )
            self._upsert_registry(conn, event.mmsi, event.vessel_name, event.ship_type, event.observed_at, event.provider)

    def _raw_event_exists(
        self,
        conn: sqlite3.Connection,
        mmsi: int,
        event_kind: str,
        observed_at_ts: int,
        provider: str,
        latitude: float | None = None,
        longitude: float | None = None,
    ) -> bool:
        if latitude is None or longitude is None:
            row = conn.execute(
                """
                SELECT 1
                FROM raw_events
                WHERE mmsi = ? AND event_kind = ? AND observed_at_ts = ? AND provider = ?
                LIMIT 1
                """,
                (mmsi, event_kind, observed_at_ts, provider),
            ).fetchone()
            return row is not None
        row = conn.execute(
            """
            SELECT 1
            FROM raw_events
            WHERE mmsi = ? AND event_kind = ? AND observed_at_ts = ? AND provider = ?
              AND latitude = ? AND longitude = ?
            LIMIT 1
            """,
            (mmsi, event_kind, observed_at_ts, provider, latitude, longitude),
        ).fetchone()
        return row is not None

    def _upsert_registry(
        self,
        conn: sqlite3.Connection,
        mmsi: int,
        vessel_name: str | None,
        ship_type: int | None,
        observed_at: datetime,
        provider: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO vessel_registry (mmsi, vessel_name, ship_type, last_seen_at, last_seen_at_ts, provider)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(mmsi) DO UPDATE SET
                vessel_name = COALESCE(excluded.vessel_name, vessel_registry.vessel_name),
                ship_type = COALESCE(excluded.ship_type, vessel_registry.ship_type),
                last_seen_at = excluded.last_seen_at,
                last_seen_at_ts = excluded.last_seen_at_ts,
                provider = excluded.provider
            """,
            (mmsi, vessel_name, ship_type, observed_at.isoformat(), int(observed_at.timestamp()), provider),
        )

    def load_state(self, mmsi: int) -> VesselState:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM vessel_state WHERE mmsi = ?", (mmsi,)).fetchone()
            registry = conn.execute("SELECT vessel_name, ship_type FROM vessel_registry WHERE mmsi = ?", (mmsi,)).fetchone()
        if row is None:
            return VesselState(
                mmsi=mmsi,
                vessel_name=registry["vessel_name"] if registry else None,
                ship_type=registry["ship_type"] if registry else None,
            )
        return VesselState(
            mmsi=row["mmsi"],
            last_zone=Zone(row["last_zone"]),
            last_seen_at=parse_datetime(row["last_seen_at"]),
            west_seen_at=parse_datetime(row["west_seen_at"]),
            middle_seen_at=parse_datetime(row["middle_seen_at"]),
            east_seen_at=parse_datetime(row["east_seen_at"]),
            last_transit_at=parse_datetime(row["last_transit_at"]),
            vessel_name=row["vessel_name"] or (registry["vessel_name"] if registry else None),
            ship_type=row["ship_type"] if row["ship_type"] is not None else (registry["ship_type"] if registry else None),
        )

    def save_state(self, state: VesselState) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO vessel_state (
                    mmsi, last_zone, last_seen_at, last_seen_at_ts, west_seen_at, west_seen_at_ts,
                    middle_seen_at, middle_seen_at_ts, east_seen_at, east_seen_at_ts,
                    last_transit_at, last_transit_at_ts, vessel_name, ship_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mmsi) DO UPDATE SET
                    last_zone = excluded.last_zone,
                    last_seen_at = excluded.last_seen_at,
                    last_seen_at_ts = excluded.last_seen_at_ts,
                    west_seen_at = excluded.west_seen_at,
                    west_seen_at_ts = excluded.west_seen_at_ts,
                    middle_seen_at = excluded.middle_seen_at,
                    middle_seen_at_ts = excluded.middle_seen_at_ts,
                    east_seen_at = excluded.east_seen_at,
                    east_seen_at_ts = excluded.east_seen_at_ts,
                    last_transit_at = excluded.last_transit_at,
                    last_transit_at_ts = excluded.last_transit_at_ts,
                    vessel_name = excluded.vessel_name,
                    ship_type = excluded.ship_type
                """,
                (
                    state.mmsi,
                    state.last_zone.value,
                    _iso(state.last_seen_at),
                    _ts(state.last_seen_at),
                    _iso(state.west_seen_at),
                    _ts(state.west_seen_at),
                    _iso(state.middle_seen_at),
                    _ts(state.middle_seen_at),
                    _iso(state.east_seen_at),
                    _ts(state.east_seen_at),
                    _iso(state.last_transit_at),
                    _ts(state.last_transit_at),
                    state.vessel_name,
                    state.ship_type,
                ),
            )

    def insert_transit(self, event: TransitEvent) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO transits (
                    mmsi, direction, started_at, started_at_ts, completed_at, completed_at_ts,
                    ship_type, vessel_name, provider
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.mmsi,
                    event.direction,
                    event.started_at.isoformat(),
                    event.started_at_ts,
                    event.completed_at.isoformat(),
                    event.completed_at_ts,
                    event.ship_type,
                    event.vessel_name,
                    event.provider,
                ),
            )
            return cursor.rowcount > 0

    def recent_transits(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT mmsi, direction, started_at, completed_at, ship_type, vessel_name, provider
                FROM transits
                ORDER BY completed_at_ts DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_raw_event_time(self) -> datetime | None:
        with self._connect() as conn:
            row = conn.execute("SELECT observed_at FROM raw_events ORDER BY observed_at_ts DESC LIMIT 1").fetchone()
        return parse_datetime(row["observed_at"]) if row else None

    def latest_transit_time(self) -> datetime | None:
        with self._connect() as conn:
            row = conn.execute("SELECT completed_at FROM transits ORDER BY completed_at_ts DESC LIMIT 1").fetchone()
        return parse_datetime(row["completed_at"]) if row else None

    def transit_count_between(self, start_at: datetime, end_at: datetime) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM transits WHERE completed_at_ts >= ? AND completed_at_ts < ?",
                (int(start_at.timestamp()), int(end_at.timestamp())),
            ).fetchone()
        return int(row["count"])

    def prior_index_counts(self, start_at: datetime, end_at: datetime) -> list[int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT count_24h
                FROM index_points
                WHERE bucket_start_ts >= ? AND bucket_start_ts < ?
                ORDER BY bucket_start_ts
                """,
                (int(start_at.timestamp()), int(end_at.timestamp())),
            ).fetchall()
        return [int(row["count_24h"]) for row in rows]

    def prior_hourly_counts(self, start_at: datetime, end_at: datetime) -> list[int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT count_1h
                FROM index_points
                WHERE bucket_start_ts >= ? AND bucket_start_ts < ?
                ORDER BY bucket_start_ts
                """,
                (int(start_at.timestamp()), int(end_at.timestamp())),
            ).fetchall()
        return [int(row["count_1h"]) for row in rows]

    def upsert_index_point(self, point: IndexPoint) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO index_points (
                    bucket_start, bucket_start_ts, count_1h, count_24h,
                    baseline_24h_median, hourly_baseline_median, index_24h,
                    generated_at, generated_at_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(bucket_start) DO UPDATE SET
                    count_1h = excluded.count_1h,
                    count_24h = excluded.count_24h,
                    baseline_24h_median = excluded.baseline_24h_median,
                    hourly_baseline_median = excluded.hourly_baseline_median,
                    index_24h = excluded.index_24h,
                    generated_at = excluded.generated_at,
                    generated_at_ts = excluded.generated_at_ts
                """,
                (
                    point.bucket_start.isoformat(),
                    point.bucket_start_ts,
                    point.count_1h,
                    point.count_24h,
                    point.baseline_24h_median,
                    point.hourly_baseline_median,
                    point.index_24h,
                    point.generated_at.isoformat(),
                    point.generated_at_ts,
                ),
            )

    def latest_index_point(self) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM index_points ORDER BY bucket_start_ts DESC LIMIT 1").fetchone()
        return dict(row) if row else None

    def index_history(self, hours: int) -> list[dict[str, Any]]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT bucket_start, count_1h, count_24h, baseline_24h_median, hourly_baseline_median, index_24h
                FROM index_points
                WHERE bucket_start_ts >= ?
                ORDER BY bucket_start_ts
                """,
                (int(cutoff.timestamp()),),
            ).fetchall()
        return [dict(row) for row in rows]

    def read_alert_history(self, alert_key: str) -> tuple[datetime, dict[str, Any] | None] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM alert_history WHERE alert_key = ?", (alert_key,)).fetchone()
        if row is None:
            return None
        payload = json.loads(row["payload_json"]) if row["payload_json"] else None
        return parse_datetime(row["last_sent_at"]) or datetime.now(timezone.utc), payload

    def upsert_alert_history(self, alert_key: str, when: datetime, payload: dict[str, Any] | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO alert_history (alert_key, last_sent_at, last_sent_at_ts, payload_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(alert_key) DO UPDATE SET
                    last_sent_at = excluded.last_sent_at,
                    last_sent_at_ts = excluded.last_sent_at_ts,
                    payload_json = excluded.payload_json
                """,
                (alert_key, when.isoformat(), int(when.timestamp()), json.dumps(payload or {}, ensure_ascii=True)),
            )


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _ts(value: datetime | None) -> int | None:
    return int(value.timestamp()) if value else None
