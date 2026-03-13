from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _db_path_from_url(url: str) -> Path:
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        raise ValueError("Only sqlite DATABASE_URL values are supported.")
    raw = url[len(prefix) :]
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT_DIR / path
    return path


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_host: str
    app_port: int
    timezone: str
    database_url: str
    database_path: Path
    ais_provider: str
    aisstream_api_key: str | None
    aisstream_ws_url: str
    ais_replay_file: Path
    ais_bbox_min_lat: float
    ais_bbox_min_lon: float
    ais_bbox_max_lat: float
    ais_bbox_max_lon: float
    corridor_min_lat: float
    corridor_max_lat: float
    west_gate_lon: float
    east_gate_lon: float
    vessel_scope: str
    transit_max_hours: float
    transit_cooldown_min: int
    index_bucket_minutes: int
    baseline_window_days: int
    baseline_min_points: int
    hourly_baseline_window_days: int
    alert_input_gap_minutes: int
    alert_index_threshold: float
    alert_hourly_drop_ratio: float
    discord_alert_enabled: bool
    discord_webhook_url: str | None
    discord_alert_cooldown_min: int

    @property
    def ais_bounding_box(self) -> list[list[list[float]]]:
        return [
            [
                [self.ais_bbox_min_lat, self.ais_bbox_min_lon],
                [self.ais_bbox_max_lat, self.ais_bbox_max_lon],
            ]
        ]

    @classmethod
    def load(cls) -> "Settings":
        database_url = os.getenv("DATABASE_URL", "sqlite:///./data/hormuz_index.db")
        return cls(
            app_name=os.getenv("APP_NAME", "Hormuz Transit Index"),
            app_host=os.getenv("APP_HOST", "127.0.0.1"),
            app_port=int(os.getenv("APP_PORT", "8010")),
            timezone=os.getenv("TIMEZONE", "Asia/Tokyo"),
            database_url=database_url,
            database_path=_db_path_from_url(database_url),
            ais_provider=os.getenv("AIS_PROVIDER", "auto"),
            aisstream_api_key=os.getenv("AISSTREAM_API_KEY"),
            aisstream_ws_url=os.getenv("AISSTREAM_WS_URL", "wss://stream.aisstream.io/v0/stream"),
            ais_replay_file=Path(os.getenv("AIS_REPLAY_FILE", "./sample_data/ais_sample.jsonl")),
            ais_bbox_min_lat=float(os.getenv("AIS_BBOX_MIN_LAT", "25.5")),
            ais_bbox_min_lon=float(os.getenv("AIS_BBOX_MIN_LON", "55.7")),
            ais_bbox_max_lat=float(os.getenv("AIS_BBOX_MAX_LAT", "26.9")),
            ais_bbox_max_lon=float(os.getenv("AIS_BBOX_MAX_LON", "57.5")),
            corridor_min_lat=float(os.getenv("CORRIDOR_MIN_LAT", "25.7")),
            corridor_max_lat=float(os.getenv("CORRIDOR_MAX_LAT", "26.7")),
            west_gate_lon=float(os.getenv("WEST_GATE_LON", "56.15")),
            east_gate_lon=float(os.getenv("EAST_GATE_LON", "56.95")),
            vessel_scope=os.getenv("VESSEL_SCOPE", "all_merchant"),
            transit_max_hours=float(os.getenv("TRANSIT_MAX_HOURS", "8")),
            transit_cooldown_min=int(os.getenv("TRANSIT_COOLDOWN_MIN", "180")),
            index_bucket_minutes=int(os.getenv("INDEX_BUCKET_MINUTES", "15")),
            baseline_window_days=int(os.getenv("BASELINE_WINDOW_DAYS", "28")),
            baseline_min_points=int(os.getenv("BASELINE_MIN_POINTS", "24")),
            hourly_baseline_window_days=int(os.getenv("HOURLY_BASELINE_WINDOW_DAYS", "7")),
            alert_input_gap_minutes=int(os.getenv("ALERT_INPUT_GAP_MINUTES", "20")),
            alert_index_threshold=float(os.getenv("ALERT_INDEX_THRESHOLD", "60")),
            alert_hourly_drop_ratio=float(os.getenv("ALERT_HOURLY_DROP_RATIO", "0.50")),
            discord_alert_enabled=_as_bool(os.getenv("DISCORD_ALERT_ENABLED"), False),
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL"),
            discord_alert_cooldown_min=int(os.getenv("DISCORD_ALERT_COOLDOWN_MIN", "30")),
        )
