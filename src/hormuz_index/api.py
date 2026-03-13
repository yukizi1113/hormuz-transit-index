from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI, Query

from hormuz_index.alerts import AlertService
from hormuz_index.config import Settings
from hormuz_index.indexer import Indexer
from hormuz_index.storage import Database


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.load()
    db = Database(settings)
    indexer = Indexer(settings, db)
    alerts = AlertService(settings, db)

    app = FastAPI(title=settings.app_name)

    @app.get("/health")
    def health() -> dict:
        now = datetime.now(timezone.utc)
        latest_raw = db.latest_raw_event_time()
        latest_transit = db.latest_transit_time()
        lag = None
        if latest_raw:
            lag = round((now - latest_raw).total_seconds(), 2)
        return {
            "status": "ok",
            "last_raw_event_at": latest_raw.isoformat() if latest_raw else None,
            "raw_event_lag_sec": lag,
            "last_transit_at": latest_transit.isoformat() if latest_transit else None,
            "latest_index_point": db.latest_index_point(),
        }

    @app.get("/index/latest")
    def latest_index() -> dict:
        point = indexer.compute_latest(persist=True)
        alerts.evaluate(point)
        return {
            "bucket_start": point.bucket_start.isoformat(),
            "count_1h": point.count_1h,
            "count_24h": point.count_24h,
            "baseline_24h_median": point.baseline_24h_median,
            "hourly_baseline_median": point.hourly_baseline_median,
            "index_24h": point.index_24h,
            "generated_at": point.generated_at.isoformat(),
        }

    @app.get("/index/history")
    def index_history(hours: int = Query(168, ge=1, le=24 * 180)) -> dict:
        return {"items": db.index_history(hours)}

    @app.get("/transits/recent")
    def recent_transits(limit: int = Query(100, ge=1, le=1000)) -> dict:
        return {"items": db.recent_transits(limit)}

    @app.post("/alerts/test")
    def send_test_alert() -> dict:
        return {"sent": alerts.send_test()}

    return app
