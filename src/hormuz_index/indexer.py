from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median

from hormuz_index.config import Settings
from hormuz_index.models import IndexPoint
from hormuz_index.storage import Database


def floor_bucket(dt: datetime, minutes: int) -> datetime:
    minute = (dt.minute // minutes) * minutes
    return dt.replace(minute=minute, second=0, microsecond=0)


class Indexer:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db

    def compute_latest(self, now: datetime | None = None, persist: bool = True) -> IndexPoint:
        now = now or datetime.now(timezone.utc)
        bucket_start = floor_bucket(now, self.settings.index_bucket_minutes)
        count_1h = self.db.transit_count_between(now - timedelta(hours=1), now)
        count_24h = self.db.transit_count_between(now - timedelta(hours=24), now)

        baseline_start = bucket_start - timedelta(days=self.settings.baseline_window_days)
        baseline_values = self.db.prior_index_counts(baseline_start, bucket_start)
        baseline = float(median(baseline_values)) if len(baseline_values) >= self.settings.baseline_min_points else None

        hourly_start = bucket_start - timedelta(days=self.settings.hourly_baseline_window_days)
        hourly_values = self.db.prior_hourly_counts(hourly_start, bucket_start)
        hourly_baseline = float(median(hourly_values)) if len(hourly_values) >= self.settings.baseline_min_points else None

        index_24h = None
        if baseline and baseline > 0:
            index_24h = round((count_24h / baseline) * 100, 2)

        point = IndexPoint(
            bucket_start=bucket_start,
            count_1h=count_1h,
            count_24h=count_24h,
            baseline_24h_median=baseline,
            hourly_baseline_median=hourly_baseline,
            index_24h=index_24h,
            generated_at=now,
        )
        if persist:
            self.db.upsert_index_point(point)
        return point
