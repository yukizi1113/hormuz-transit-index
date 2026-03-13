from datetime import datetime, timezone

from hormuz_index.config import Settings
from hormuz_index.indexer import Indexer
from hormuz_index.models import TransitEvent
from hormuz_index.storage import Database


def _settings(tmp_path):
    settings = Settings.load()
    return settings.__class__(**{**settings.__dict__, "database_path": tmp_path / "test.db"})


def test_index_computation_without_baseline(tmp_path) -> None:
    settings = _settings(tmp_path)
    db = Database(settings)
    db.insert_transit(
        TransitEvent(
            mmsi=1,
            direction="eastbound",
            started_at=datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 3, 10, 0, 30, tzinfo=timezone.utc),
            provider="test",
        )
    )
    indexer = Indexer(settings, db)
    point = indexer.compute_latest(now=datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc), persist=False)

    assert point.count_1h == 1
    assert point.count_24h == 1
    assert point.index_24h is None
