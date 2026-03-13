from datetime import datetime, timezone

from hormuz_index.config import Settings
from hormuz_index.models import PositionEvent
from hormuz_index.storage import Database
from hormuz_index.transit import TransitDetector


def _event(mmsi: int, iso: str, lat: float, lon: float) -> PositionEvent:
    return PositionEvent(
        mmsi=mmsi,
        observed_at=datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc),
        latitude=lat,
        longitude=lon,
        provider="test",
    )


def _settings(tmp_path):
    settings = Settings.load()
    return settings.__class__(**{**settings.__dict__, "database_path": tmp_path / "test.db"})


def test_eastbound_transit_detection(tmp_path) -> None:
    settings = _settings(tmp_path)
    db = Database(settings)
    detector = TransitDetector(settings, db)

    assert detector.process(_event(1, "2026-03-10T00:00:00Z", 26.0, 56.0)) is None
    assert detector.process(_event(1, "2026-03-10T00:10:00Z", 26.0, 56.5)) is None
    transit = detector.process(_event(1, "2026-03-10T00:20:00Z", 26.0, 57.1))

    assert transit is not None
    assert transit.direction == "eastbound"


def test_westbound_transit_detection(tmp_path) -> None:
    settings = _settings(tmp_path)
    db = Database(settings)
    detector = TransitDetector(settings, db)

    assert detector.process(_event(2, "2026-03-10T02:00:00Z", 26.0, 57.1)) is None
    assert detector.process(_event(2, "2026-03-10T02:10:00Z", 26.0, 56.5)) is None
    transit = detector.process(_event(2, "2026-03-10T02:20:00Z", 26.0, 56.0))

    assert transit is not None
    assert transit.direction == "westbound"
