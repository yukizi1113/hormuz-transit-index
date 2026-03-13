from hormuz_index.config import Settings
from hormuz_index.geo import zone_for_position
from hormuz_index.models import Zone


def test_zone_for_position() -> None:
    settings = Settings.load()
    assert zone_for_position(26.0, 56.0, settings) is Zone.WEST
    assert zone_for_position(26.0, 56.5, settings) is Zone.MIDDLE
    assert zone_for_position(26.0, 57.1, settings) is Zone.EAST
    assert zone_for_position(27.5, 56.5, settings) is Zone.OUTSIDE
