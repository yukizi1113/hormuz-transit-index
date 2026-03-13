from __future__ import annotations

from hormuz_index.config import Settings
from hormuz_index.models import Zone


def inside_bbox(latitude: float, longitude: float, settings: Settings) -> bool:
    return (
        settings.ais_bbox_min_lat <= latitude <= settings.ais_bbox_max_lat
        and settings.ais_bbox_min_lon <= longitude <= settings.ais_bbox_max_lon
    )


def zone_for_position(latitude: float, longitude: float, settings: Settings) -> Zone:
    if not inside_bbox(latitude, longitude, settings):
        return Zone.OUTSIDE
    if latitude < settings.corridor_min_lat or latitude > settings.corridor_max_lat:
        return Zone.OUTSIDE
    if longitude <= settings.west_gate_lon:
        return Zone.WEST
    if longitude >= settings.east_gate_lon:
        return Zone.EAST
    return Zone.MIDDLE
