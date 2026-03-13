from __future__ import annotations

from datetime import timedelta

from hormuz_index.config import Settings
from hormuz_index.geo import zone_for_position
from hormuz_index.models import PositionEvent, TransitEvent, VesselState, Zone
from hormuz_index.storage import Database
from hormuz_index.vessels import is_merchant_ship


class TransitDetector:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db

    def process(self, event: PositionEvent) -> TransitEvent | None:
        state = self.db.load_state(event.mmsi)
        effective_ship_type = event.ship_type if event.ship_type is not None else state.ship_type
        if self.settings.vessel_scope == "all_merchant" and not is_merchant_ship(effective_ship_type):
            return None
        zone = zone_for_position(event.latitude, event.longitude, self.settings)
        self._apply_observation(state, event, zone)
        transit = self._try_build_transit(state, event, zone)
        state.last_zone = zone
        state.last_seen_at = event.observed_at
        if event.vessel_name:
            state.vessel_name = event.vessel_name
        if effective_ship_type is not None:
            state.ship_type = effective_ship_type
        self.db.save_state(state)
        if transit and self.db.insert_transit(transit):
            return transit
        return None

    def _apply_observation(self, state: VesselState, event: PositionEvent, zone: Zone) -> None:
        max_age = timedelta(hours=self.settings.transit_max_hours)
        now = event.observed_at
        for attr in ("west_seen_at", "middle_seen_at", "east_seen_at"):
            value = getattr(state, attr)
            if value and now - value > max_age:
                setattr(state, attr, None)

        if zone is Zone.WEST:
            state.west_seen_at = event.observed_at
        elif zone is Zone.MIDDLE:
            state.middle_seen_at = event.observed_at
        elif zone is Zone.EAST:
            state.east_seen_at = event.observed_at

    def _try_build_transit(self, state: VesselState, event: PositionEvent, zone: Zone) -> TransitEvent | None:
        cooldown = timedelta(minutes=self.settings.transit_cooldown_min)
        if state.last_transit_at and event.observed_at - state.last_transit_at < cooldown:
            return None

        max_window = timedelta(hours=self.settings.transit_max_hours)
        if zone is Zone.EAST and state.west_seen_at:
            middle_ok = state.middle_seen_at is not None and state.middle_seen_at >= state.west_seen_at
            sparse_ok = state.last_zone in {Zone.WEST, Zone.MIDDLE}
            if event.observed_at - state.west_seen_at <= max_window and (middle_ok or sparse_ok):
                transit = TransitEvent(
                    mmsi=event.mmsi,
                    direction="eastbound",
                    started_at=state.west_seen_at,
                    completed_at=event.observed_at,
                    ship_type=event.ship_type or state.ship_type,
                    vessel_name=event.vessel_name or state.vessel_name,
                    provider=event.provider,
                )
                state.last_transit_at = event.observed_at
                state.west_seen_at = None
                state.middle_seen_at = None
                return transit

        if zone is Zone.WEST and state.east_seen_at:
            middle_ok = state.middle_seen_at is not None and state.middle_seen_at >= state.east_seen_at
            sparse_ok = state.last_zone in {Zone.EAST, Zone.MIDDLE}
            if event.observed_at - state.east_seen_at <= max_window and (middle_ok or sparse_ok):
                transit = TransitEvent(
                    mmsi=event.mmsi,
                    direction="westbound",
                    started_at=state.east_seen_at,
                    completed_at=event.observed_at,
                    ship_type=event.ship_type or state.ship_type,
                    vessel_name=event.vessel_name or state.vessel_name,
                    provider=event.provider,
                )
                state.last_transit_at = event.observed_at
                state.east_seen_at = None
                state.middle_seen_at = None
                return transit
        return None
