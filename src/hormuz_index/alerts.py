from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from hormuz_index.config import Settings
from hormuz_index.models import IndexPoint
from hormuz_index.storage import Database


class DiscordNotifier:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def send(self, content: str, embeds: list[dict[str, Any]] | None = None) -> bool:
        if not self.settings.discord_alert_enabled or not self.settings.discord_webhook_url:
            return False
        payload: dict[str, Any] = {"content": content, "allowed_mentions": {"parse": []}}
        if embeds:
            payload["embeds"] = embeds
        url = self.settings.discord_webhook_url
        wait_url = url if "wait=true" in url else f"{url}?wait=true"
        delay = 0.5
        for attempt in range(3):
            try:
                resp = requests.post(wait_url, json=payload, timeout=10)
                if 200 <= resp.status_code < 300:
                    return True
                if resp.status_code in {429, 500, 502, 503, 504} and attempt < 2:
                    time.sleep(delay)
                    delay *= 2
                    continue
                return False
            except requests.RequestException:
                if attempt == 2:
                    return False
                time.sleep(delay)
                delay *= 2
        return False


class AlertService:
    def __init__(self, settings: Settings, db: Database, notifier: DiscordNotifier | None = None) -> None:
        self.settings = settings
        self.db = db
        self.notifier = notifier or DiscordNotifier(settings)

    def _can_send(self, alert_key: str, now: datetime) -> bool:
        existing = self.db.read_alert_history(alert_key)
        if existing is None:
            return True
        last_sent, _ = existing
        cooldown = timedelta(minutes=self.settings.discord_alert_cooldown_min)
        return now - last_sent >= cooldown

    def _record_send(self, alert_key: str, now: datetime, payload: dict[str, Any] | None = None) -> None:
        self.db.upsert_alert_history(alert_key, now, payload)

    def evaluate(self, point: IndexPoint, now: datetime | None = None) -> list[str]:
        now = now or datetime.now(timezone.utc)
        triggered: list[str] = []

        latest_raw = self.db.latest_raw_event_time()
        if latest_raw is None or now - latest_raw >= timedelta(minutes=self.settings.alert_input_gap_minutes):
            if self._can_send("input_gap", now):
                content = f"[Hormuz Transit Index] AIS input gap detected at {now.isoformat()}"
                if self.notifier.send(content):
                    self._record_send("input_gap", now, {"kind": "input_gap"})
                triggered.append("input_gap")

        if point.index_24h is not None and point.index_24h < self.settings.alert_index_threshold:
            if self._can_send("index_drop", now):
                content = f"[Hormuz Transit Index] 24h index dropped to {point.index_24h:.2f}"
                if self.notifier.send(content):
                    self._record_send("index_drop", now, {"kind": "index_drop", "value": point.index_24h})
                triggered.append("index_drop")

        if point.hourly_baseline_median and point.hourly_baseline_median > 0:
            ratio = point.count_1h / point.hourly_baseline_median
            if ratio < self.settings.alert_hourly_drop_ratio and self._can_send("hourly_drop", now):
                content = (
                    "[Hormuz Transit Index] 1h transit count drop "
                    f"{point.count_1h} vs baseline {point.hourly_baseline_median:.2f}"
                )
                if self.notifier.send(content):
                    self._record_send("hourly_drop", now, {"kind": "hourly_drop", "ratio": ratio})
                triggered.append("hourly_drop")
        return triggered

    def send_test(self) -> bool:
        return self.notifier.send("[Hormuz Transit Index] test alert")
