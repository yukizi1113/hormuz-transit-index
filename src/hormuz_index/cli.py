from __future__ import annotations

import argparse
import asyncio
import time

import uvicorn

from hormuz_index.alerts import AlertService
from hormuz_index.api import create_app
from hormuz_index.collector import run_aisstream_collector, run_live_collector, run_marinetraffic_collector, run_replay_collector
from hormuz_index.config import Settings
from hormuz_index.indexer import Indexer
from hormuz_index.storage import Database


def _fallback_provider_without_marinetraffic(settings: Settings) -> str:
    if settings.aisstream_api_key:
        return "aisstream"
    return "replay"


def _resolve_provider(settings: Settings, explicit: str | None) -> str:
    provider = explicit or settings.ais_provider
    if provider in {"auto", "live"}:
        if settings.marinetraffic_api_key:
            return "marinetraffic"
        if settings.aisstream_api_key:
            return "aisstream"
        return "replay"
    if provider == "marinetraffic" and not settings.marinetraffic_api_key:
        return _fallback_provider_without_marinetraffic(settings)
    if provider == "aisstream" and not settings.aisstream_api_key:
        return "replay"
    return provider


def main() -> None:
    parser = argparse.ArgumentParser(description="Hormuz Transit Index CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("run-collector", help="Run AIS collector")
    collect_parser.add_argument(
        "--provider",
        choices=["auto", "marinetraffic", "aisstream", "live", "replay"],
        default=None,
    )
    collect_parser.add_argument("--replay-sleep-sec", type=float, default=0.0)

    subparsers.add_parser("run-indexer-once", help="Compute one index point")

    loop_parser = subparsers.add_parser("run-indexer-loop", help="Loop indexer and alerts")
    loop_parser.add_argument("--interval-sec", type=int, default=300)

    subparsers.add_parser("run-api", help="Run FastAPI service")
    args = parser.parse_args()

    settings = Settings.load()
    db = Database(settings)

    if args.command == "run-collector":
        provider = _resolve_provider(settings, args.provider)
        if provider in {"live", "marinetraffic"}:
            if provider == "marinetraffic":
                asyncio.run(run_marinetraffic_collector(settings, db))
            else:
                asyncio.run(run_live_collector(settings, db))
        elif provider == "aisstream":
            asyncio.run(run_aisstream_collector(settings, db))
        else:
            asyncio.run(run_replay_collector(settings, db, sleep_sec=args.replay_sleep_sec))
        return

    if args.command == "run-indexer-once":
        indexer = Indexer(settings, db)
        alerts = AlertService(settings, db)
        point = indexer.compute_latest(persist=True)
        alerts.evaluate(point)
        print(
            {
                "bucket_start": point.bucket_start.isoformat(),
                "count_1h": point.count_1h,
                "count_24h": point.count_24h,
                "index_24h": point.index_24h,
            }
        )
        return

    if args.command == "run-indexer-loop":
        indexer = Indexer(settings, db)
        alerts = AlertService(settings, db)
        while True:
            point = indexer.compute_latest(persist=True)
            alerts.evaluate(point)
            time.sleep(args.interval_sec)
        return

    if args.command == "run-api":
        uvicorn.run(create_app(settings), host=settings.app_host, port=settings.app_port)


if __name__ == "__main__":
    main()
