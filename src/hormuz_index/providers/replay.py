from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import AsyncIterator


async def replay_messages(path: Path, sleep_sec: float = 0.0) -> AsyncIterator[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)
            if sleep_sec > 0:
                await asyncio.sleep(sleep_sec)
