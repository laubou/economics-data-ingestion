"""
Scheduler service — orchestrates the daily batch ingestion trigger.

Runs continuously and fires the downloader → producer sequence once per day
at 09:00 UTC (configurable via PIPELINE_INGESTION_CRON).

In AWS this service is replaced by an EventBridge Scheduler rule targeting
two ECS Fargate tasks (downloader then producer) run as a Step Functions
state machine or a simple rule with a dependency on exit code.

Locally it is a long-running Docker container that wakes up, checks whether
it is time to run, fires the pipeline, then sleeps until the next window.

Architecture:
  scheduler ──► downloader (fetch + archive + land if new data)
                    │
                    ▼ (only if state == "extracted")
              producer (land CSV → Kafka topic)
                    │
                    ▼ (always running, not triggered by scheduler)
         consumer_bronze  /  transformer_silver
"""

from __future__ import annotations

import logging
import subprocess
import sys
import time
from datetime import datetime, timezone

from economics_pipeline.config import get_settings
from economics_pipeline.ingestion.state import IngestionStateManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# How often the scheduler wakes up to check the clock (seconds).
# Fine-grained enough not to miss the window; coarse enough to not burn CPU.
_POLL_INTERVAL_SECONDS = 60


def _should_fire(cron: str, now: datetime) -> bool:
    """Return True if the current minute matches the cron expression.

    Supports standard 5-field cron: minute hour dom month dow.
    For this pipeline the default is "0 9 * * *" (daily at 09:00 UTC).
    Only minute and hour fields are evaluated here — sufficient for a
    daily batch trigger. A proper cron library (croniter) is used when
    available; otherwise falls back to simple hour+minute matching.
    """
    try:
        from croniter import croniter  # type: ignore[import-untyped]
        base = now.replace(second=0, microsecond=0)
        return croniter.match(cron, base)
    except ImportError:
        # Fallback: parse hour and minute from the cron expression
        parts = cron.split()
        if len(parts) < 2:
            return False
        minute_field, hour_field = parts[0], parts[1]
        minute_ok = minute_field == "*" or int(minute_field) == now.minute
        hour_ok = hour_field == "*" or int(hour_field) == now.hour
        return minute_ok and hour_ok


def _run_service(script_path: str, name: str) -> bool:
    """Run a service script as a subprocess. Returns True on success."""
    logger.info("Starting %s", name)
    result = subprocess.run([sys.executable, script_path])
    if result.returncode != 0:
        logger.error("%s exited with code %d", name, result.returncode)
        return False
    logger.info("%s completed successfully", name)
    return True


def run_ingestion_cycle(state_mgr: IngestionStateManager) -> None:
    """One complete batch ingestion cycle: check for new data → land → produce."""
    logger.info("=== Ingestion cycle starting at %s ===", datetime.now(timezone.utc).isoformat())

    # Step 1 — Downloader: HEAD check → download → archive → land
    ok = _run_service("services/downloader/app.py", "downloader")
    if not ok:
        logger.error("Downloader failed — aborting cycle")
        return

    # Step 2 — Only produce if downloader actually landed new data
    state = state_mgr.load()
    if state is None or state.status != "extracted":
        logger.info("No new data was landed — skipping producer")
        return

    # Step 3 — Producer: land CSV → Kafka topic
    ok = _run_service("services/producer/app.py", "producer")
    if not ok:
        logger.error("Producer failed — state remains 'extracted' for next retry")
        return

    # Step 4 — Mark cycle as fully complete
    state_mgr.mark_produced()
    logger.info("=== Ingestion cycle complete ===")


def main() -> None:
    settings = get_settings()
    state_mgr = IngestionStateManager(settings.state_path)
    cron = settings.ingestion_cron
    logger.info("Scheduler started — cron='%s' (UTC)", cron)

    fired_this_minute: str | None = None

    while True:
        now = datetime.now(timezone.utc)
        minute_key = now.strftime("%Y-%m-%dT%H:%M")

        if _should_fire(cron, now) and minute_key != fired_this_minute:
            fired_this_minute = minute_key
            try:
                run_ingestion_cycle(state_mgr)
            except Exception:
                logger.exception("Ingestion cycle raised an unexpected error")

        time.sleep(_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
