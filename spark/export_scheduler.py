import logging
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from spark.daily_registration_job import main as run_export

LOGGER = logging.getLogger(__name__)

EXPORT_HOUR_UTC = 0
MAX_SLEEP_SECONDS = 60.0


def next_export_time(now: datetime) -> datetime:
    candidate = now.replace(
        hour=EXPORT_HOUR_UTC,
        minute=0,
        second=0,
        microsecond=0,
    )
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def seconds_until_next_export(now: datetime) -> float:
    return (next_export_time(now) - now).total_seconds()


def wait_until(target: datetime) -> None:
    while True:
        remaining = (target - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            return
        time.sleep(min(remaining, MAX_SLEEP_SECONDS))


def run_forever(export: Callable[[], None]) -> None:
    while True:
        target = next_export_time(datetime.now(timezone.utc))
        LOGGER.info("Next Snowflake export at %s", target.isoformat())
        wait_until(target)
        try:
            export()
        except Exception:
            LOGGER.exception("Snowflake export failed; retrying tomorrow")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run_forever(run_export)


if __name__ == "__main__":
    main()
