import re
import os
import calendar
from datetime import date
from pathlib import Path

SUPPORTED_TRIP_JOBS = {
    "trips:jc",
    "trips:nyc",
}

def configured_bronze_root() -> Path:
    return Path(
        os.environ.get(
            "BRONZE_ROOT",
            "/bronze",
        )
    )

def parse_trip_job(job: str) -> tuple[str, str]:
    if job not in SUPPORTED_TRIP_JOBS:
        raise ValueError(f"Unsupported trips job: {job}")

    dataset, market = job.split(":", maxsplit=1)
    return dataset, market

def parse_month_window(window:str) -> tuple[str,str]:
    if re.fullmatch(
        r"\d{4}-(0[1-9]|1[0-2])",
        window,    
    ) is None:
        raise ValueError(
            f"Window must be an ISO-8601 month: {window}"
        )

    year, month = window.split("-", maxsplit=1)

    return year, month

def month_bounds(window: str) -> tuple[date, date, int]:
    year_text, month_text = parse_month_window(window)
    year = int(year_text)
    month = int(month_text)

    month_start = date(year, month, 1)

    if month == 12:
        next_month_start = date(year + 1, 1, 1)
    else:
        next_month_start = date(year, month + 1, 1)

    _, num_days = calendar.monthrange(year, month)

    return month_start, next_month_start, num_days

def bronze_window_directory(job: str, window: str, bronze_root: Path | None = None,) -> Path:
    dataset, market = parse_trip_job(job)
    year, month = parse_month_window(window)

    if bronze_root is None:
        bronze_root = configured_bronze_root()

    return bronze_root / dataset / market / year / month
