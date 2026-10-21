from datetime import datetime, timezone


EARLIEST_WINDOWS = {
    "jc": "2021-01",
    "nyc": "2026-01",
}


def earliest_start_date(market: str) -> datetime:
    return datetime.strptime(
        EARLIEST_WINDOWS[market],
        "%Y-%m",
    ).replace(tzinfo=timezone.utc)
