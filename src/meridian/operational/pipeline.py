import json
import time
import os
import psycopg
from urllib.request import Request, urlopen
from uuid import uuid4

from meridian.operational.runs import (
    get_airflow_token,
    read_run_details,
)
from meridian.trip_windows import parse_month_window
from meridian.operational.progress import coverage
from meridian.operational.publication import is_month_published

def run_pipeline(market: str, month: str | None = None) -> dict:
    if market not in {"jc", "nyc"}:
        raise ValueError(f"Unsupported market: {market}")

    if not month:
        dsn = os.environ["DATABASE_URL"]

        with psycopg.connect(dsn) as connection:
            progress = coverage(connection, f"trips:{market}")

        month = progress["next"]

        if not is_month_published(f"trips:{market}", month):
            return {
                "market": market,
                "month": None,
                "work": "skipped",
            }

    parse_month_window(month)

    token = get_airflow_token()
    dag_id = f"trips_{market}_monthly"

    body = {
        "logical_date": None,
        "conf": {
            "start": month,
        },
    }

    request = Request(
        f"http://airflow-api-server:8080/api/v2/dags/"
        f"{dag_id}/dagRuns",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )

    with urlopen(request, timeout=30) as response:
        created_run = json.load(response)

    run_id = created_run["dag_run_id"]

    while True:
        details = read_run_details(
            market,
            run_id,
            token=token,
        )

        if details["state"] == "success":
            return {
                "market": market,
                "month": month,
                "run_id": run_id,
                "state": "success",
            }

        if details["state"] == "failed":
            raise RuntimeError(
                f"Pipeline failed: {dag_id}, run_id={run_id}"
            )

        time.sleep(3)

def create_backfill(market: str, start_month: str, end_month: str,) -> dict:
    if market not in {"jc", "nyc"}:
        raise ValueError(f"Unsupported market: {market}")

    parse_month_window(start_month)
    parse_month_window(end_month)

    if start_month > end_month:
        raise ValueError(
            "Start month must not be after end month"
        )

    token = get_airflow_token()

    body = {
        "dag_id": f"trips_{market}_monthly",
        "from_date": f"{start_month}-01T00:00:00Z",
        "to_date": f"{end_month}-01T00:00:00Z",
        "max_active_runs": 4,
        "reprocess_behavior": "failed",
        "dag_run_conf": {
            "request_id": uuid4().hex,
        },
    }

    request = Request(
        "http://airflow-api-server:8080/api/v2/backfills",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )

    with urlopen(request, timeout=30) as response:
        return json.load(response)