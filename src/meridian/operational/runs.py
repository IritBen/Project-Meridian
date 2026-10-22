import json
import subprocess
import calendar
import time
from urllib.request import Request, urlopen
from urllib.parse import quote
from urllib.error import HTTPError, URLError

def get_airflow_token() -> str:
    credentials = {
        "username": "airflow",
        "password": "airflow",
    }

    request = Request(
        "http://airflow-api-server:8080/auth/token",
        data=json.dumps(credentials).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urlopen(request, timeout=30) as response:
        result = json.load(response)

    return result["access_token"]

def read_run_details(
    market: str,
    run_id: str,
    token: str | None = None,
) -> dict:
    if token is None:
        token = get_airflow_token()

    dag_id = f"trips_{market}_monthly"
    encoded_run_id = quote(run_id, safe="")

    return read_airflow_data(
        f"dags/{dag_id}/dagRuns/{encoded_run_id}",
        token,
    )

def read_task_details(market: str, run_id: str, token: str | None = None,) -> list:
    if token is None:
        token = get_airflow_token()

    dag_id = f"trips_{market}_monthly"
    encoded_run_id = quote(run_id, safe="")

    request = Request(
        f"http://airflow-api-server:8080/api/v2/dags/"
        f"{dag_id}/dagRuns/{encoded_run_id}/taskInstances",
        headers={
            "Authorization": f"Bearer {token}",
        },
    )

    with urlopen(request, timeout=30) as response:
        result = json.load(response)

    return result["task_instances"]

def read_run_tasks(dag_id: str, run_id: str) -> list:
    result = subprocess.run(
        [
            "airflow",
            "tasks",
            "states-for-dag-run",
            dag_id,
            run_id,
            "--output",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    return json.loads(result.stdout)

def read_gold_summary(market: str, run_id: str) -> dict | None:
    token = get_airflow_token()

    dag_id = f"trips_{market}_monthly"
    encoded_run_id = quote(run_id, safe="")

    request = Request(
        f"http://airflow-api-server:8080/api/v2/dags/"
        f"{dag_id}/dagRuns/{encoded_run_id}/"
        "taskInstances/transform_gold/xcomEntries/gold_summary",
        headers={
            "Authorization": f"Bearer {token}",
        },
    )

    try:
        with urlopen(request, timeout=30) as response:
            result = json.load(response)

        return result["value"]

    except HTTPError as error:
        if error.code == 404:
            return None

        raise

def inspect_runs(market: str) -> dict:
    if market not in {"jc", "nyc"}:
        raise ValueError(f"Unsupported market: {market}")

    dag_id = f"trips_{market}_monthly"

    result = subprocess.run(
        [
            "airflow",
            "dags",
            "list-runs",
            dag_id,
            "--output",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    runs = json.loads(result.stdout)

    formatted_runs = []

    runs = sorted(
        runs,
        key=lambda run: run["run_after"],
        reverse=True,
    )

    for run in runs:
        run_id = run["run_id"]

        details = read_run_details(market, run_id)
        task_details = read_task_details(market, run_id)

        run_type = details["run_type"]

        if run_type == "manual":
            month = (details["conf"] or {}).get("start")
        else:
            interval_start = details["data_interval_start"]
            month = interval_start[:7] if interval_start else None

        task_names = {
            "ingest_bronze":
                f"ingest-to-bronze trips:{market} {month}",
            "transform_silver":
                f"transform-to-silver trips:{market} {month}",
            "transform_gold":
                "transform-to-gold station-daily",
        }

        tasks = {}

        for task in task_details:
            task_id = task["task_id"]

            if task_id not in task_names:
                continue

            task_name = task_names[task_id]

            tasks[task_name] = {
                "state": task["state"],
                "tries": task["try_number"],
            }

            if task_id == "transform_gold" and month is not None:
                year_number, month_number = map(
                    int,
                    month.split("-"),
                )

                _, num_days = calendar.monthrange(
                    year_number,
                    month_number,
                )

                tasks[task_name]["days"] = num_days

        gold_summary = read_gold_summary(market, run_id)

        gold_task_name = "transform-to-gold station-daily"

        if gold_summary is not None:
            tasks[gold_task_name]["days"] = gold_summary["days"]
            tasks[gold_task_name]["failed_days"] = (
                gold_summary["failed_days"]
            )
        else:
            tasks[gold_task_name]["failed_days"] = None

        formatted_runs.append({
            "month": month,
            "type": run_type,
            "state": details["state"],
            "tasks": tasks,
        })

    return {
        "market": market,
        "runs": formatted_runs,
    }

def read_airflow_data(path: str, token: str) -> dict:
    request = Request(
        f"http://airflow-api-server:8080/api/v2/{path}",
        headers={
            "Authorization": f"Bearer {token}",
        },
        method="GET",
    )

    for attempt in range(5):
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)

        except HTTPError as error:
            if error.code not in {500, 502, 503, 504}:
                raise

            if attempt == 4:
                raise

        except (URLError, TimeoutError, ConnectionError):
            if attempt == 4:
                raise

        time.sleep(5)
