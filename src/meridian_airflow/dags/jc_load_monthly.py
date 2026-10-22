from meridian.operational import markets
from airflow.sdk import (
    CronDataIntervalTimetable,
    PokeReturnValue,
    chain,
    dag,
    task,
    get_current_context
)
from airflow.sdk.exceptions import AirflowSkipException, AirflowFailException
from meridian.operational.progress import (is_month_complete, next_due)
from meridian.operational.publication import (is_month_published,)
from meridian.trip_windows import parse_month_window
from meridian.bronze import ingest_to_bronze
from meridian.silver import transform_to_silver
from meridian.operational.gold import gold_loop
from meridian.operational.control import record_job_status
import psycopg
import subprocess
import os
from datetime import timedelta

@dag(
    dag_id="trips_jc_monthly",
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    start_date=markets.earliest_start_date("jc"),
    catchup=True,
    max_active_runs=4,
    default_args={"retries":2, "retry_delay": timedelta(seconds=90), "owner": "data-eng"},
    params={"start": ""},
    tags=["trips-jc", "monthly"],
    doc_md=__doc__,
)
def jc_load_monthly():

    @task
    def window(*, params, dag_run, data_interval_start=None) -> str:
        run_type = getattr(dag_run.run_type, "value", dag_run.run_type,)
        params = params or {}
        requested_month = params.get("start")
        job = "trips:jc"
        if run_type == "manual" and requested_month:
            parse_month_window(requested_month)
            if requested_month < markets.EARLIEST_WINDOWS["jc"]:
                raise AirflowFailException(
                    f"{requested_month} is before the earliest window for jc"
                )
            return requested_month

        dsn = os.environ["DATABASE_URL"]

        with psycopg.connect(dsn) as connection:
            if run_type in ("scheduled", "backfill"):
                requested_month = data_interval_start.strftime("%Y-%m")

                if is_month_complete(connection, job, requested_month):
                    raise AirflowSkipException(
                        f"{requested_month} is already complete"
                    )
                return requested_month

            else:
                requested_month = next_due(connection, job)

                if requested_month is None:
                    raise AirflowSkipException(
                        "there is no month to run for"
                    )
                return requested_month

    @task.sensor(
        poke_interval=60 * 60,
        timeout=14 * 24 * 60 * 60,
        mode="reschedule",
    )
    def wait_for_publication(
        selected_month: str,
    ) -> PokeReturnValue:
        is_published = is_month_published(
            "trips:jc",
            selected_month,
        )

        return PokeReturnValue(
            is_done=is_published,
        )

    selected_window = window()

    publication_ready = wait_for_publication(
        selected_window,
    )

    @task
    def ingest_bronze(selected_month: str) -> None:
        dsn = os.environ["DATABASE_URL"]

        try:
            ingest_to_bronze(
                job="trips:jc",
                window=selected_month,
            )

        except Exception:
            with psycopg.connect(dsn) as connection:
                record_job_status(
                    connection,
                    "trips",
                    "ingest-to-bronze",
                    "jc",
                    selected_month,
                    "failure",
                )

            raise

        else:
            with psycopg.connect(dsn) as connection:
                record_job_status(
                    connection,
                    "trips",
                    "ingest-to-bronze",
                    "jc",
                    selected_month,
                    "success",
                )

    bronze_task = ingest_bronze(selected_window)

    chain(
        publication_ready,
        bronze_task
    )

    @task
    def transform_silver(selected_month: str ) -> None:

        dsn = os.environ["DATABASE_URL"]

        project_root = "/opt/airflow/meridian" 

        try:
            subprocess.run(
                [
                    "just",
                    "run",
                    "transform-to-silver",
                    "trips:jc",
                    selected_month
                ],
                cwd=project_root,
                check=True
            )
        except Exception:
            with psycopg.connect(dsn) as connection:
                record_job_status(
                    connection,
                    "trips",
                    "transform-to-silver",
                    "jc",
                    selected_month,
                    "failure",
                )

            raise
        else:
            with psycopg.connect(dsn) as connection:
                record_job_status(
                    connection,
                    "trips",
                    "transform-to-silver",
                    "jc",
                    selected_month,
                    "success",
                )

    silver_task = transform_silver(selected_window)

    chain(
        bronze_task,
        silver_task
    )

    @task
    def transform_gold(selected_month: str) -> None:

        dsn = os.environ["DATABASE_URL"]

        with psycopg.connect(dsn) as connection:
            result = gold_loop(connection, selected_month)

        context = get_current_context()

        context["ti"].xcom_push(
            key="gold_summary",
            value=result,
        )

        if result["failed_days"]:
            raise RuntimeError(
                f"Gold failed for days: "
                f"{', '.join(result['failed_days'])}"
            )

        return result

    gold_task = transform_gold(selected_window)

    chain(
        silver_task,
        gold_task
    )

    
jc_load_monthly()

        