from meridian.operational import markets
from airflow.sdk import (
    CronDataIntervalTimetable,
    PokeReturnValue,
    chain,
    dag,
    task,
)
from airflow.sdk.exceptions import AirflowSkipException, AirflowFailException
from meridian.operational.progress import (is_month_complete, next_due)
from meridian.operational.publication import (is_month_published,)
from meridian.trip_windows import parse_month_window
from meridian.bronze import ingest_to_bronze
from meridian.operational.control import record_job_status
import psycopg
import os

@dag(
    dag_id="trips_jc_monthly",
    schedule=CronDataIntervalTimetable("0 0 1 * *",timezone="UTC",),
    start_date=markets.earliest_start_date("jc"),
    catchup=True,
    max_active_runs=4,
    default_args={"retries":2, "owner": "data-eng"},
    params={"start": ""},
    tags=["trips-jc", "monthly"],
    doc_md=__doc__,
)
def jc_load_monthly():

    @task
    def window(*, params, data_interval_start, dag_run) -> str:
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
        try:
            ingest_to_bronze(job="trips:jc", window=selected_month)
        except Exception:

            dsn = os.environ["DATABASE_URL"]

            with psycopg.connect(dsn) as connection: 
                record_job_status(
                    connection,
                    "trips",
                    "ingest_to_bronze",
                    "jc",
                    selected_month,
                    "failure"
                )
            raise

        record_job_status(
            connection,
            "trips",
            "ingest_to_bronze",
            "jc",
            selected_month,
            "success"
        )

    bronze_task = ingest_bronze(selected_window)

    chain(
        publication_ready,
        bronze_task
    )
                

jc_load_monthly()

        