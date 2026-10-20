from meridian.operational import markets
from airflow.sdk import chain, dag, task
from airflow.sdk import CronDataIntervalTimetable
from airflow.sdk.exceptions import AirflowSkipException
from meridian.operational.progress import (is_month_complete, next_due)
import psycopg

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
        run_type = getattr(dag_run.run_type,"value",dag_run.run_type,)
        params = params or {}
        requested_month = params.get("start")
        job = "trips:jc"
        if requested_month:
            return params["start"]
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

    selected_window = window()

jc_load_monthly()

        