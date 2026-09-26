import os
from datetime import date

import psycopg


def transform_to_gold(job: str, window: str) -> None:
    if job != "station-daily":
        raise ValueError(
            f"Unsupported Gold job: {job}. Expected station-daily"
        )

    try:
        service_day = date.fromisoformat(window)
    except ValueError as error:
        raise ValueError(
            f"Gold window must be an ISO-8601 day: {window}"
        ) from error

    if service_day.isoformat() != window:
        raise ValueError(
            f"Gold window must be an ISO-8601 day: {window}"
        )

    dsn = os.environ["DATABASE_URL"]

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS gold.station_daily (
                    source_market TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    service_day DATE NOT NULL,
                    departures BIGINT NOT NULL,
                    arrivals BIGINT NOT NULL,
                    PRIMARY KEY (
                        source_market,
                        station_id,
                        service_day
                    )
                )
                """
            )

            cursor.execute(
                """
                DELETE FROM gold.station_daily
                WHERE service_day = %s
                """,
                (service_day,),
            )

            cursor.execute(
                """
                INSERT INTO gold.station_daily (
                    source_market,
                    station_id,
                    service_day,
                    departures,
                    arrivals
                )
                SELECT
                    source_market,
                    station_id,
                    %s,
                    SUM(departures),
                    SUM(arrivals)
                FROM (
                    SELECT
                        source_market,
                        start_station_id AS station_id,
                        1 AS departures,
                        0 AS arrivals
                    FROM silver.trips
                    WHERE started_at::date = %s

                    UNION ALL

                    SELECT
                        source_market,
                        end_station_id AS station_id,
                        0 AS departures,
                        1 AS arrivals
                    FROM silver.trips
                    WHERE ended_at::date = %s
                ) AS station_events
                GROUP BY
                    source_market,
                    station_id
                """,
                (
                    service_day,
                    service_day,
                    service_day,
                ),
            )

            print(
                f"Loaded {cursor.rowcount} rows "
                f"into gold.station_daily for {service_day}"
            )

def report_daily_station_trips(
    market: str,
    station: str,
    day: str,
) -> dict:
    if market not in {"jc", "nyc"}:
        raise ValueError(
            f"Unsupported market: {market}. Expected jc or nyc"
        )

    try:
        report_day = date.fromisoformat(day)
    except ValueError as error:
        raise ValueError(
            f"Report day must be an ISO-8601 day: {day}"
        ) from error

    if report_day.isoformat() != day:
        raise ValueError(
            f"Report day must be an ISO-8601 day: {day}"
        )

    dsn = os.environ["DATABASE_URL"]

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    departures,
                    arrivals
                FROM gold.station_daily
                WHERE source_market = %s
                  AND station_id = %s
                  AND service_day = %s
                """,
                (
                    market,
                    station,
                    report_day,
                ),
            )

            row = cursor.fetchone()

    if row is None:
        departures = 0
        arrivals = 0
    else:
        departures, arrivals = row

    return {
        "market": market,
        "station": station,
        "day": day,
        "departures": departures,
        "arrivals": arrivals,
    }