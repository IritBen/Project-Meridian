import calendar
from datetime import date, datetime, timezone
from meridian.operational.markets import earliest_start_date

import psycopg

from meridian.trip_windows import (
    parse_month_window,
    parse_trip_job,
)

def is_month_complete(connection: psycopg.Connection, job: str, month: str) -> bool:
    with connection.cursor() as cursor:

        dataset, market = parse_trip_job(job)
        year_text, month_text = parse_month_window(month)
        year = int(year_text)
        month_number = int(month_text)
        _, num_days = calendar.monthrange(year, month_number)

        month_start = date(
            year,
            month_number,
            1,
        )

        if month_number == 12:
            next_month_start = date(
                year + 1,
                1,
                1,
            )
        else:
            next_month_start = date(
                year,
                month_number + 1,
                1,
            )

        cursor.execute(
            """
            SELECT max(updated_at)
            FROM operational.control_table
            WHERE job_window = %s 
              AND job_description = 'transform-to-silver' 
              AND market = %s
              AND job_name = %s
              AND status = 'success'
            """, 
            (month, market, dataset)
        )

        max_silver_updated_at = cursor.fetchone()[0]

        if max_silver_updated_at is None:
            return False

        cursor.execute(
            """
            SELECT COUNT(DISTINCT job_window)
            FROM operational.control_table
            WHERE job_window >= %s
              AND job_window < %s
              AND job_description = 'transform-to-gold'
              AND market = %s
              AND job_name = 'station-daily'
              AND status = 'success'
              AND updated_at > %s
            """,
            (
                month_start.isoformat(),
                next_month_start.isoformat(),
                market,
                max_silver_updated_at,
            ),
        )

        completed_gold_days = cursor.fetchone()[0]

        return completed_gold_days == num_days

def next_due(connection: psycopg.Connection, job: str) -> str | None:
    now = datetime.now(timezone.utc)
    first_of_this_month = now.replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    _, market = parse_trip_job(job)
    earliest_month = earliest_start_date(market)
    current_month = earliest_month
    while current_month < first_of_this_month:
        if is_month_complete(connection, job, current_month.strftime("%Y-%m"),):
            if current_month.month == 12:
                current_month = current_month.replace(
                    year=current_month.year + 1,
                    month=1,
                )
            else:
                current_month = current_month.replace(
                    month=current_month.month + 1,
                )
        else:
            return current_month.strftime("%Y-%m")
    return None 

def coverage(connection: psycopg.Connection, job: str,) -> dict:
    dataset, market = parse_trip_job(job)
    earliest_date = earliest_start_date(market)

    current_date = earliest_date

    watermark = None
    completed_months = 0
    incomplete_months = []
    gaps = []
    next_month = None

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT max(job_window)
            FROM operational.control_table
            WHERE 
              market = %s
              AND job_name = %s
              AND status = 'success'
              AND job_description = 'transform-to-silver'
              AND job_window >= %s
            """, 
            (
                market, dataset, earliest_date.strftime("%Y-%m")
            ),
        )

        max_month = cursor.fetchone()[0]

    if max_month is not None:
        max_date = datetime.strptime(
        max_month,
        "%Y-%m",
    ).replace(tzinfo=timezone.utc)
            
        while current_date <= max_date:

            month_value = current_date.strftime("%Y-%m")

            if is_month_complete(connection, job, month_value):
                completed_months += 1

                if next_month is None:
                    watermark = month_value

                gaps = incomplete_months.copy()

            else:
                incomplete_months.append(month_value)

                if next_month is None:
                    next_month = month_value

            if current_date.month == 12:
                current_date = current_date.replace(
                        year=current_date.year + 1,
                        month=1,
                        day=1
                    )
            else:
                current_date = current_date.replace(
                        year=current_date.year,
                        month=current_date.month + 1,
                        day=1
                    )

    if next_month is None:
        next_month = current_date.strftime("%Y-%m")

    return {
        "job": job,
        "earliest": earliest_date.strftime("%Y-%m"),
        "watermark": watermark,
        "complete": completed_months,
        "gaps": gaps,
        "next": next_month
        }
