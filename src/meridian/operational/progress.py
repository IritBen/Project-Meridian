import calendar
from datetime import date

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





