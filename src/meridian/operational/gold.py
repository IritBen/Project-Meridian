from meridian.operational.control import record_job_status
from meridian.gold import transform_to_gold
from datetime import date
import calendar
import psycopg

from meridian.trip_windows import (
    parse_month_window,
    parse_trip_job,
)

def gold_loop(connection: psycopg.Connection, selected_month: str) -> dict:
        
        with connection.cursor() as cursor:

            year_text, month_text = parse_month_window(selected_month)
            year_number = int(year_text)
            month_number = int(month_text)
            _, num_days = calendar.monthrange(year_number, month_number)

            month_start = date(
                year_number,
                month_number,
                1
            )

            if month_number == 12:
                next_month_start = date(
                    year_number + 1,
                    1,
                    1
                )
            else:
                next_month_start = date(
                    year_number,
                    month_number + 1,
                    1
                )

            cursor.execute(
                """
                SELECT max(updated_at)
                FROM operational.control_table
                WHERE job_window = %s 
                  AND job_description = 'transform-to-silver' 
                  AND market = 'jc'
                  AND job_name = 'trips'
                  AND status = 'success'
                """, 
                (selected_month,)                
            )

            max_silver_updated_at = cursor.fetchone()[0]

            if max_silver_updated_at is None:
                raise ValueError(
                    f"No successful Silver load for jc {selected_month}"
                )

            cursor.execute(
                """
                SELECT job_window
                FROM operational.control_table
                WHERE job_window >= %s
                  AND job_window < %s
                  AND job_description = 'transform-to-gold'
                  AND market = 'jc'
                  AND job_name = 'station-daily'
                  AND status = 'success'
                  AND updated_at > %s
                """,
                (
                    month_start.isoformat(),
                    next_month_start.isoformat(),
                    max_silver_updated_at,
                ),
            )

            successful_days = {
                row[0]
                for row in cursor.fetchall()
            }

        connection.commit()

        failed_days = []

        for day_number in range(1, num_days + 1):
            day = date(
                year_number,
                month_number,
                day_number,
            ).isoformat()

            if day in successful_days:
                continue
            else:
                try:
                    transform_to_gold("station-daily", day, connection)

                    record_job_status(
                        connection,
                        "station-daily",
                        "transform-to-gold",
                        "jc",
                        day,
                        "success"
                    )

                    connection.commit()

                except Exception:

                    connection.rollback()

                    record_job_status(
                        connection,
                        "station-daily",
                        "transform-to-gold",
                        "jc",
                        day,
                        "failure"
                    )

                    connection.commit()
                    failed_days.append(day)

        return {
            "days": num_days,
            "failed_days": failed_days,
        }