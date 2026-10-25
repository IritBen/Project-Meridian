from meridian.operational.control import record_job_status
from meridian.gold import transform_to_gold
from meridian.operational.locks import named_lock
import psycopg

from meridian.trip_windows import (
    parse_month_window,
    parse_trip_job,
    month_bounds
)

def gold_loop(connection: psycopg.Connection, selected_month: str, market: str) -> dict:

        parse_trip_job(f"trips:{market}")
        
        with connection.cursor() as cursor:

            year_text, month_text = parse_month_window(selected_month)
            month_start, next_month_start, num_days = month_bounds(
                selected_month
            )

            cursor.execute(
                """
                SELECT max(updated_at)
                FROM operational.control_table
                WHERE job_window = %s 
                  AND job_description = 'transform-to-silver' 
                  AND market = %s
                  AND job_name = 'trips'
                  AND status = 'success'
                """, 
                (selected_month, market)                
            )

            max_silver_updated_at = cursor.fetchone()[0]

            if max_silver_updated_at is None:
                raise ValueError(
                    f"No successful Silver load for {market} {selected_month}"
                )

            cursor.execute(
                """
                SELECT job_window
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

            successful_days = {
                row[0]
                for row in cursor.fetchall()
            }

        connection.commit()

        failed_days = []

        for day_number in range(1, num_days + 1):
            day = month_start.replace(day=day_number).isoformat()

            if day in successful_days:
                continue

            with named_lock(f"gold-day-{day}"):
                try:
                    transform_to_gold(
                        "station-daily",
                        day,
                        connection,
                    )

                    record_job_status(
                        connection,
                        "station-daily",
                        "transform-to-gold",
                        market,
                        day,
                        "success",
                    )

                    connection.commit()

                except Exception:
                    connection.rollback()

                    record_job_status(
                        connection,
                        "station-daily",
                        "transform-to-gold",
                        market,
                        day,
                        "failure",
                    )

                    connection.commit()
                    failed_days.append(day)

        return {
            "days": num_days,
            "failed_days": failed_days,
        }