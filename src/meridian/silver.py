import psycopg
import csv
import hashlib
import json
from pathlib import Path
from psycopg.types.json import Jsonb
from meridian.trip_windows import (
    bronze_window_directory,
    parse_month_window,
    parse_trip_job,
)

INSERT_TRIP_SQL = """
    INSERT INTO silver.trips (
        source_market,
        source_window,
        source_ride_id,
        started_at,
        ended_at,
        start_station_id,
        start_station_name,
        start_lat,
        start_lng,
        end_station_id,
        end_station_name,
        end_lat,
        end_lng,
        member_casual,
        source_file
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
"""

def normalize_trip_row(
    row: dict[str, str],
    is_modern_schema: bool,
) -> dict[str, str | None]:
    if is_modern_schema:
        return {
            "source_ride_id": row["ride_id"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "start_station_id": row["start_station_id"],
            "start_station_name": row["start_station_name"],
            "start_lat": row["start_lat"],
            "start_lng": row["start_lng"],
            "end_station_id": row["end_station_id"],
            "end_station_name": row["end_station_name"],
            "end_lat": row["end_lat"],
            "end_lng": row["end_lng"],
            "member_casual": row["member_casual"],
        }

    return {
        "source_ride_id": None,
        "started_at": row["starttime"],
        "ended_at": row["stoptime"],
        "start_station_id": row["start station id"],
        "start_station_name": row["start station name"],
        "start_lat": row["start station latitude"],
        "start_lng": row["start station longitude"],
        "end_station_id": row["end station id"],
        "end_station_name": row["end station name"],
        "end_lat": row["end station latitude"],
        "end_lng": row["end station longitude"],
        "member_casual": row["usertype"],
    }

def find_missing_required_fields(
    trip: dict[str, str | None],
) -> list[str]:
    required_fields = (
        "started_at",
        "ended_at",
        "start_station_id",
        "end_station_id",
    )

    return [
        field_name
        for field_name in required_fields
        if not trip[field_name]
    ]

def quarantine_trip_row(
    cursor: psycopg.Cursor,
    market: str,
    window: str,
    source_file: str,
    row_number: int,
    row: dict[str, str],
    reason: str,
) -> None:
    raw_record_json = json.dumps(
        row,
        sort_keys=True,
        ensure_ascii=False,
    )

    fingerprint_source = (
        f"{market}|{window}|{source_file}|"
        f"{row_number}|{raw_record_json}"
    )

    record_fingerprint = hashlib.sha256(
        fingerprint_source.encode("utf-8")
    ).hexdigest()

    cursor.execute(
        """
        INSERT INTO silver.trips_quarantine (
            record_fingerprint,
            source_market,
            source_window,
            source_file,
            source_row_number,
            reason,
            raw_record
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            record_fingerprint,
            market,
            window,
            source_file,
            row_number,
            reason,
            Jsonb(row),
        ),
    )

def clear_silver_window(
    cursor: psycopg.Cursor,
    market: str,
    window: str,
) -> None:
    cursor.execute(
        """
        DELETE FROM silver.trips
        WHERE source_market = %s
          AND source_window = %s
        """,
        (market, window),
    )

    cursor.execute(
        """
        DELETE FROM silver.trips_quarantine
        WHERE source_market = %s
          AND source_window = %s
        """,
        (market, window),
    )


def load_trip_files(
    cursor: psycopg.Cursor,
    csv_files: list[Path],
    market: str,
    window: str,
    batch_size: int = 1_000,
) -> int:
    loaded_rows = 0

    for csv_file in csv_files:
        with csv_file.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:
            reader = csv.DictReader(file)

            fieldnames = reader.fieldnames or []
            is_modern_schema = "ride_id" in fieldnames

            batch = []

            for row_number, row in enumerate(
                reader,
                start=2,
            ):
                trip = normalize_trip_row(
                    row,
                    is_modern_schema,
                )

                missing_fields = (
                    find_missing_required_fields(trip)
                )

                if missing_fields:
                    reason = (
                        "missing required fields: "
                        f"{', '.join(missing_fields)}"
                    )

                    quarantine_trip_row(
                        cursor=cursor,
                        market=market,
                        window=window,
                        source_file=csv_file.name,
                        row_number=row_number,
                        row=row,
                        reason=reason,
                    )

                    continue

                values = (
                    market,
                    window,
                    trip["source_ride_id"],
                    trip["started_at"],
                    trip["ended_at"],
                    trip["start_station_id"],
                    trip["start_station_name"],
                    trip["start_lat"] or None,
                    trip["start_lng"] or None,
                    trip["end_station_id"],
                    trip["end_station_name"],
                    trip["end_lat"] or None,
                    trip["end_lng"] or None,
                    trip["member_casual"] or None,
                    csv_file.name,
                )

                batch.append(values)

                if len(batch) >= batch_size:
                    cursor.executemany(
                        INSERT_TRIP_SQL,
                        batch,
                    )

                    loaded_rows += len(batch)
                    batch.clear()

                    if loaded_rows % 100_000 == 0:
                        print(
                            f"inserted {loaded_rows} rows"
                        )

            if batch:
                cursor.executemany(
                    INSERT_TRIP_SQL,
                    batch,
                )
                loaded_rows += len(batch)

    return 

def count_silver_window(
    cursor: psycopg.Cursor,
    market: str,
    window: str,
) -> tuple[int, int]:
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM silver.trips
        WHERE source_market = %s
          AND source_window = %s
        """,
        (market, window),
    )

    trip_count = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM silver.trips_quarantine
        WHERE source_market = %s
          AND source_window = %s
        """,
        (market, window),
    )

    quarantine_count = cursor.fetchone()[0]

    return trip_count, quarantine_count

def transform_to_silver(job: str, window: str, connection: psycopg.Connection,) -> None:

    _, market = parse_trip_job(job)
    bronze_dir = bronze_window_directory(job, window)
    csv_files = sorted(bronze_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in Bronze directory: {bronze_dir}"
        )

    with connection.cursor() as cursor:
        clear_silver_window(
            cursor,
            market,
            window,
        )
        load_trip_files(
            cursor=cursor,
            csv_files=csv_files,
            market=market,
            window=window,
        )
        row_count, quarantine_count = (
            count_silver_window(
                cursor,
                market,
                window,
            )
        )
        
        print(f"Loaded {row_count} rows into silver.trips")
        
        print(
            f"Quarantine contains {quarantine_count} rows "
            f"for {market} {window}"
        )


def inspect_silver(job: str, window: str, connection: psycopg.Connection,) -> dict:
    _, market = parse_trip_job(job)
    parse_month_window(window)

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT to_regclass('silver.trips')"
        )
        trips_table_exists = cursor.fetchone()[0] is not None
        if trips_table_exists:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM silver.trips
                WHERE source_market = %s
                  AND source_window = %s
                """,
                (market, window),
            )
            row_count = cursor.fetchone()[0]
        else:
            row_count = 0
        cursor.execute(
            "SELECT to_regclass('silver.trips_quarantine')"
        )
        quarantine_table_exists = (
            cursor.fetchone()[0] is not None
        )
        if quarantine_table_exists:
            cursor.execute(
                """
                SELECT
                    reason,
                    COUNT(*)
                FROM silver.trips_quarantine
                WHERE source_market = %s
                  AND source_window = %s
                GROUP BY reason
                ORDER BY reason
                """,
                (market, window),
            )
            reasons = {
                reason: count
                for reason, count in cursor.fetchall()
            }
        else:
            reasons = {}

    reject_count = sum(reasons.values())

    return {
        "layer": "silver",
        "job": job,
        "window": window,
        "rows": row_count,
        "rejects": reject_count,
        "reasons": reasons,
    }
