import os
import psycopg
import csv
import hashlib
import json
import re
from pathlib import Path
from psycopg.types.json import Jsonb

def transform_to_silver(job: str, window: str) -> None:

    if job not in {"trips:jc", "trips:nyc"}:
        raise ValueError(
            f"Unsupported Silver job: {job}"
        )
    
    if re.fullmatch(
        r"\d{4}-(0[1-9]|1[0-2])",
        window,
    ) is None:
        raise ValueError(
            f"Silver window must be an ISO-8601 month: {window}"
        )

    dataset, market = job.split(":")
    year, month = window.split("-")

    bronze_dir = Path("/bronze") / dataset / market / year / month
    csv_files = sorted(bronze_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in Bronze directory: {bronze_dir}"
        )

    dsn = os.environ["DATABASE_URL"]

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver.trips(
                ride_sk                 BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                source_market           TEXT NOT NULL, 
                source_window           TEXT NOT NULL,
                source_ride_id          TEXT,
                started_at              TIMESTAMPTZ NOT NULL,       
                ended_at                TIMESTAMPTZ NOT NULL,
                start_station_id        TEXT NOT NULL,
                start_station_name      TEXT,
                start_lat               DOUBLE PRECISION,
                start_lng               DOUBLE PRECISION,
                end_station_id          TEXT NOT NULL,
                end_station_name        TEXT,
                end_lat                 DOUBLE PRECISION,
                end_lng                 DOUBLE PRECISION,
                member_casual           TEXT,
                source_file             TEXT NOT NULL
                )
                """)

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS silver.trips_quarantine (
                    quarantine_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    record_fingerprint TEXT NOT NULL UNIQUE,
                    source_market TEXT NOT NULL,
                    source_window TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    source_row_number BIGINT NOT NULL,
                    reason TEXT NOT NULL,
                    raw_record JSONB NOT NULL,
                    quarantined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            cursor.execute(
                """
                DELETE FROM silver.trips
                WHERE source_market = %s
                    AND source_window = %s
                """,
                (market, window),
            )

            insert_sql = """
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

            batch_size = 1_000

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
        
                    for row_number, row in enumerate(reader, start=2):
                        if is_modern_schema:
                            source_ride_id = row["ride_id"]
                            started_at = row["started_at"]
                            ended_at = row["ended_at"]
                            start_station_id = row["start_station_id"]
                            start_station_name = row["start_station_name"]
                            start_lat = row["start_lat"]
                            start_lng = row["start_lng"]
                            end_station_id = row["end_station_id"]
                            end_station_name = row["end_station_name"]
                            end_lat = row["end_lat"]
                            end_lng = row["end_lng"]
                            member_casual = row["member_casual"]
                        else:
                            source_ride_id = None
                            started_at = row["starttime"]
                            ended_at = row["stoptime"]
                            start_station_id = row["start station id"]
                            start_station_name = row["start station name"]
                            start_lat = row["start station latitude"]
                            start_lng = row["start station longitude"]
                            end_station_id = row["end station id"]
                            end_station_name = row["end station name"]
                            end_lat = row["end station latitude"]
                            end_lng = row["end station longitude"]
                            member_casual = row["usertype"]

                        missing_fields = [
                            field_name
                            for field_name, value in (
                                ("started_at", started_at),
                                ("ended_at", ended_at),
                                ("start_station_id", start_station_id),
                                ("end_station_id", end_station_id),
                            )
                            if not value
                        ]

                        if missing_fields:
                            reason = "never docked"

                            raw_record_json = json.dumps(
                                row,
                                sort_keys=True,
                                ensure_ascii=False,
                            )

                            fingerprint_source = (
                                f"{market}|{window}|{csv_file.name}|"
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
                                ON CONFLICT (record_fingerprint)
                                DO UPDATE SET reason = EXCLUDED.reason
                                """,
                                (
                                    record_fingerprint,
                                    market,
                                    window,
                                    csv_file.name,
                                    row_number,
                                    reason,
                                    Jsonb(row),
                                ),
                            )

                            continue
                        
                        values = (
                            market,
                            window,
                            source_ride_id,
                            started_at,
                            ended_at,
                            start_station_id,
                            start_station_name,
                            start_lat or None,
                            start_lng or None,
                            end_station_id,
                            end_station_name,
                            end_lat or None,
                            end_lng or None,
                            member_casual or None,
                            csv_file.name,
                        )
        
                        batch.append(values)

                        if len(batch) >= batch_size:
                            cursor.executemany(insert_sql, batch)
                            loaded_rows += len(batch)
                            batch.clear()

                            if loaded_rows % 100_000 == 0:
                                print(f"inserted {loaded_rows} rows")

                    if batch:
                        cursor.executemany(insert_sql, batch)
                        loaded_rows += len(batch)

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
            print(f"Loaded {row_count} rows into silver.trips")

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
            
            print(
                f"Quarantine contains {quarantine_count} rows "
                f"for {market} {window}"
            )


def inspect_silver(job: str, window: str) -> dict:
    dataset, market = job.split(":")

    if dataset != "trips" or market not in {"jc", "nyc"}:
        raise ValueError(f"Unsupported Silver job: {job}")

    dsn = os.environ["DATABASE_URL"]

    with psycopg.connect(dsn) as connection:
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
