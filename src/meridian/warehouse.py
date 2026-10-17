import psycopg


def initialize_warehouse(
    connection: psycopg.Connection,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE SCHEMA IF NOT EXISTS silver"
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.trips (
                ride_sk
                    BIGINT GENERATED ALWAYS AS IDENTITY
                    PRIMARY KEY,
                source_market TEXT NOT NULL,
                source_window TEXT NOT NULL,
                source_ride_id TEXT,
                started_at TIMESTAMPTZ NOT NULL,
                ended_at TIMESTAMPTZ NOT NULL,
                start_station_id TEXT NOT NULL,
                start_station_name TEXT,
                start_lat DOUBLE PRECISION,
                start_lng DOUBLE PRECISION,
                end_station_id TEXT NOT NULL,
                end_station_name TEXT,
                end_lat DOUBLE PRECISION,
                end_lng DOUBLE PRECISION,
                member_casual TEXT,
                source_file TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS
                silver.trips_quarantine (
                    quarantine_sk
                        BIGINT GENERATED ALWAYS AS IDENTITY
                        PRIMARY KEY,
                    record_fingerprint
                        TEXT NOT NULL UNIQUE,
                    source_market TEXT NOT NULL,
                    source_window TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    source_row_number BIGINT NOT NULL,
                    reason TEXT NOT NULL,
                    raw_record JSONB NOT NULL,
                    quarantined_at
                        TIMESTAMPTZ NOT NULL
                        DEFAULT CURRENT_TIMESTAMP
                )
            """
        )

        cursor.execute(
            "CREATE SCHEMA IF NOT EXISTS gold"
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS
                gold.station_daily (
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
