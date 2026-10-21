import psycopg

def record_job_status(
    connection: psycopg.Connection,
    job_name: str,
    job_description: str,
    market: str,
    job_window: str,
    status: str
) -> None:
    if status not in ('success', 'failure'):
        raise ValueError(f"status {status} is not recognized value")
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO operational.control_table (
                job_description,
                market, job_window,
                job_name,
                status,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (
                job_description,
                market,
                job_window,
                job_name
            )
            DO UPDATE SET
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP
            """, 
            (
                job_description,
                market,
                job_window,
                job_name,
                status
            )
        )
        

