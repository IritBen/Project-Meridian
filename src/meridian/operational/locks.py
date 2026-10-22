from contextlib import contextmanager
from pathlib import Path
import fcntl


@contextmanager
def named_lock(name: str):
    lock_directory = Path(
        "/opt/airflow/logs/meridian-locks"
    )

    lock_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    lock_path = lock_directory / f"{name}.lock"

    with lock_path.open("a") as lock_file:
        fcntl.flock(
            lock_file.fileno(),
            fcntl.LOCK_EX,
        )

        try:
            yield

        finally:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_UN,
            )
