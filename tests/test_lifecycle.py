import os

TEST_ENV = os.environ.copy()

TEST_ENV.update(
    {
        "COMPOSE_PROJECT_NAME": "meridian-pipeline-test",
        "POSTGRES_PORT": "5433",
        "AIRFLOW_PORT": "8082",
    }
)

# Test 1 - postgres up
# Invoke just up
    # for 10 times try :
        # connect to postgres (timeout 2s)
        # success -> ok
        # failure -> sleep 2

DSN = (
    "postgresql://meridian:meridian"
    "@localhost:5433/meridian"
)


import subprocess
import pytest
import psycopg
import time
import docker
from collections.abc import Callable
from functools import partial
import json

def just(command: str, *args: str) -> str:
    result = subprocess.run(["just", command, *args], check=True, stdout=subprocess.PIPE, text=True, env=TEST_ENV,)
    return result.stdout

@pytest.fixture(autouse=True)
def clean_stack():
    subprocess.run(
        ["just", "down"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=TEST_ENV,
    )

    yield

    subprocess.run(
        ["just", "down"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=TEST_ENV,
    )

def is_postgres_up(dsn: str) -> bool:
    try:
        with psycopg.connect(dsn, connect_timeout=2):
            return True
    except psycopg.Error:
        return False

def is_postgres_down() -> bool:
    return not is_postgres_up(DSN)

def wait_for(condition_function: Callable[[], bool], iterations: int, time_to_sleep: int) -> bool:
    is_condition_satisfied = False
    for _ in range(iterations):
        is_condition_satisfied = condition_function()
        if is_condition_satisfied:
            break
        else:
            time.sleep(time_to_sleep)
    return is_condition_satisfied   

def test_just_up_postgres():
    assert not is_postgres_up(DSN), "postgres was up before even running just up, remove it first"
    just("up")
    postgres_is_up = wait_for(partial(is_postgres_up, DSN), 10, 2)
    assert postgres_is_up
            

# Test 2 - volumn exists 
# Invoke just up
    # for 10 times try :
        # check if volume exists
        # success -> ok
        # failure -> sleep 2


VOLUME_NAME = ("meridian-pipeline-test_bronze-data")

def is_volume_exists(volume_name: str) -> bool:
    client = docker.from_env()
    try:
        client.volumes.get(volume_name)
        return True
    except docker.errors.NotFound:
        return False

def test_just_up_creates_bronze_volume():
    just("up")

    assert is_volume_exists(
        VOLUME_NAME
    ), "just up did not create the Bronze volume"


# Test 3 - postgres down
# Invoke just down
    # for 10 times try :
        # connect to postgres (timeout 2s)
        # success -> sleep 2
        # failure -> ok


def test_just_down_postgres():
    just("up")
    postgres_is_up = wait_for(partial(is_postgres_up, DSN), 10, 2)
    assert postgres_is_up, "postgres didn't come up in time"
    just("down")
    postgres_is_down = wait_for(is_postgres_down, 10, 2)
    assert postgres_is_down, "potgress didn't go down althoug it should have"


# Test 4 - volumn does not exist
# Invoke just down
    # for 10 times try :
        # check if volume exists
        # success -> sleep 2
        # failure -> ok

def test_just_down_removes_volume():
    just("up")

    assert is_volume_exists(VOLUME_NAME)

    just("down")

    assert not is_volume_exists(VOLUME_NAME)

# Test 5 - bronze
# Invoke just up
    # just run to bronze for JC 202102
    # check inspect output equal to the json I write

def test_just_run_ingest_to_bronze_jc_202102():
    just("up")
    just("run", "ingest-to-bronze", "trips:jc", "2021-02")
    result = json.loads(just("inspect", "bronze", "trips:jc", "2021-02"))
    expected_result = {"layer": "bronze","job": "trips:jc","window": "2021-02","objects": 1,"rows": 4881}
    assert result == expected_result

# Test 6 - NYC 2018-04

def test_just_run_ingest_to_bronze_nyc_201804():
    just("up")

    just(
        "run",
        "ingest-to-bronze",
        "trips:nyc",
        "2018-04",
    )

    result = json.loads(
        just(
            "inspect",
            "bronze",
            "trips:nyc",
            "2018-04",
        )
    )

    expected_result = {
        "layer": "bronze",
        "job": "trips:nyc",
        "window": "2018-04",
        "objects": 2,
        "rows": 1_307_543,
    }

    assert result == expected_result

# Test 7 - quarantine

@pytest.mark.parametrize(
    ("window", "expected_result"),
    [
        (
            "2026-06",
            {
                "layer": "silver",
                "job": "trips:jc",
                "window": "2026-06",
                "rows": 109_510,
                "rejects": 387,
                "reasons": {
                    "missing required fields: end_station_id": 387,
                },
            },
        ),
        (
            "2019-06",
            {
                "layer": "silver",
                "job": "trips:jc",
                "window": "2019-06",
                "rows": 39_430,
                "rejects": 0,
                "reasons": {},
            },
        ),
    ],
)
def test_silver_quarantine_counts(
    window: str,
    expected_result: dict,
):
    just("up")

    just(
        "run",
        "ingest-to-bronze",
        "trips:jc",
        window,
    )

    just(
        "run",
        "transform-to-silver",
        "trips:jc",
        window,
    )

    result = json.loads(
        just(
            "inspect",
            "silver",
            "trips:jc",
            window,
        )
    )

    assert result == expected_result

# Test 8 - Gold answer 216 / 209

def test_daily_station_trips_gold_answer():
    just("up")

    just(
        "run",
        "ingest-to-bronze",
        "trips:jc",
        "2026-06",
    )

    just(
        "run",
        "transform-to-silver",
        "trips:jc",
        "2026-06",
    )

    just(
        "run",
        "transform-to-gold",
        "station-daily",
        "2026-06-02",
    )

    result = json.loads(
        just(
            "report",
            "daily-station-trips",
            "jc",
            "JC115",
            "2026-06-02",
        )
    )

    expected_result = {
        "market": "jc",
        "station": "JC115",
        "day": "2026-06-02",
        "departures": 216,
        "arrivals": 209,
    }

    assert result == expected_result       
    
# Test 9 - Indempotency - executing the same job twice, expexting to get the same result

def test_pipeline_jobs_are_idempotent():
    just("up")

    just(
        "run",
        "ingest-to-bronze",
        "trips:jc",
        "2021-02",
    )

    bronze_first_run = json.loads(
        just(
            "inspect",
            "bronze",
            "trips:jc",
            "2021-02",
        )
    )

    just(
        "run",
        "ingest-to-bronze",
        "trips:jc",
        "2021-02",
    )

    bronze_second_run = json.loads(
        just(
            "inspect",
            "bronze",
            "trips:jc",
            "2021-02",
        )
    )

    assert bronze_second_run == bronze_first_run

    just(
        "run",
        "transform-to-silver",
        "trips:jc",
        "2021-02",
    )

    silver_first_run = json.loads(
        just(
            "inspect",
            "silver",
            "trips:jc",
            "2021-02",
        )
    )

    just(
        "run",
        "transform-to-silver",
        "trips:jc",
        "2021-02",
    )

    silver_second_run = json.loads(
        just(
            "inspect",
            "silver",
            "trips:jc",
            "2021-02",
        )
    )

    assert silver_second_run == silver_first_run

    just(
        "run",
        "transform-to-gold",
        "station-daily",
        "2021-02-03",
    )

    gold_first_run = json.loads(
        just(
            "report",
            "daily-station-trips",
            "jc",
            "JC105",
            "2021-02-03",
        )
    )

    just(
        "run",
        "transform-to-gold",
        "station-daily",
        "2021-02-03",
    )

    gold_second_run = json.loads(
        just(
            "report",
            "daily-station-trips",
            "jc",
            "JC105",
            "2021-02-03",
        )
    )

    assert gold_second_run == gold_first_run

# Test 10 - Check that the process rejects wrong window grain

@pytest.mark.parametrize(
    ("layer", "job", "window"),
    [
        (
            "ingest-to-bronze",
            "trips:jc",
            "2021-02-03",
        ),
        (
            "transform-to-silver",
            "trips:jc",
            "2021-02-03",
        ),
        (
            "transform-to-gold",
            "station-daily",
            "2021-02",
        ),
    ],
)
def test_jobs_reject_wrong_window_grain(
    layer: str,
    job: str,
    window: str,
):
    just("up")

    with pytest.raises(
        subprocess.CalledProcessError
    ):
        just(
            "run",
            layer,
            job,
            window,
        )