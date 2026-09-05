# Test 1 - postgres up
# Invoke just up
    # for 10 times try :
        # connect to postgres (timeout 2s)
        # success -> ok
        # failure -> sleep 2

DSN = "postgresql://meridian:meridian@localhost:5432/meridian"

import subprocess
import pytest
import psycopg
import time
import docker
from collections.abc import Callable
from functools import partial
import json

def just(command: str, *args: str) -> str:
    result = subprocess.run(["just", command, *args], check=True, stdout=subprocess.PIPE, text=True)
    return result.stdout


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

VOLUME_NAME = "bronze-data"

def is_volume_exists(volume_name: str) -> bool:
    client = docker.from_env()
    try:
        client.volumes.get(volume_name)
        return True
    except docker.errors.NotFound:
        return False

def test_just_up_volume():
    just("up")
    volume_is_exists = wait_for(partial(is_volume_exists, VOLUME_NAME), 10, 2)
    assert volume_is_exists


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

def test_just_down_volume():
    just("up")
    assert is_volume_exists(VOLUME_NAME), "volume is not up in time"
    just("down")
    assert is_volume_exists(VOLUME_NAME), "volume is down after just down and suppose to be up"

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

         
    
    
