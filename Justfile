up:
    #!/usr/bin/env bash
    set -euo pipefail

    docker compose build compute airflow-init

    compute_uid="$(docker compose run --rm --no-deps -T --entrypoint id compute -u)"

    docker_gid="$(docker compose run --rm --no-deps -T --user root --entrypoint bash airflow-init -c 'stat -c %g /var/run/docker.sock')"
    export DOCKER_GID="$docker_gid"

    docker compose run --rm --no-deps -T --user root --entrypoint bash airflow-init -c "chown -R ${compute_uid}:0 /bronze /opt/airflow/logs && chmod -R u+rwX /bronze /opt/airflow/logs"

    docker compose up -d --wait postgres

    docker compose run --rm --no-deps -T compute python src/meridian/cli.py init warehouse

    docker compose up -d --wait airflow-api-server airflow-scheduler airflow-dag-processor

    dags_ready=false

    for attempt in {1..30}; do
        if docker compose exec -T airflow-scheduler python -c "from meridian.operational.runs import get_airflow_token, read_airflow_data; token = get_airflow_token(); read_airflow_data('dags/trips_jc_monthly', token); read_airflow_data('dags/trips_nyc_monthly', token)" > /dev/null 2>&1; then
            dags_ready=true
            break
        fi

        sleep 3
    done

    if [ "$dags_ready" != "true" ]; then
        echo "DAGs were not ready. Check Airflow API and DAG processor logs." >&2
        exit 1
    fi

    docker compose exec -T airflow-scheduler airflow dags pause trips_jc_monthly --yes
    docker compose exec -T airflow-scheduler airflow dags pause trips_nyc_monthly --yes

down:
    docker compose down -v

run layer job window="" end_window="":
    #!/usr/bin/env bash
    set -e

    if [ "{{layer}}" = "pipeline" ]; then
        docker compose exec -T airflow-scheduler python src/meridian/cli.py run "{{layer}}" "{{job}}" "{{window}}" "{{end_window}}"
    else
        docker compose run --rm --no-deps -T compute python src/meridian/cli.py run "{{layer}}" "{{job}}" "{{window}}" "{{end_window}}"
    fi

inspect layer job window="":
    #!/usr/bin/env bash
    set -e

    if [ "{{layer}}" = "runs" ]; then
        docker compose exec -T airflow-scheduler python src/meridian/cli.py inspect "{{layer}}" "{{job}}" "{{window}}"
    else
        docker compose run --rm --no-deps -T compute python src/meridian/cli.py inspect "{{layer}}" "{{job}}" "{{window}}"
    fi

report report_name market station day:
    @docker compose run --rm --no-deps -T compute python src/meridian/cli.py report "{{report_name}}" "{{market}}" "{{station}}" "{{day}}"

test:
    pytest -q

schedule market state:
    #!/usr/bin/env bash
    set -e

    if [ "{{market}}" != "jc" ] && [ "{{market}}" != "nyc" ]; then
        echo "Unsupported market: {{market}}" >&2
        exit 1
    fi

    if [ "{{state}}" = "on" ]; then
        action="unpause"
    elif [ "{{state}}" = "off" ]; then
        action="pause"
    else
        echo "Schedule state must be on or off" >&2
        exit 1
    fi

    docker compose exec -T airflow-scheduler airflow dags "$action" "trips_{{market}}_monthly" --yes --output json
