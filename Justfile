up:
    docker compose up -d --wait postgres
    docker compose run --rm --build compute python src/meridian/cli.py init warehouse

down:
    docker compose down -v

run layer job window="":
    #!/usr/bin/env bash
    set -e

    if [ "{{layer}}" = "pipeline" ]; then
        docker compose exec -T airflow-scheduler python src/meridian/cli.py run "{{layer}}" "{{job}}" "{{window}}"
    else
        docker compose run --rm compute python src/meridian/cli.py run "{{layer}}" "{{job}}" "{{window}}"
    fi

inspect layer job window="":
    #!/usr/bin/env bash
    set -e

    if [ "{{layer}}" = "runs" ]; then
        docker compose exec -T airflow-scheduler python src/meridian/cli.py inspect "{{layer}}" "{{job}}" "{{window}}"
    else
        docker compose run --rm compute python src/meridian/cli.py inspect "{{layer}}" "{{job}}" "{{window}}"
    fi

report report_name market station day:
    @docker compose run --rm compute python src/meridian/cli.py report "{{report_name}}" "{{market}}" "{{station}}" "{{day}}"

test:
    pytest -q