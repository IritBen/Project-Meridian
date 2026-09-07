up:
    docker volume create bronze-data
    docker compose up -d postgres

down:
    docker compose down

run layer job window:
    docker compose run --rm --build compute python src/meridian/cli.py run "{{layer}}" "{{job}}" "{{window}}"
