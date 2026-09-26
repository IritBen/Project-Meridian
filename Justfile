up:
    docker volume create bronze-data
    docker compose up -d postgres

down:
    docker compose down

run layer job window:
    docker compose run --rm --build compute python src/meridian/cli.py run "{{layer}}" "{{job}}" "{{window}}"

inspect layer job window:
    @docker compose run --rm compute python src/meridian/cli.py inspect "{{layer}}" "{{job}}" "{{window}}"

report report_name market station day:
    @docker compose run --rm compute python src/meridian/cli.py report "{{report_name}}" "{{market}}" "{{station}}" "{{day}}"