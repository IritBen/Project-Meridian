up:
    docker compose up -d --wait postgres
    docker compose run --rm --build compute python src/meridian/cli.py init warehouse

down:
    docker compose down -v

run layer job window:
    docker compose run --rm --build compute python src/meridian/cli.py run "{{layer}}" "{{job}}" "{{window}}"

inspect layer job window:
    @docker compose run --rm compute python src/meridian/cli.py inspect "{{layer}}" "{{job}}" "{{window}}"

report report_name market station day:
    @docker compose run --rm compute python src/meridian/cli.py report "{{report_name}}" "{{market}}" "{{station}}" "{{day}}"

test:
    pytest -q