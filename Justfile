up:
    docker volume create bronze-data
    docker compose up -d postgres

down:
    docker compose down

run layer job window:
    echo " Layer: {{layer}} Job: {{job}} Window: {{window}}"