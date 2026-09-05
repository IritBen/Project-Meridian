up:
    docker volume create bronze-data
    docker compose up -d postgres

down:
    docker compose down