FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.10.12 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --locked --no-dev --no-install-project

COPY src/ ./src/

RUN uv sync --locked --no-dev

ENV PATH="/app/.venv/bin:$PATH"
