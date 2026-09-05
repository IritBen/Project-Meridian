FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.10.12 /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./