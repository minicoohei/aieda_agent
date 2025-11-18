FROM ghcr.io/astral-sh/uv:python3.12-bookworm AS base

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --all-extras

COPY . .

EXPOSE 2718

CMD ["uv", "run", "marimo", "run", "notebooks/index.py", "--host", "0.0.0.0", "--port", "2718"]

