FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install --upgrade pip && \
    pip install uv

COPY pyproject.toml .

# Use uv to install dependencies with dev dependencies
RUN uv venv && uv pip install -e ".[dev]" && uv pip install pytest pytest-cov

COPY . .

ENV PYTHONPATH=$PYTHONPATH:/app/src:/app/tests