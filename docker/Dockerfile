FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install --upgrade pip && \
    pip install uv

COPY pyproject.toml .

# Use uv to install dependencies
RUN uv venv && uv pip install -e .

COPY . .

EXPOSE 5000
ENV PYTHONPATH=$PYTHONPATH:src/
# Use uv to run Flask with explicit app module
CMD ["uv", "run", "-m", "org.boxbuilder.variablesengine.server"]