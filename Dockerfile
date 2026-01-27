# ContextBrain Dockerfile
#
# gRPC service for taxonomy, vectors, and knowledge graph

FROM python:3.13-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

# Copy pyproject first for layer caching
COPY pyproject.toml uv.lock* ./

# Copy application code
COPY src/ ./src/

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen || uv sync

# gRPC port
EXPOSE 50051

# Environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/src:/deps/contextcore/src

# Default command
CMD ["uv", "run", "python", "-m", "brain.server"]
