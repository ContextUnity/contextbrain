# syntax=docker/dockerfile:1
FROM python:3.13-slim
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
COPY packages/core packages/core
COPY services/brain services/brain

# Compose runs the default local ONNX profile. Base installations can instead
# select an HTTP provider or deterministic test profile without this extra.
RUN uv sync --frozen --no-dev --package contextunity-brain --extra embeddings-onnx

RUN /app/.venv/bin/python -c "from contextunity.brain.core.config.providers import EmbeddingProviderConfig; from contextunity.brain.service.embeddings.onnx import prefetch_onnx_assets; prefetch_onnx_assets(EmbeddingProviderConfig(model_cache_dir='/var/cache/contextunity/huggingface'))"

ENV UV_NO_SYNC=1
ENV HF_HUB_OFFLINE=1
ENV PATH="/app/.venv/bin:$PATH"
WORKDIR /app/services/brain
CMD ["contextbrain", "serve"]
