FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HOME=/root/.cache/huggingface \
    PORT=7860

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libjpeg-dev zlib1g-dev libpng-dev git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml poetry.lock* ./

RUN pip install --upgrade pip poetry && \
    poetry config virtualenvs.create false && \
    python -m pip install --index-url https://download.pytorch.org/whl/cpu torch torchvision && \
    poetry install --no-root --only main

COPY . /app

EXPOSE 7860

CMD ["uvicorn", "mush_finder.server:app", "--host", "0.0.0.0", "--port", "7860"]
