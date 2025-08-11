FROM python:3.10-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY config/ /app/config/
COPY rpc_worker/ /app/rpc_worker_code/

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.10-slim

WORKDIR /app


COPY --from=builder /opt/venv /opt/venv

COPY --from=builder /app/config /app/config

COPY --from=builder /app/rpc_worker_code /app/rpc_worker_code


ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app:/app/config:/app/rpc_worker_code \
    PYTHONUNBUFFERED=1 \
    WORKER_CONCURRENCY="2"


    RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && chown -R appuser:appuser /app
USER appuser


CMD ["sh", "-c", "python -m rpc_worker_code.main --concurrency ${WORKER_CONCURRENCY:-2}"]
