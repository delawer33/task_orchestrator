# Этап сборки
FROM python:3.10-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
COPY config/ /app/config/
COPY dlx_handler/ /app/dlx_handler_code/

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir -r requirements.txt

FROM python:3.10-slim

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/config /app/config
COPY --from=builder /app/dlx_handler_code /app/dlx_handler_code

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app:/app/config:/app/dlx_handler_code \
    PYTHONUNBUFFERED=1 \
    DLX_CONCURRENCY="2"

RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-m", "dlx_handler_code.main"]