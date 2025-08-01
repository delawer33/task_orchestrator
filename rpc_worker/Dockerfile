# Этап сборки
FROM python:3.10-slim AS builder

WORKDIR /app

# Установка зависимостей сборки
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Копирование requirements.txt из контекста сборки (корня проекта)
COPY requirements.txt .
# Копирование директории config
COPY config/ /app/config/
# Копирование кода RPC Worker из папки 'rpc_worker/'
COPY rpc_worker/ /app/rpc_worker_code/

# Создание и активация venv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Финальный образ
FROM python:3.10-slim

WORKDIR /app

# Копирование venv и необходимых исходных кодов
COPY --from=builder /opt/venv /opt/venv
# Копируем конфиг из builder'а
COPY --from=builder /app/config /app/config
# Копируем код RPC Worker из builder'а
COPY --from=builder /app/rpc_worker_code /app/rpc_worker_code

# Настройка окружения
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app:/app/config:/app/rpc_worker_code \
    PYTHONUNBUFFERED=1 \
    WORKER_CONCURRENCY="2"

# Пользователь приложения
RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && chown -R appuser:appuser /app
USER appuser

# Точка входа для RPC Worker
CMD ["sh", "-c", "python -m rpc_worker_code.main --concurrency ${WORKER_CONCURRENCY:-2}"]