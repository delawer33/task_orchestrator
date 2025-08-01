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
# Копирование кода Worker из папки 'worker/' в поддиректорию 'worker_code/' для монтирования в dev
COPY worker/ /app/worker_code/

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
# Копируем код Worker из builder'а (для продакшена, будет перекрываться томом для разработки)
COPY --from=builder /app/worker_code /app/worker_code

# Настройка окружения
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app:/app/config:/app/worker_code \
    PYTHONUNBUFFERED=1 \
    WORKER_CONCURRENCY="4" 
    # WORKER_QUEUES="default"

# Пользователь приложения
RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && chown -R appuser:appuser /app
USER appuser

# Точка входа для Worker
# Используем worker_code.main, так как main.py находится в /app/worker_code/main.py
CMD ["sh", "-c", "python -m worker_code.main --concurrency ${WORKER_CONCURRENCY}
#  --queues ${WORKER_QUEUES}"]