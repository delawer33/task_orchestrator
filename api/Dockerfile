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
# Копирование кода API из папки 'api/' в поддиректорию 'api_code/' для монтирования в dev
COPY api/ /app/api_code/

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
# Копируем код API из builder'а (для продакшена, будет перекрываться томом для разработки)
COPY --from=builder /app/api_code /app/api_code
# Настройка окружения
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app:/app/config:/app/api_code \
    PYTHONUNBUFFERED=1

# Пользователь приложения
RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && chown -R appuser:appuser /app
USER appuser

# Точка входа для API
# Используем api_code.main:app, так как main.py находится в /app/api_code/main.py
CMD ["uvicorn", "api_code.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]