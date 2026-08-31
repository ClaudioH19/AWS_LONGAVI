FROM node:20-slim AS frontend-builder

WORKDIR /frontend

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build


FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gosu sqlite3 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 10001 appuser \
    && useradd --uid 10001 --gid appuser --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /data \
    && chown appuser:appuser /data

COPY requirements.txt requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock

COPY backend ./backend
COPY weather_server.py .
COPY payload.js .
COPY entrypoint.sh .
COPY --from=frontend-builder /frontend/dist ./frontend_dist
RUN chmod +x entrypoint.sh

ENV DB_PATH=/data/weather_data.db
ENV PORT=3000
ENV LOG_FILE=""
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV SQLITE_JOURNAL_MODE=DELETE

EXPOSE 3000

ENTRYPOINT ["./entrypoint.sh"]
