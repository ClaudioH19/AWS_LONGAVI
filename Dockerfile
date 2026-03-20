FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY weather_server.py .
COPY dashboard.html .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENV DB_PATH=/data/weather_data.db
ENV LOG_FILE=/data/weather_server.log
ENV PORT=3000

EXPOSE 3000

ENTRYPOINT ["./entrypoint.sh"]