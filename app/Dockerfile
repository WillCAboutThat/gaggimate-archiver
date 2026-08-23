FROM python:3.12-slim

# tzdata: SYNC_SCHEDULE is interpreted in local time via the TZ env.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY archiver ./archiver

CMD ["python", "-m", "archiver.cli", "daemon"]
