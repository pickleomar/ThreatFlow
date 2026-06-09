#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOST="${KAFKA_HOST:-kafka}"
KAFKA_PORT="${KAFKA_PORT:-9094}"

echo "[entrypoint] waiting for Kafka at ${KAFKA_HOST}:${KAFKA_PORT}..."
# Wait until the Kafka port accepts connections (bash /dev/tcp, no extra tools needed)
until (echo > /dev/tcp/${KAFKA_HOST}/${KAFKA_PORT}) >/dev/null 2>&1; do
  echo "[entrypoint] Kafka not ready yet, retrying in 3s..."
  sleep 3
done
echo "[entrypoint] Kafka is reachable. Submitting Spark job."

# Clear stale checkpoints: if the Kafka topic was recreated (e.g. compose down -v),
# old saved offsets point to messages that no longer exist -> OffsetOutOfRangeException.
# Safe to wipe in this dev setup; the job re-reads from 'latest' on a fresh start.
echo "[entrypoint] clearing old checkpoints..."
rm -rf /data/checkpoints/* 2>/dev/null || true

# JARs are already baked into /opt/spark/jars, so NO --packages / no Maven download.
exec /opt/spark/bin/spark-submit \
  --master "local[*]" \
  /app/stream_processor.py