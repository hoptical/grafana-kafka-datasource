#!/usr/bin/env bash
# bench-lag-monitor.sh — polls Kafka end-offsets every INTERVAL seconds and
# prints a tab-separated line: <epoch_ms> <total_offset>
#
# The plugin does not use consumer groups (low-level partition assignment), so
# kafka-consumer-groups.sh cannot show lag directly. Instead, track the end
# offset over time and compare the derivative against the known produce rate.
#
# Usage: ./bench-lag-monitor.sh [topic] [interval_sec]
#   topic         Kafka topic to monitor (default: bench-p1)
#   interval_sec  Polling interval in seconds (default: 5)
#
# Output columns: epoch_ms  end_offset
# To compute consume rate from output:
#   awk 'NR>1 { rate=($2-prev_off)/($1-prev_ts)*1000; print rate " msg/s" }
#             { prev_ts=$1; prev_off=$2 }' lag.log

set -euo pipefail

TOPIC="${1:-bench-p1}"
INTERVAL="${2:-5}"

echo "# Monitoring topic: $TOPIC (polling every ${INTERVAL}s)" >&2
echo "# Columns: epoch_ms  total_end_offset" >&2
echo "# Press Ctrl+C to stop." >&2

while true; do
  TS_MS=$(date +%s%3N)
  OFFSETS=$(docker exec kafka \
    /opt/bitnami/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" \
    --time -1 2>/dev/null) || true

  if [ -z "$OFFSETS" ]; then
    echo "$TS_MS  (no offset data yet)"
  else
    TOTAL=$(echo "$OFFSETS" | awk -F: '{sum += $3} END {print sum+0}')
    echo "$TS_MS  $TOTAL"
  fi

  sleep "$INTERVAL"
done
