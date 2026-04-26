#!/usr/bin/env bash
# bench-resource-monitor.sh — samples CPU and memory for the Grafana container
# and the plugin subprocess every INTERVAL seconds.
#
# Usage: ./bench-resource-monitor.sh [interval_sec]
#   interval_sec  Sampling interval in seconds (default: 5)
#
# Output columns: epoch_s  container_cpu%  container_mem  plugin_rss_kb  plugin_threads
#
# Notes:
#  - Container-level stats come from `docker stats` and include all processes.
#  - Plugin-level stats come from /proc/<pid>/status inside the container and
#    isolate just the grafana-kafka-datasource subprocess.
#  - The plugin process name used by pgrep is "grafana-kafka-da" (15-char limit).

set -euo pipefail

INTERVAL="${1:-5}"
CONTAINER="grafana-kafka-datasource"

echo "# Resource monitor (sampling every ${INTERVAL}s)" >&2
echo "# Columns: epoch_s  container_cpu%  container_mem_mib  plugin_rss_kb  plugin_threads" >&2
echo "# Press Ctrl+C to stop." >&2

while true; do
  EPOCH=$(date +%s)

  # Container-level stats (CPU% and memory usage).
  RAW=$(docker stats "$CONTAINER" --no-stream \
    --format "{{.CPUPerc}} {{.MemUsage}}" 2>/dev/null) || RAW="N/A N/A"
  CPU=$(echo "$RAW" | awk '{print $1}' | tr -d '%')
  MEM_RAW=$(echo "$RAW" | awk '{print $2}')
  # Convert MiB/GiB to a plain MiB number for consistency.
  MEM_MIB=$(echo "$MEM_RAW" | awk '
    /GiB/ { gsub(/GiB/, ""); printf "%.0f", $1 * 1024; next }
    /MiB/ { gsub(/MiB/, ""); printf "%.0f", $1; next }
    /kB/  { gsub(/kB/,  ""); printf "%.0f", $1 / 1024; next }
    { print "?" }
  ')

  # Plugin subprocess stats from inside the container.
  PLUGIN_PID=$(docker exec "$CONTAINER" \
    pgrep -f "grafana-kafka-datasource" 2>/dev/null | head -1) || PLUGIN_PID=""

  if [ -n "$PLUGIN_PID" ]; then
    VMRSS=$(docker exec "$CONTAINER" \
      awk '/VmRSS/{print $2}' /proc/"$PLUGIN_PID"/status 2>/dev/null) || VMRSS="?"
    THREADS=$(docker exec "$CONTAINER" \
      awk '/Threads/{print $2}' /proc/"$PLUGIN_PID"/status 2>/dev/null) || THREADS="?"
  else
    VMRSS="no_plugin"
    THREADS="?"
  fi

  echo "$EPOCH  ${CPU}%  ${MEM_MIB}MiB  ${VMRSS}kB  threads=${THREADS}"

  sleep "$INTERVAL"
done
