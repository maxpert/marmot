#!/bin/bash
# Collect batch committer metrics from Marmot cluster
# Usage: ./collect-batch-metrics.sh <output_path> <duration_seconds> [interval_seconds]

set -e

OUTPUT_PATH="${1:?Usage: $0 <output_path> <duration_seconds> [interval_seconds]}"
DURATION="${2:?Usage: $0 <output_path> <duration_seconds> [interval_seconds]}"
INTERVAL="${3:-1}"  # Default: 1 second

PORTS=(8081 8082 8083)
METRICS_FILTER="marmot_v2_batch_committer"

echo "Collecting batch committer metrics for ${DURATION}s (interval: ${INTERVAL}s)"
echo "Output: ${OUTPUT_PATH}"

# Initialize output file with header
echo "timestamp,node_id,metric,value" > "${OUTPUT_PATH}"

START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION))

collect_metrics() {
    local ts=$(date +%s)
    local timestamp=$(date -r $ts +"%Y-%m-%d %H:%M:%S")

    for port in "${PORTS[@]}"; do
        curl -s "http://localhost:${port}/metrics" 2>/dev/null | \
        grep "^${METRICS_FILTER}" | \
        while read line; do
            # Parse Prometheus format: metric{labels} value
            metric=$(echo "$line" | sed 's/{.*$//' | sed 's/ .*//')
            node_id=$(echo "$line" | grep -o 'node_id="[^"]*"' | cut -d'"' -f2)
            value=$(echo "$line" | awk '{print $NF}')
            echo "${timestamp},${node_id:-$port},${metric},${value}"
        done
    done
}

while [ $(date +%s) -lt $END_TIME ]; do
    collect_metrics >> "${OUTPUT_PATH}"
    sleep "${INTERVAL}"
done

echo "Collection complete. Output saved to ${OUTPUT_PATH}"

# Print summary
echo ""
echo "=== Summary ==="
tail -100 "${OUTPUT_PATH}" | grep "flushes_total" | tail -6
