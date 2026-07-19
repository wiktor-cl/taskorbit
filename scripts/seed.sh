#!/usr/bin/env bash
# Creates a few example jobs against a running stack so you can watch
# taskorbit actually do something right after `docker compose up`,
# instead of having to drive the API by hand first.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"

future() {
  local seconds="$1"
  date -u -d "+${seconds} seconds" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
    || date -u -v+"${seconds}"S +%Y-%m-%dT%H:%M:%SZ
}

echo "Waiting for ${BASE_URL} to be healthy..."
for i in $(seq 1 30); do
  if curl -sf "${BASE_URL}/healthz" > /dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Creating a one-off log job (fires in ~10s)..."
curl -sf -X POST "${BASE_URL}/jobs" -H "Content-Type: application/json" -d "{
  \"name\": \"say-hello\",
  \"job_type\": \"log\",
  \"payload\": {\"message\": \"Hello from taskorbit!\"},
  \"schedule_type\": \"once\",
  \"run_at\": \"$(future 10)\"
}" > /dev/null

echo "Creating a one-off HTTP job (calls the api service's own /healthz)..."
curl -sf -X POST "${BASE_URL}/jobs" -H "Content-Type: application/json" -d "{
  \"name\": \"ping-api\",
  \"job_type\": \"http\",
  \"payload\": {\"url\": \"http://api:8080/healthz\", \"method\": \"GET\"},
  \"schedule_type\": \"once\",
  \"run_at\": \"$(future 15)\"
}" > /dev/null

echo "Creating a cron job that logs once a minute..."
curl -sf -X POST "${BASE_URL}/jobs" -H "Content-Type: application/json" -d '{
  "name": "heartbeat-log",
  "job_type": "log",
  "payload": {"message": "tick"},
  "schedule_type": "cron",
  "cron_expr": "* * * * *"
}' > /dev/null

echo ""
echo "Seed complete. Watch it happen:"
echo "  docker compose logs -f worker-1 worker-2"
echo ""
echo "Check status any time:"
echo "  curl ${BASE_URL}/jobs"
