#!/usr/bin/env bash

set -u

# Load .env from repo root if available
if [ -f ".env" ]; then
  set -a
  source ".env"
  set +a
fi


KURS_API="${KURS_API:-http://100.96.124.11:8000}"
COMMODITY_API="${COMMODITY_API:-http://100.96.124.11:8001}"
NEWS_API="${NEWS_API:-http://100.118.244.91:8000}"

check_url() {
  local name="$1"
  local url="$2"

  echo
  echo "=============================="
  echo "$name -> $url"
  echo "=============================="

  echo "[TCP]"
  host=$(echo "$url" | sed -E 's#https?://([^:/]+).*#\1#')
  port=$(echo "$url" | sed -E 's#https?://[^:/]+:([0-9]+).*#\1#')
  if [ "$port" = "$url" ]; then
    port=80
  fi
  nc -vz -w 3 "$host" "$port" || true

  echo
  echo "[ROOT]"
  curl -sS --max-time 5 "$url/" || true

  echo
  echo
  echo "[OPENAPI PATHS]"
  body=$(curl -sS --max-time 5 "$url/openapi.json" || true)
  if echo "$body" | python3 -m json.tool >/dev/null 2>&1; then
    echo "$body" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("\n".join(d.get("paths", {}).keys()))'
  else
    echo "No valid JSON/OpenAPI response"
    echo "$body" | head -20
  fi
}

check_url "KURS" "$KURS_API"
check_url "COMMODITY" "$COMMODITY_API"
check_url "NEWS" "$NEWS_API"

echo
echo "Specific endpoint checks:"
for endpoint in \
  "$KURS_API/stats/summary" \
  "$KURS_API/kurs/latest" \
  "$KURS_API/kurs/daily" \
  "$KURS_API/predict/today" \
  "$KURS_API/market/signals/latest" \
  "$COMMODITY_API/stats/summary" \
  "$COMMODITY_API/commodity/daily?limit=5" \
  "$COMMODITY_API/commodity/silver?limit=5" \
  "$NEWS_API/stats/summary" \
  "$NEWS_API/market/signals/latest"
do
  echo
  echo ">>> $endpoint"
  curl -sS --max-time 5 "$endpoint" | head -40 || true
done
