#!/usr/bin/env bash
set -e

cd "$(dirname "$0")/.."

source .venv/bin/activate

python3 rafah/modelling/market_flow_correlation.py

mkdir -p rafah/dashboard-react/public/market_flow_outputs

cp rafah/modelling/market_flow_outputs/* \
   rafah/dashboard-react/public/market_flow_outputs/

echo "[OK] Market Flow modelling outputs exported to React dashboard."
