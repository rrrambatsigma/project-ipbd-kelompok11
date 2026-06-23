#!/bin/bash
set -e

WORK_POOL_NAME="modelling-pool"
DEPLOY_NAME="euro-news-modelling"
PREFECT_SERVER="http://prefect-server:4200/api"
MAX_RETRIES=30
PID_WORKER=""

shutdown() {
    echo ""
    echo " SIGTERM diterima — menghentikan worker gracefully..."
    if [ -n "$PID_WORKER" ] && kill -0 "$PID_WORKER" 2>/dev/null; then
        kill -TERM "$PID_WORKER"
        wait "$PID_WORKER" 2>/dev/null
    fi
    echo " Worker berhenti. Container siap dimatikan."
    exit 0
}
trap shutdown SIGTERM SIGINT

echo " Menunggu Prefect Server..."
RETRIES=0
until python -c "import urllib.request; urllib.request.urlopen('${PREFECT_SERVER}/health').read()" 2>/dev/null; do
    RETRIES=$((RETRIES + 1))
    if [ $RETRIES -ge $MAX_RETRIES ]; then
        echo " Prefect Server tidak reachable setelah ${MAX_RETRIES} percobaan"
        exit 1
    fi
    echo "  Server belum ready (${RETRIES}/${MAX_RETRIES}), tunggu 10s..."
    sleep 10
done
echo " Prefect Server ready"

cd /app/modelling

echo " Membuat work pool (skip jika sudah ada)..."
python -c "
import asyncio
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import WorkPoolCreate

async def main():
    async with get_client() as client:
        pools = await client.read_work_pools()
        exists = any(p.name == '${WORK_POOL_NAME}' for p in pools)
        if exists:
            print(f'  Work pool ${WORK_POOL_NAME} already exists, skip')
        else:
            await client.create_work_pool(WorkPoolCreate(name='${WORK_POOL_NAME}', type='process'))
            print(f'  Work pool ${WORK_POOL_NAME} created')

asyncio.run(main())
" || echo "  Gagal create work pool (mungkin udah ada)"

echo " Register deployment (skip jika sudah ada)..."
DEP_EXISTS=$(python -c "
import asyncio
from prefect.client.orchestration import get_client

async def main():
    async with get_client() as client:
        deps = await client.read_deployments()
        exists = any(d.name == 'euro-news-modelling' for d in deps)
        print(exists)

asyncio.run(main())
")
if [ "$DEP_EXISTS" = "True" ]; then
    echo "  Deployment sudah ada, skip"
else
    echo "  Register deployment..."
    prefect deploy --all 2>&1 || echo "  prefect deploy gagal"
fi

echo " Start worker..."
prefect worker start --pool "${WORK_POOL_NAME}" &
PID_WORKER=$!
echo "  Worker PID: ${PID_WORKER}"

while true; do
    if ! kill -0 "$PID_WORKER" 2>/dev/null; then
        echo " Worker process mati! Keluar..."
        exit 1
    fi
    sleep 30
done
