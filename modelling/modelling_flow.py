import subprocess
import sys
import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from prefect import flow

load_dotenv()

from failure_notifier import prefect_failure_hook, send_failure_alert

WORK_DIR = Path("/app/modelling")


@flow(name="Euro News Modelling", log_prints=True, on_failure=[prefect_failure_hook])
def modelling_flow():
    logger.info("=" * 60)
    logger.info("Modelling flow dimulai — python run_pipeline.py")
    logger.info("=" * 60)

    try:
        result = subprocess.run(
            [sys.executable, "run_pipeline.py"],
            cwd=WORK_DIR,
            capture_output=True, text=True,
        )
    except Exception as e:
        send_failure_alert(
            flow_name="Euro News Modelling",
            error_message=f"subprocess error: {e}",
            task_name="run_pipeline"
        )
        raise

    if result.returncode != 0:
        error_log = (result.stderr or "")[:300] + (result.stdout or "")[:200]
        send_failure_alert(
            flow_name="Euro News Modelling",
            error_message=f"Modelling pipeline gagal (code={result.returncode}): {error_log}",
            task_name="run_pipeline"
        )
        raise RuntimeError(
            f"Modelling pipeline gagal (return code={result.returncode})"
        )

    logger.info("=" * 60)
    logger.info("Modelling flow selesai dengan sukses")
    logger.info("=" * 60)


if __name__ == "__main__":
    modelling_flow()
