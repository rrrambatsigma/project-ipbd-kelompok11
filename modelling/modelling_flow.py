import subprocess
import sys
import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from prefect import flow

load_dotenv()

WORK_DIR = Path("/app/modelling")


@flow(name="Euro News Modelling", log_prints=True)
def modelling_flow():
    logger.info("=" * 60)
    logger.info("Modelling flow dimulai — python run_pipeline.py")
    logger.info("=" * 60)

    result = subprocess.run(
        [sys.executable, "run_pipeline.py"],
        cwd=WORK_DIR,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Modelling pipeline gagal (return code={result.returncode})"
        )

    logger.info("=" * 60)
    logger.info("Modelling flow selesai dengan sukses")
    logger.info("=" * 60)


if __name__ == "__main__":
    modelling_flow()
