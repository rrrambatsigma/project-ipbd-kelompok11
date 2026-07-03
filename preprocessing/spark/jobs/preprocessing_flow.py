import subprocess
import sys
import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from prefect import flow

load_dotenv()

SPARK_CMD = [
    "spark-submit",
    "--packages", "org.apache.hadoop:hadoop-aws:3.4.2",
    "--py-files",
    "/app/jobs/lang_filter.py,/app/jobs/sentiment_udfs.py,/app/jobs/schema.py",
    "/app/jobs/news_sentiment_job.py",
    "--raw",
]


@flow(name="Euro News Preprocessing", log_prints=True)
def preprocessing_flow():
    logger.info("=" * 60)
    logger.info("Preprocessing flow dimulai — spark-submit news_sentiment_job.py --raw")
    logger.info("=" * 60)

    result = subprocess.run(SPARK_CMD)

    if result.returncode != 0:
        raise RuntimeError(
            f"spark-submit gagal (return code={result.returncode})"
        )

    logger.info("=" * 60)
    logger.info("Preprocessing flow selesai dengan sukses")
    logger.info("=" * 60)


if __name__ == "__main__":
    preprocessing_flow()
