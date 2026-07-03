import subprocess
import sys
import os
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
from prefect import flow

load_dotenv()

from failure_notifier import prefect_failure_hook, send_failure_alert

SPARK_CMD = [
    "spark-submit",
    "--packages", "org.apache.hadoop:hadoop-aws:3.4.2",
    "--py-files",
    "/app/jobs/lang_filter.py,/app/jobs/sentiment_udfs.py,/app/jobs/schema.py",
    "/app/jobs/news_sentiment_job.py",
    "--raw",
]


@flow(name="Euro News Preprocessing", log_prints=True, on_failure=[prefect_failure_hook])
def preprocessing_flow():
    logger.info("=" * 60)
    logger.info("Preprocessing flow dimulai — spark-submit news_sentiment_job.py --raw")
    logger.info("=" * 60)

    try:
        result = subprocess.run(SPARK_CMD, capture_output=True, text=True)
    except Exception as e:
        send_failure_alert(
            flow_name="Euro News Preprocessing",
            error_message=f"subprocess error: {e}",
            task_name="spark-submit"
        )
        raise

    if result.returncode != 0:
        error_log = (result.stderr or "")[:300] + (result.stdout or "")[:200]
        send_failure_alert(
            flow_name="Euro News Preprocessing",
            error_message=f"spark-submit gagal (code={result.returncode}): {error_log}",
            task_name="spark-submit"
        )
        raise RuntimeError(
            f"spark-submit gagal (return code={result.returncode})"
        )

    logger.info("=" * 60)
    logger.info("Preprocessing flow selesai dengan sukses")
    logger.info("=" * 60)


if __name__ == "__main__":
    preprocessing_flow()
