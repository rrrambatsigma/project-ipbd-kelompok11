import pandas as pd
from datetime import datetime
from loguru import logger
import io

from config import BUCKET_PROCESSED, MODEL_PATH, METRICS_FILE
from model_store import _get_s3, save_csv


def load_metrics_history() -> pd.DataFrame:
    s3 = _get_s3()
    key = f"{MODEL_PATH}/{METRICS_FILE}"
    try:
        obj = s3.get_object(Bucket=BUCKET_PROCESSED, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        logger.info(f"Loaded metrics history: {len(df)} previous runs")
        return df
    except Exception:
        logger.info("No existing metrics history, starting fresh")
        return pd.DataFrame()


def track_run(
    run_id: str,
    accuracy: float,
    precision_macro: float,
    recall_macro: float,
    f1_macro: float,
    lda_coherence: float | None,
    n_articles: int,
    n_train: int,
    n_test: int,
):
    new_row = pd.DataFrame([{
        "run_id": run_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "accuracy": round(accuracy, 4),
        "precision_macro": round(precision_macro, 4),
        "recall_macro": round(recall_macro, 4),
        "f1_macro": round(f1_macro, 4),
        "lda_coherence": round(lda_coherence, 4) if lda_coherence else 0.0,
        "n_articles": n_articles,
        "n_train": n_train,
        "n_test": n_test,
    }])

    history = load_metrics_history()
    history = pd.concat([history, new_row], ignore_index=True)

    save_csv(history, BUCKET_PROCESSED, f"{MODEL_PATH}/{METRICS_FILE}")
    logger.info(f"Metrics appended for run {run_id}")

    if len(history) > 1:
        prev = history.iloc[-2]
        curr = history.iloc[-1]
        acc_delta = curr["accuracy"] - prev["accuracy"]
        f1_delta = curr["f1_macro"] - prev["f1_macro"]
        direction_acc = "↑" if acc_delta >= 0 else "↓"
        direction_f1 = "↑" if f1_delta >= 0 else "↓"
        logger.info(
            f"  Trend: accuracy={curr['accuracy']:.4f} ({direction_acc}{abs(acc_delta):.4f}), "
            f"F1={curr['f1_macro']:.4f} ({direction_f1}{abs(f1_delta):.4f})"
        )
