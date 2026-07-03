import os
import io
import pandas as pd
from datetime import datetime
from loguru import logger
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import requests

from config import BUCKET_PROCESSED, MODEL_PATH, METRICS_FILE
from model_store import _get_s3, save_csv

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN_MODELLING", "8883638359:AAFlmg19cd5oOdhFPDQbXSNZrW8r7nHgcPk")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_MODELLING", "-1004360195236")
PUSHGATEWAY_URL = "http://pushgateway:9091"


def _send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"Telegram modelling gagal: {e}")


def _push_metrics(
    duration: int, accuracy: float, precision_macro: float,
    recall_macro: float, f1_macro: float, lda_coherence: float,
    n_articles: int, n_train: int, n_test: int,
):
    registry = CollectorRegistry()
    Gauge("modelling_articles_total", "", registry=registry).set(n_articles)
    Gauge("modelling_accuracy", "", registry=registry).set(accuracy)
    Gauge("modelling_f1_macro", "", registry=registry).set(f1_macro)
    Gauge("modelling_precision_macro", "", registry=registry).set(precision_macro)
    Gauge("modelling_recall_macro", "", registry=registry).set(recall_macro)
    Gauge("modelling_lda_coherence", "", registry=registry).set(lda_coherence)
    Gauge("modelling_duration_seconds", "", registry=registry).set(duration)
    Gauge("modelling_n_train", "", registry=registry).set(n_train)
    Gauge("modelling_n_test", "", registry=registry).set(n_test)
    try:
        push_to_gateway(PUSHGATEWAY_URL, job="modelling", registry=registry)
        logger.info("[Metrics] Pushed to Pushgateway")
    except Exception as e:
        logger.warning(f"[Metrics] Gagal push: {e}")


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
    duration: int = 0,
    report_df=None,
    confusion_df=None,
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

    # ── Push metrics ──
    _push_metrics(
        duration=duration,
        accuracy=accuracy,
        precision_macro=precision_macro,
        recall_macro=recall_macro,
        f1_macro=f1_macro,
        lda_coherence=lda_coherence or 0.0,
        n_articles=n_articles,
        n_train=n_train,
        n_test=n_test,
    )

    # ── Telegram notification ──
    coherence_str = f"{lda_coherence:.4f}" if lda_coherence else "N/A"
    acc_pct = accuracy * 100
    f1_pct = f1_macro * 100
    prec_pct = precision_macro * 100
    rec_pct = recall_macro * 100

    msg = (
        f"🤖 <b>Modelling Pipeline Selesai!</b>\n"
        f"{'─' * 28}\n"
        f"🆔 Run ID       : {run_id}\n"
        f"📥 Input        : {n_articles:,} artikel\n"
        f"📊 Train/Test   : {n_train:,} / {n_test:,}\n"
        f"⏱️ Durasi       : {duration} detik\n\n"
        f"📈 <b>Kinerja Model</b>\n"
        f"  Accuracy      : {acc_pct:.2f}%\n"
        f"  F1 Macro      : {f1_pct:.2f}%\n"
        f"  Precision     : {prec_pct:.2f}%\n"
        f"  Recall        : {rec_pct:.2f}%\n"
    )

    if report_df is not None and not report_df.empty:
        msg += f"\n📋 <b>Per Class</b>\n"
        for _, row in report_df.iterrows():
            cls = row["class"]
            if cls in ("macro avg", "weighted avg", "accuracy"):
                continue
            msg += (
                f"  {cls:>8} \u2192 "
                f"P:{row['precision']*100:.1f}% "
                f"R:{row['recall']*100:.1f}% "
                f"F1:{row['f1']*100:.1f}% "
                f"(n={int(row['support'])})\n"
            )

    if confusion_df is not None and not confusion_df.empty:
        msg += f"\n🔄 <b>Confusion Matrix</b>\n"
        cols = list(confusion_df.columns)
        header = "          " + "  ".join(f"{c:>8}" for c in cols)
        msg += f"<code>{header}\n"
        for idx, row in confusion_df.iterrows():
            vals = "  ".join(f"{int(row[c]):>8}" for c in cols)
            msg += f"  {str(idx):<8}{vals}\n"
        msg += "</code>"

    msg += (
        f"\n📚 <b>LDA Topic Coherence</b>\n"
        f"  ✦ Score: {coherence_str}\n"
    )

    _send_telegram(msg)
