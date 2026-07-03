import pandas as pd
import numpy as np
from sklearn.metrics import classification_report, confusion_matrix
from loguru import logger


def classification_report_df(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    label_map: dict[int, str],
) -> pd.DataFrame:
    report = classification_report(
        y_true, y_pred,
        labels=sorted(label_map.keys()),
        target_names=[label_map[k] for k in sorted(label_map.keys())],
        output_dict=True,
    )
    rows = []
    for class_name in [label_map[k] for k in sorted(label_map.keys())]:
        rows.append({
            "class": class_name,
            "precision": round(report[class_name]["precision"], 4),
            "recall": round(report[class_name]["recall"], 4),
            "f1": round(report[class_name]["f1-score"], 4),
            "support": int(report[class_name]["support"]),
        })
    rows.append({
        "class": "macro avg",
        "precision": round(report["macro avg"]["precision"], 4),
        "recall": round(report["macro avg"]["recall"], 4),
        "f1": round(report["macro avg"]["f1-score"], 4),
        "support": int(report["macro avg"]["support"]),
    })
    rows.append({
        "class": "weighted avg",
        "precision": round(report["weighted avg"]["precision"], 4),
        "recall": round(report["weighted avg"]["recall"], 4),
        "f1": round(report["weighted avg"]["f1-score"], 4),
        "support": int(report["weighted avg"]["support"]),
    })
    rows.append({
        "class": "accuracy",
        "precision": round(report["accuracy"], 4),
        "recall": round(report["accuracy"], 4),
        "f1": round(report["accuracy"], 4),
        "support": int(sum(report[c]["support"] for c in label_map.values())),
    })
    return pd.DataFrame(rows)


def confusion_matrix_df(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    label_map: dict[int, str],
) -> pd.DataFrame:
    labels = sorted(label_map.keys())
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    class_names = [label_map[l] for l in labels]
    df = pd.DataFrame(cm, index=class_names, columns=class_names)
    df.index.name = "actual"
    df.columns.name = "predicted"
    return df


def print_summary(
    report_df: pd.DataFrame,
    cm_df: pd.DataFrame,
    accuracy: float,
    lda_coherence: float | None = None,
    n_articles: int = 0,
    run_id: str = "",
):
    logger.info("=" * 50)
    logger.info(f"  RUN SUMMARY  [{run_id}]")
    logger.info("=" * 50)
    logger.info(f"  Articles used: {n_articles}")
    if lda_coherence is not None:
        logger.info(f"  LDA coherence: {lda_coherence:.4f}")
    logger.info(f"  Accuracy: {accuracy:.4f}")
    logger.info("")
    logger.info("  Classification Report:")
    logger.info(f"  {'Class':>12}  {'Prec':>6}  {'Recall':>6}  {'F1':>6}  {'Support':>8}")
    logger.info("  " + "-" * 42)
    for _, row in report_df.iterrows():
        logger.info(
            f"  {row['class']:>12}  {row['precision']:>6.3f}  "
            f"{row['recall']:>6.3f}  {row['f1']:>6.3f}  {int(row['support']):>8}"
        )
    logger.info("")
    logger.info("  Confusion Matrix:")
    for label in cm_df.index:
        vals = "  ".join(f"{cm_df.loc[label, c]:>5}" for c in cm_df.columns)
        logger.info(f"    actual {label:<10}  {vals}")
    logger.info("=" * 50)
