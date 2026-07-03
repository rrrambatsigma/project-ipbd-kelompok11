from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
from loguru import logger

from config import (
    VADER_THRESHOLD_POS,
    VADER_THRESHOLD_NEG,
    TFIDF_MAX_FEATURES,
    TEST_SIZE,
    RANDOM_STATE,
)


def _label_sentiment(compound: float) -> int:
    if compound >= VADER_THRESHOLD_POS:
        return 2
    elif compound <= VADER_THRESHOLD_NEG:
        return 0
    return 1


LABEL_MAP = {0: "negative", 2: "positive"}


def run_classifier(df: pd.DataFrame) -> dict:
    texts = df["clean_text"].tolist()
    y = df["vader_compound"].apply(_label_sentiment).values

    raw_counts = pd.Series(y).value_counts().sort_index()
    logger.info(f"Raw label distribution: neg={raw_counts.get(0,0)}, neut={raw_counts.get(1,0)}, pos={raw_counts.get(2,0)}")

    mask = y != 1
    logger.info(f"Drop neutral: {len(y) - mask.sum()} articles, remaining: {mask.sum()} articles")
    X_texts = np.array(texts)[mask]
    y = y[mask]

    indices = np.arange(len(texts))[mask]
    X_train, X_test, y_train, y_test, idx_train, idx_test = train_test_split(
        X_texts, y, indices,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE,
        stratify=y,
    )
    logger.info(f"Train: {len(X_train)}, Test: {len(X_test)}")

    logger.info(f"TF-IDF: max_features={TFIDF_MAX_FEATURES}, ngram=(1,2)")
    vectorizer = TfidfVectorizer(
        max_features=TFIDF_MAX_FEATURES,
        ngram_range=(1, 2),
        min_df=2,
        stop_words="english",
    )
    X_train_vec = vectorizer.fit_transform(X_train)
    X_test_vec = vectorizer.transform(X_test)
    logger.info(f"  Train shape: {X_train_vec.shape}")

    logger.info("Training LogisticRegression...")
    clf = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        multi_class="multinomial",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    clf.fit(X_train_vec, y_train)
    logger.info("  Done")

    y_pred = clf.predict(X_test_vec)
    y_proba = clf.predict_proba(X_test_vec)

    from evaluator import classification_report_df, confusion_matrix_df

    report_df = classification_report_df(y_test, y_pred, LABEL_MAP)
    cm_df = confusion_matrix_df(y_test, y_pred, LABEL_MAP)

    accuracy = (y_pred == y_test).mean()
    logger.info(f"Accuracy: {accuracy:.4f}")
    for _, row in report_df.iterrows():
        logger.info(f"  {row['class']:>10}  P={row['precision']:.3f}  R={row['recall']:.3f}  F1={row['f1']:.3f}")

    test_df = pd.DataFrame({
        "article_id": df["article_id"].values[idx_test],
        "true_label": y_test,
        "pred_label": y_pred,
        "true_class": pd.Series(y_test).map(LABEL_MAP).values,
        "pred_class": pd.Series(y_pred).map(LABEL_MAP).values,
    })

    return {
        "vectorizer": vectorizer,
        "classifier": clf,
        "report_df": report_df,
        "confusion_df": cm_df,
        "test_predictions": test_df,
        "accuracy": accuracy,
    }
