from datetime import datetime
from loguru import logger

from data_loader import load_articles
from lda_pipeline import run_lda
from sentiment_trainer import run_classifier
from evaluator import print_summary
from model_store import save_all, save_latest_symlink
from monitor import track_run


def main():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info("=" * 55)
    logger.info(f"  MODELLING PIPELINE  [{run_id}]")
    logger.info("=" * 55)

    # 1. Load data
    logger.info("\n[1/5] Loading articles from MinIO...")
    df = load_articles()
    n_articles = len(df)

    # 2. LDA
    logger.info("\n[2/5] Running LDA Topic Modeling...")
    lda_result = run_lda(df)
    coherence = lda_result["coherence"]

    # 3. Sentiment Classifier
    logger.info("\n[3/5] Training TF-IDF + LogisticRegression...")
    sentiment_result = run_classifier(df)
    accuracy = sentiment_result["accuracy"]

    report_df = sentiment_result["report_df"]
    precision_macro = report_df[report_df["class"] == "macro avg"]["precision"].values[0]
    recall_macro = report_df[report_df["class"] == "macro avg"]["recall"].values[0]
    f1_macro = report_df[report_df["class"] == "macro avg"]["f1"].values[0]
    n_train = len(df) - int(len(df) * 0.2)
    n_test = int(len(df) * 0.2)

    # 4. Print summary
    logger.info("\n[4/5] Evaluation Summary...")
    print_summary(
        report_df=sentiment_result["report_df"],
        cm_df=sentiment_result["confusion_df"],
        accuracy=accuracy,
        lda_coherence=coherence,
        n_articles=n_articles,
        run_id=run_id,
    )

    # 5. Save to MinIO
    logger.info("\n[5/5] Saving models to MinIO...")
    save_all(run_id, lda_result, sentiment_result)
    save_latest_symlink("lda", run_id)
    save_latest_symlink("sentiment", run_id)

    # Track metrics
    track_run(
        run_id=run_id,
        accuracy=accuracy,
        precision_macro=precision_macro,
        recall_macro=recall_macro,
        f1_macro=f1_macro,
        lda_coherence=coherence,
        n_articles=n_articles,
        n_train=n_train,
        n_test=n_test,
    )

    logger.info("\n" + "=" * 55)
    logger.info(f"  PIPELINE COMPLETE  [{run_id}]")
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
