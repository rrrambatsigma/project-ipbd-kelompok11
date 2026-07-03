from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import pandas as pd
import numpy as np
from loguru import logger

from config import (
    LDA_N_TOPICS,
    LDA_N_TOP_WORDS,
    LDA_MAX_DF,
    LDA_MIN_DF,
    LDA_RANDOM_STATE,
)


def run_lda(df: pd.DataFrame) -> dict:
    texts = df["clean_text"].tolist()

    logger.info(f"LDA: CountVectorizer (max_df={LDA_MAX_DF}, min_df={LDA_MIN_DF})")
    vectorizer = CountVectorizer(
        max_df=LDA_MAX_DF,
        min_df=LDA_MIN_DF,
        stop_words="english",
    )
    dtm = vectorizer.fit_transform(texts)
    vocab_size = dtm.shape[1]
    logger.info(f"  Vocabulary size: {vocab_size}")

    logger.info(f"LDA: fitting {LDA_N_TOPICS} topics...")
    lda = LatentDirichletAllocation(
        n_components=LDA_N_TOPICS,
        random_state=LDA_RANDOM_STATE,
        n_jobs=-1,
    )
    topic_dist = lda.fit_transform(dtm)
    logger.info(f"  Done. Topic distribution shape: {topic_dist.shape}")

    dominant_topics = np.argmax(topic_dist, axis=1)
    topic_probs = topic_dist.max(axis=1)

    topic_words = _get_top_words(vectorizer, lda, LDA_N_TOP_WORDS)
    for tid, words in enumerate(topic_words):
        logger.info(f"  Topic {tid}: {', '.join(words[:6])}...")

    coherence = _compute_coherence(lda, dtm)
    logger.info(f"  Topic coherence (avg pairwise): {coherence:.4f}")

    topics_df = pd.DataFrame({
        "article_id": df["article_id"].values,
        "dominant_topic": dominant_topics,
        "topic_prob": topic_probs,
    })
    for t in range(LDA_N_TOPICS):
        topics_df[f"topic_{t}_prob"] = topic_dist[:, t]

    top_words_df = pd.DataFrame(
        {"topic_id": range(LDA_N_TOPICS), "top_words": [", ".join(w) for w in topic_words]}
    )

    return {
        "vectorizer": vectorizer,
        "lda_model": lda,
        "topics_df": topics_df,
        "top_words_df": top_words_df,
        "topic_words": topic_words,
        "coherence": coherence,
    }


def _get_top_words(vectorizer, lda, n_top_words: int) -> list[list[str]]:
    feature_names = vectorizer.get_feature_names_out()
    topics = []
    for topic_idx, topic in enumerate(lda.components_):
        top_indices = topic.argsort()[: -n_top_words - 1 : -1]
        top_words = [feature_names[i] for i in top_indices]
        topics.append(top_words)
    return topics


def _compute_coherence(lda, dtm) -> float:
    n_topics = lda.n_components
    n_top_words = 10
    topic_words = []
    for topic in lda.components_:
        top_indices = topic.argsort()[: -n_top_words - 1 : -1]
        topic_words.append(set(top_indices))

    scores = []
    for i in range(n_topics):
        for j in range(i + 1, n_topics):
            common = len(topic_words[i] & topic_words[j])
            total = len(topic_words[i] | topic_words[j])
            if total > 0:
                scores.append(1 - common / total)

    return float(np.mean(scores)) if scores else 0.0
