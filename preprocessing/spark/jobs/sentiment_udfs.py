import os
import re

import nltk
import pandas as pd
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType, BooleanType,
)
from pyspark.sql.functions import pandas_udf

NLTK_DATA = "/usr/share/nltk_data"
nltk.data.path.insert(0, NLTK_DATA)

MIN_RELEVANCE_SCORE = 3
MIN_TEXT_LENGTH = 100
FUZZY_DEDUP_THRESHOLD = 0.85

SESSION_BOUNDARIES = {
    "pre_market": (23, 2),
    "open":       (2,  6),
    "mid":        (6,  10),
    "pre_close":  (10, 14),
    "overlap":    (14, 23),
}

FINANCIAL_KEYWORDS = {
    "inflation":      r"\binflation\b",
    "interest_rate":  r"\binterest rates?\b",
    "ecb":            r"\becb\b",
    "monetary_policy":r"\bmonetary policy\b",
    "gdp":            r"\bgdp\b",
    "recession":      r"\brecession\b",
    "unemployment":   r"\bunemployment\b",
    "growth":         r"\beconomic growth\b",
    "trade":          r"\btrade\b",
    "forex":          r"\bforex\b",
    "currency":       r"\bcurrency\b",
}

KEYWORD_WEIGHTS = {
    "inflation": 2, "interest_rate": 2, "ecb": 3,
    "monetary_policy": 2, "gdp": 1, "recession": 1,
    "unemployment": 1, "growth": 1, "trade": 1,
    "forex": 2, "currency": 2,
}

CORE_PATTERNS = [
    r"\beuro\b", r"\beur\b", r"\becb\b",
    r"\beuropean central bank\b", r"\beurozone\b", r"\beuro area\b",
]

MONETARY_PATTERNS = [
    r"\binterest rates?\b", r"\bmonetary policy\b",
    r"\binflation\b", r"\bcpi\b", r"\brate decision\b",
]

FOREX_PATTERNS = [
    r"\bexchange rates?\b", r"\bcurrency\b",
    r"\bforex\b", r"\beur/usd\b", r"\beuro dollar\b",
]

ECONOMY_PATTERNS = [
    r"\bgdp\b", r"\brecession\b", r"\bgrowth\b",
    r"\bunemployment\b", r"\bgerman economy\b", r"\bfrench economy\b",
]

SENTIMENT_SCHEMA = StructType([
    StructField("compound", FloatType(), True),
    StructField("pos",      FloatType(), True),
    StructField("neg",      FloatType(), True),
    StructField("neu",      FloatType(), True),
])

KEYWORD_FLAGS_SCHEMA = StructType([
    StructField(f"has_{kw}", BooleanType(), True) for kw in FINANCIAL_KEYWORDS
])

STOPWORD_EXTRA = {
    "said", "also", "would", "could", "may", "might",
    "latest", "report", "according", "yet", "still", "already",
    "much", "many", "even", "well", "get", "also",
}

_stop_words = None
_lemmatizer = None


def _get_nlp():
    global _stop_words, _lemmatizer
    if _stop_words is None:
        _stop_words = set(stopwords.words("english")) | STOPWORD_EXTRA
    if _lemmatizer is None:
        _lemmatizer = WordNetLemmatizer()
    return _stop_words, _lemmatizer


@pandas_udf(SENTIMENT_SCHEMA)
def vader_sentiment_udf(text_series: pd.Series) -> pd.DataFrame:
    analyzer = SentimentIntensityAnalyzer()
    results = []
    for t in text_series:
        if not t or not isinstance(t, str) or len(t.strip()) < 10:
            results.append({"compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0})
            continue
        scores = analyzer.polarity_scores(t)
        results.append(scores)
    return pd.DataFrame(results)


@pandas_udf(IntegerType())
def financial_relevance_udf(text_series: pd.Series) -> pd.Series:
    scores = []
    for t in text_series:
        if not t or not isinstance(t, str):
            scores.append(0)
            continue
        t_lower = t.lower()
        score = 0
        for kw, pattern in FINANCIAL_KEYWORDS.items():
            if re.search(pattern, t_lower):
                score += KEYWORD_WEIGHTS.get(kw, 1)
        scores.append(min(score, 20))
    return pd.Series(scores)


@pandas_udf(StringType())
def nlp_clean_udf(text_series: pd.Series) -> pd.Series:
    stop_words, lemmatizer = _get_nlp()
    results = []

    for t in text_series:
        if not t or not isinstance(t, str):
            results.append("")
            continue

        t = t.lower()
        t = re.sub(r"https?://\S+", "", t)
        t = re.sub(r"\b\d+\b", "", t)
        t = re.sub(r"[^\w\s'\-]", " ", t)
        t = re.sub(r"\s+", " ", t).strip()

        tokens = word_tokenize(t)
        clean = [
            lemmatizer.lemmatize(tok)
            for tok in tokens
            if tok not in stop_words and len(tok) > 2
        ]
        results.append(" ".join(clean))

    return pd.Series(results)


@pandas_udf(BooleanType())
def is_topic_relevant_udf(text_series: pd.Series) -> pd.Series:
    results = []
    for t in text_series:
        if not t or not isinstance(t, str):
            results.append(False)
            continue
        t_lower = t.lower()
        has_core = any(re.search(p, t_lower) for p in CORE_PATTERNS)
        has_monetary = any(re.search(p, t_lower) for p in MONETARY_PATTERNS)
        has_forex = any(re.search(p, t_lower) for p in FOREX_PATTERNS)
        has_economy = any(re.search(p, t_lower) for p in ECONOMY_PATTERNS)

        score = 0
        if has_core:
            score += 3
        if has_monetary:
            score += 2
        if has_forex:
            score += 2
        if has_economy:
            score += 1

        results.append(score >= MIN_RELEVANCE_SCORE)
    return pd.Series(results)


@pandas_udf(KEYWORD_FLAGS_SCHEMA)
def keyword_flags_udf(text_series: pd.Series) -> pd.DataFrame:
    results = []
    for t in text_series:
        if not t or not isinstance(t, str):
            results.append({f"has_{kw}": False for kw in FINANCIAL_KEYWORDS})
            continue
        t_lower = t.lower()
        row = {}
        for kw, pattern in FINANCIAL_KEYWORDS.items():
            row[f"has_{kw}"] = bool(re.search(pattern, t_lower))
        results.append(row)
    return pd.DataFrame(results)


@pandas_udf(StringType())
def session_tag_udf(hour_series: pd.Series) -> pd.Series:
    def _tag(h):
        if h >= 23 or h < 2:
            return "pre_market"
        elif h < 6:
            return "open"
        elif h < 10:
            return "mid"
        elif h < 14:
            return "pre_close"
        return "overlap"
    return hour_series.apply(_tag)


def jaccard_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    set_a = set(word_tokenize(a.lower()))
    set_b = set(word_tokenize(b.lower()))
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)
