import os
from dotenv import load_dotenv

load_dotenv()

# ── MinIO ───────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET_PROCESSED = "news-processed"
ARTICLES_PATH = "sentiment/articles"
MODEL_PATH = "models"

# ── LDA ─────────────────────────────────────────
LDA_N_TOPICS = 8
LDA_N_TOP_WORDS = 10
LDA_MAX_DF = 0.95
LDA_MIN_DF = 2
LDA_RANDOM_STATE = 42

# ── Sentiment Classifier ────────────────────────
VADER_THRESHOLD_POS = 0.30
VADER_THRESHOLD_NEG = -0.30
TFIDF_MAX_FEATURES = 5000
TEST_SIZE = 0.2
RANDOM_STATE = 42

# ── Output ──────────────────────────────────────
METRICS_FILE = "metrics_history.csv"
