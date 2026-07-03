"""
jobs/lang_filter.py
─────────────────────────────────────────────────────────
Deteksi & filter bahasa artikel pakai fastText (lid.176).

Kenapa bukan pakai field `language` mentah dari source?
Karena tidak konsisten:
  - GDELT  → kadang nama bahasa penuh ("English"), bukan kode ISO
  - Guardian/NewsAPI → kadang kosong, tergantung query saat scraping
  - ECB → biasanya English, tapi tidak dijamin

Jadi deteksi langsung dari isi `raw_text` lebih reliable.
─────────────────────────────────────────────────────────
"""

import os
import pandas as pd
from loguru import logger

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

MODEL_PATH = os.getenv(
    "FASTTEXT_LID_PATH",
    "/app/jobs/models/lid.176.ftz",
)

_model = None  # lazy-loaded sekali per executor process


def _load_model():
    global _model
    if _model is None:
        import fasttext
        _model = fasttext.load_model(MODEL_PATH)
    return _model


def model_available() -> bool:
    """Cek apakah model fastText sudah ter-download."""
    return os.path.exists(MODEL_PATH)


def _detect_batch(texts: list[str]) -> list[str]:
    """Deteksi bahasa untuk satu batch teks. Return kode ISO 639-1 ('en', 'fr', dst)."""
    model = _load_model()
    results = []
    for t in texts:
        if not t or len(t.strip()) < 3:
            results.append("unknown")
            continue
        clean = t.replace("\n", " ").replace("\r", " ").strip()[:500]
        try:
            label, _prob = model.predict(clean, k=1)
            results.append(label[0].replace("__label__", ""))
        except Exception:
            results.append("unknown")
    return results


@pandas_udf(StringType())
def _detect_lang_udf(text_series: pd.Series) -> pd.Series:
    return pd.Series(_detect_batch(text_series.tolist()))


def filter_english(df: DataFrame, text_col: str = "raw_text") -> DataFrame:
    """
    Tambah kolom `detected_language` dan filter hanya baris English.

    Jika model fastText belum ter-download, fungsi ini akan SKIP filtering
    (warning, bukan crash) supaya pipeline tetap bisa dites end-to-end
    sebelum model di-setup. Kolom `detected_language` akan diisi "unknown".
    """
    if not model_available():
        logger.warning(
            f"[LANG_FILTER] Model fastText tidak ditemukan di {MODEL_PATH} — "
            "SKIP filtering bahasa. Download dengan:\n"
            "  mkdir -p jobs/models && wget "
            "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz "
            f"-O {MODEL_PATH}"
        )
        return df.withColumn("detected_language", F.lit("unknown"))

    before = df.count()
    df = df.withColumn("detected_language", _detect_lang_udf(F.col(text_col)))
    df = df.filter(F.col("detected_language") == "en")
    after = df.count()
    logger.success(
        f"[LANG_FILTER] {before} → {after} baris (hanya English, "
        f"dibuang: {before - after})"
    )
    return df