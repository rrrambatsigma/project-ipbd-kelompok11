#!/bin/sh
# jobs/download_lang_model.sh
# Download model fastText lid.176 (~917KB) untuk language detection.
# Jalankan sekali: chmod +x download_lang_model.sh && ./download_lang_model.sh

set -e

MODEL_DIR="$(dirname "$0")/models"
MODEL_PATH="$MODEL_DIR/lid.176.ftz"

mkdir -p "$MODEL_DIR"

if [ -f "$MODEL_PATH" ]; then
    echo "Model sudah ada di $MODEL_PATH — skip download."
    exit 0
fi

echo "Mengunduh model fastText lid.176 ke $MODEL_PATH ..."
wget -q https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz -O "$MODEL_PATH"
echo "Selesai."