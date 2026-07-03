# Docker Setup — IPBD Kelompok 11 (RAFAH)
## Commodity Pipeline: GLD | BTC-USD | SI=F

Dokumen ini menjelaskan layanan Docker yang dibutuhkan oleh pipeline komoditas Rafah.
**Tidak ada perubahan pada `docker-compose.yml`** — semua layanan sudah tersedia dari
setup bersama Kelompok 11.

---

## Layanan Docker yang Digunakan

Pipeline komoditas Rafah menggunakan layanan yang **sudah didefinisikan** di
`docker-compose.yml` root (milik Jojo). Rafah tidak memerlukan container tambahan.

| Layanan    | Port          | Kegunaan untuk Rafah                                    |
|------------|---------------|---------------------------------------------------------|
| zookeeper  | 2181          | Koordinator Kafka (tidak diakses langsung)              |
| kafka      | 29092         | Topic `commodity_stream` — terima tick dari streaming   |
| postgres   | 5433          | DB `kurs_eur_db` — simpan Bronze/Silver/Gold layer      |
| minio      | 9000 / 9001   | Object storage Parquet (bucket `commodity-eur`)         |
| grafana    | 3001          | Dashboard monitoring harga komoditas                    |

---

## Perbedaan Rafah vs Jojo (Kurs EUR/USD)

| Aspek            | Jojo (Kurs)            | Rafah (Commodity)           |
|------------------|------------------------|-----------------------------|
| Kafka topic      | `kurs_eur_stream`      | `commodity_stream`          |
| MinIO bucket     | `kurs-eur`             | `commodity-eur`             |
| PostgreSQL tabel | `kurs_raw/silver/daily`| `commodity_raw/silver/daily`|
| Ticker           | EUR/USD                | GLD, BTC-USD, SI=F          |

---

## MinIO Bucket: `commodity-eur`

Bucket `kurs-eur` dibuat secara otomatis oleh `minio-init` di `docker-compose.yml`.
Bucket **`commodity-eur` TIDAK** dibuat oleh `minio-init` (hanya membuat `kurs-eur`).

**Solusi:** bucket `commodity-eur` dibuat secara **programatik** oleh
`rafah/spark_commodity.py` via fungsi `init_minio_bucket()` yang dipanggil
saat pipeline start.

```python
# rafah/spark_commodity.py — dipanggil di main()
def init_minio_bucket():
    client = boto3.client("s3", endpoint_url="http://localhost:9000", ...)
    client.create_bucket(Bucket="commodity-eur")  # idempotent — skip jika sudah ada
    return client
```

Struktur Parquet di MinIO:
```
commodity-eur/
  bronze/
    YYYY/MM/DD/
      commodity_raw_HHMMSS.parquet   ← tick mentah valid
  silver/
    YYYY/MM/DD/
      commodity_silver_HHMMSS.parquet ← window 1 menit + fitur
```

---

## Cara Start Layanan

```bash
# Dari root direktori websocket-streaming/
docker-compose up -d

# Tunggu semua container healthy, lalu jalankan pipeline:
python3 rafah/streaming_commodity.py    # WebSocket → Kafka
python3 rafah/spark_commodity.py        # Kafka → PostgreSQL + MinIO
```

## Verifikasi

```bash
# Cek status container
docker ps

# Cek MinIO bucket
# Buka: http://localhost:9001 (user: minioadmin / pass: minioadmin)

# Cek Grafana dashboard
# Buka: http://localhost:3001 (user: admin / pass: admin123)

# Verifikasi data PostgreSQL
python3 rafah/modelling/test_commodity_data.py
```
