# 📰 Euro News Ingestion Pipeline
### Analisis Market Flow Nilai Tukar Euro — Kelompok 11 IPBD

Pipeline batch ingestion berita Eropa dari **ECB**, **Reuters**, dan **NewsAPI**,
diorkestasi dengan **Prefect**, disimpan ke **MinIO** sebagai raw JSON.

---

## 🗂️ Struktur Proyek

```
E:\project-ipbd-kelompok11\
│   README.md
│
└───ingestion\
    │   .env                        ← Konfigurasi API key & credentials
    │   docker-compose.yml          ← Semua service Docker
    │   prefect.yaml                ← Konfigurasi deployment jadwal batch
    │
    ├───collector\
    │   │   Dockerfile
    │   │   requirements.txt
    │   │
    │   └───flows\
    │       │   news_ingestion_flow.py   ← Prefect flow utama (orkestrasi)
    │       │   __init__.py
    │       │
    │       ├───scrapers\
    │       │       ecb_scraper.py       ← Tier 1: ECB RSS Feed
    │       │       newsapi_scraper.py   ← Tier 3: NewsAPI REST
    │       │       reuters_scraper.py   ← Tier 2: Reuters RSS
    │       │       __init__.py
    │       │
    │       ├───storage\
    │       │       minio_client.py      ← Upload artikel ke MinIO
    │       │       __init__.py
    │       │
    │       └───utils\
    │               config.py
    │               __init__.py
    │
    └───prefect\                     ← (direktori reserved untuk config tambahan)
```

---

## ⚙️ Prasyarat

Pastikan sudah terinstall di komputer kamu:

| Tools | Versi minimal | Cek versi |
|---|---|---|
| Docker Desktop | 4.x | `docker --version` |
| Docker Compose | v2 | `docker compose version` |
| Python | 3.10+ | `python --version` |

> **Windows:** Pastikan Docker Desktop sudah **Running** (ikon di system tray hijau).

---

## 🚀 Cara Menjalankan

### Langkah 1 — Isi API Key

Buka file `.env` di `E:\project-ipbd-kelompok11\ingestion\.env`,
lalu isi bagian berikut:

```env
NEWSAPI_KEY=isi_api_key_kamu_disini
```

> Daftar gratis di [https://newsapi.org/register](https://newsapi.org/register) → dapat 100 request/hari.  
> ECB dan Reuters tidak perlu API key (RSS publik).

---

### Langkah 2 — Jalankan Semua Service

Buka **PowerShell** atau **Command Prompt**, masuk ke folder ingestion:

```powershell
cd E:\project-ipbd-kelompok11\ingestion
```

Jalankan semua service Docker:

```powershell
docker compose up -d
```

Tunggu sekitar **1–2 menit** hingga semua service siap. Cek status:

```powershell
docker compose ps
```

Output yang diharapkan (semua `running` / `healthy`):

```
NAME                STATUS
prefect-server      running (healthy)
prefect-worker      running
selenium-chrome     running (healthy)
minio               running (healthy)
minio-init          exited (0)        ← normal, hanya berjalan sekali
collector-app       running
```

---

### Langkah 3 — Buka Dashboard

| Service | URL | Login |
|---|---|---|
| **Prefect UI** (orkestrasi) | http://localhost:4200 | tidak perlu login |
| **MinIO Console** (storage) | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| **Selenium** (scraping) | http://localhost:4444 | tidak perlu login |

---

### Langkah 4 — Test Run Sekali (Manual)

Jalankan pipeline satu kali untuk memastikan semua scraper bekerja:

```powershell
docker compose exec collector-app python -m flows.news_ingestion_flow --run-once
```

Pantau log yang keluar. Jika berhasil, kamu akan melihat:

```
✓ MinIO health check: PASSED
[ECB] Selesai — 15 artikel dikumpulkan
[Reuters] Selesai — 8 artikel relevan
[NewsAPI] Selesai — 12 artikel unik
BATCH SUMMARY
  Status : SUCCESS
```

---

### Langkah 5 — Daftarkan Jadwal Batch

Daftarkan deployment dengan jadwal otomatis ke Prefect server:

```powershell
docker compose exec collector-app python -m flows.news_ingestion_flow --deploy
```

Dua deployment akan terdaftar:

| Deployment | Jadwal (WIB) | Keterangan |
|---|---|---|
| `euro-news-weekday` | 06:00, 14:00, 20:00, 00:00 | Senin–Jumat (4x/hari) |
| `euro-news-weekend` | 08:00 | Sabtu–Minggu (1x/hari) |

Verifikasi di Prefect UI → menu **Deployments**: http://localhost:4200/deployments

---

### Langkah 6 — Verifikasi Data di MinIO

1. Buka http://localhost:9001
2. Login: `minioadmin` / `minioadmin123`
3. Masuk ke bucket **`news-raw`**
4. Struktur folder yang terbentuk:

```
news-raw/
├── ecb/
│   └── 2025-06-05/
│       └── ecb_20250605_060023_a1b2c3d4.json
├── reuters/
│   └── 2025-06-05/
│       └── reuters_20250605_060045_b2c3d4e5.json
└── newsapi/
    └── 2025-06-05/
        └── newsapi_20250605_060112_c3d4e5f6.json
```

Setiap file JSON berisi satu artikel dengan format:

```json
{
  "title": "ECB holds rates steady amid inflation concerns",
  "url": "https://www.ecb.europa.eu/...",
  "published_at": "2025-06-05T04:30:00+00:00",
  "source": "ecb",
  "source_tier": 1,
  "category": "press_releases",
  "raw_text": "The Governing Council of the ECB...",
  "language": "en",
  "_ingested_at": "2025-06-05T06:00:23.412Z",
  "_source_tier": "ecb"
}
```

---

## 🛑 Menghentikan Service

```powershell
# Hentikan semua service (data tetap tersimpan)
docker compose down

# Hentikan dan hapus semua data (MinIO + Prefect)
docker compose down -v
```

---

## 🔧 Troubleshooting

### MinIO tidak bisa diakses
```powershell
# Cek log MinIO
docker compose logs minio

# Restart service MinIO saja
docker compose restart minio
```

### Scraper gagal / artikel 0
```powershell
# Lihat log detail collector
docker compose logs collector-app

# Cek apakah NEWSAPI_KEY sudah diisi
docker compose exec collector-app env | grep NEWSAPI_KEY
```

### Prefect UI tidak muncul di localhost:4200
```powershell
# Cek status prefect-server
docker compose logs prefect-server

# Tunggu 30 detik lagi lalu refresh browser
```

### Port sudah terpakai
Jika ada error `port is already allocated`, ganti port di `docker-compose.yml`:
```yaml
# Contoh: ganti port Prefect dari 4200 ke 4201
ports:
  - "4201:4200"
```

### Reset total (mulai dari awal)
```powershell
docker compose down -v
docker compose up -d
```

---

## 📊 Alur Pipeline Ingestion

```
Sumber Berita
    │
    ├── Tier 1: ECB RSS Feed          (gratis, resmi, tidak perlu auth)
    ├── Tier 2: Reuters RSS           (gratis, filter keyword EUR)
    └── Tier 3: NewsAPI.org           (100 req/hari free tier)
         │
         ▼
   Prefect Batch Flow
   (news_ingestion_flow.py)
         │
    ┌────┴────┐
    │  Tasks  │  health_check → scrape → upload → summary
    └────┬────┘
         │
         ▼
   MinIO Object Storage
   bucket: news-raw/
   format: JSON per artikel
         │
         ▼
   [Next] Preprocessing → Apache Spark
```

---

## 👥 Tim Kelompok 11

| Nama | Bagian |
|---|---|
| ... | Orkestrasi & Ingestion Berita (Prefect + Scrapers) |
| ... | Preprocessing (Apache Spark) |
| ... | Storage & Modeling |
| ... | Serving & Visualisasi |

---

## 📚 Referensi

- [Prefect Documentation](https://docs.prefect.io/2.16.9/)
- [MinIO Python SDK](https://min.io/docs/minio/linux/developers/python/API.html)
- [NewsAPI Documentation](https://newsapi.org/docs)
- [ECB RSS Feeds](https://www.ecb.europa.eu/home/html/rss.en.html)
- [Reuters RSS](https://www.reuters.com/tools/rss)