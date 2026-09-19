# In Class Exercise — El Niño ETL Case Study

> Latihan kelas end-to-end: dari memahami fenomena El Niño → profiling API Open-Meteo → merancang data model (Star Schema) → membangun pipeline ETL dengan Airflow → menghasilkan insight risiko cuaca.
> Reference solusi: [rydteguh/public-training — etl-case-study](https://github.com/rydteguh/public-training/tree/public-training/etl-case-study)

---

## Overview

Arsitektur pipeline yang dibangun di kelas (berjalan di stack lokal peserta: **Airflow + MinIO + DuckDB**):

```
Open-Meteo API (forecast / archive)
   → Airflow DAG ingest (Python)
   → MinIO landing zone (raw/open-meteo/*.json)
   → DuckDB Bronze  (bronze.open_meteo_daily)
   → DuckDB Silver  (dim_date, dim_location, fact_*)
   → DuckDB Gold    (gold.* dashboard-ready)
```

- **Airflow** menjalankan DAG `elnino_etl_pipeline` (schedule harian) — file: `dags/elnino_etl_pipeline.py` + `dags/elnino_common.py`.
- **MinIO** sebagai data lake (sama seperti DAG AdventureWorks, via Airflow Variables `MINIO_*`).
- **DuckDB** sebagai warehouse analitik (`/opt/airflow/dags/repo/elnino.duckdb`) dengan layer `bronze → silver → gold`.
- Reference asli (Postgres) ada di [rydteguh/public-training — etl-case-study](https://github.com/rydteguh/public-training/tree/public-training/etl-case-study); di sini diadaptasi ke DuckDB agar cocok dengan stack lokal peserta.

---

## Langkah 1 — Memahami Case Study (El Niño)

**Tujuan:** Memahami konteks bisnis dan problem.

- Peserta memahami fenomena El Niño dan dampaknya:
  - Kekeringan
  - Kebakaran hutan (karhutla)
  - Perubahan cuaca
- Peserta memahami kebutuhan analisis berbasis data cuaca.
- Peserta memahami pertanyaan bisnis:
  - *Wilayah Indonesia mana yang punya risiko kekeringan paling tinggi?*
  - *Bagaimana pola curah hujan & suhu selama El Niño?*
  - *Daerah mana yang punya potensi karhutla paling tinggi?*
  - *Kapan mitigasi / intervensi perlu dilakukan?*

**Pertanyaan bisnis yang dijawab di akhir kelas (dari `docs/data_model.md`):**
- Risiko kekeringan → `fact_drought_risk_daily`
- Potensi karhutla → `fact_fire_risk_daily`
- Timing mitigasi → `fact_mitigation_daily`

---

## Langkah 2 — Memahami Data Source (Open-Meteo API)

**Tujuan:** Mengenal sumber data.

Endpoint API yang digunakan:
- `/v1/forecast` — prediksi cuaca hingga 14 hari ke depan
- `/v1/archive` — data historis (repo memuat 1 tahun data per lokasi)
- `/v1/air-quality` — kualitas udara
- `/v1/flood` — data banjir

Parameter data cuaca penting (dari `fact_weather_daily`):
- `temperature_2m_mean_c`, `temperature_2m_max_c`
- `precipitation_sum_mm`
- `relative_humidity_min_pct`
- `wind_speed_10m_max_kmh`
- `soil_moisture_avg`
- `evapotranspiration_mm`

Limitasi API:
- Open-Meteo gratis tanpa API key untuk penggunaan wajar.
- Batasan rate-limit / jumlah call per hari.
- Forecast maksimal ~14 hari; archive terbatas pada rentang historis tertentu.

---

## Langkah 3 — Data Profiling menggunakan Postman

**Tujuan:** Memahami struktur data API.

- Peserta melakukan request API di Postman (mis. `GET https://api.open-meteo.com/v1/forecast?latitude=-6.2&longitude=106.8&daily=temperature_2m_max,precipitation_sum&timezone=Asia/Jakarta`).
- Peserta menganalisis response JSON.
- Peserta mengidentifikasi field penting untuk pipeline.

Contoh response (disingkat):
```json
{
  "latitude": -6.2,
  "longitude": 106.8,
  "daily": {
    "time": ["2026-06-26", "2026-06-27"],
    "temperature_2m_max": [32.1, 31.8],
    "precipitation_sum": [0.0, 1.2]
  }
}
```

Setiap file JSON yang ditulis ke MinIO menyimpan: API response data, source endpoint, request parameters, run date, ingestion timestamp.

---

## Langkah 4 — Mendesain Data Model (Star Schema)

**Tujuan:** Mendesain struktur data warehouse.

Model memisahkan output bisnis ke dalam 4 fact tables (grain: **1 row per location per day**).

**Dimensions:**
- `dim_date` — `date_key` (YYYYMMDD), `full_date`, `year`, `month`, `month_name`, `season_indonesia` (wet/dry/transition), `climate_event`
- `dim_location` — `location_key`, `location_name`, `province`, `island`, `latitude`, `longitude`, `peatland_flag` (penting untuk risiko karhutla)

**Facts:**
- `fact_weather_daily` — metric cuaca harian mentah dari Open-Meteo
- `fact_drought_risk_daily` — risiko kekeringan turunan
- `fact_fire_risk_daily` — risiko karhutla turunan
- `fact_mitigation_daily` — prioritas intervensi

Relasi: setiap fact dihubungkan ke `dim_date` via `date_key` dan ke `dim_location` via `location_key` (star schema klasik).

> Catatan: di repo reference, tabel `dim_weather_condition` dan `dim_risk_level` dari materi kelas digabung ke dalam kolom `drought_risk_level` / `fire_risk_level` di masing-masing fact table untuk menyederhanakan desain.

---

## Langkah 5 — Memahami Pipeline Architecture

**Tujuan:** Memahami alur ETL.

Flow diagram pipeline:

```
[Open-Meteo API]
      │
      ▼
[Ingestion Engine — Airflow task (dynamic mapping)]
      │  writes JSON
      ▼
[MinIO Landing Zone]  raw/open-meteo/{dataset}/location={loc}/ingestion_date=YYYY-MM-DD/{ts}.json
      │
      ▼
[Bronze — DuckDB]  → bronze.open_meteo_daily
      │
      ▼
[Silver Transform]  → dim_date, dim_location, fact_weather_daily,
      │                 fact_drought_risk_daily, fact_fire_risk_daily, fact_mitigation_daily
      ▼
[Gold Transform]  → gold.drought_risk_by_region, gold.el_nino_weather_trend,
                     gold.karhutla_risk_by_region, gold.mitigation_priority_daily
      │
      ▼
[Visualization — Metabase / BI tool]
```

DAG dependency (Airflow) — `elnino_etl_pipeline`:
```
check_connections
  >> ingest_open_meteo      (dynamic mapping: 2 dataset x 15 lokasi)
  >> build_bronze           (JSON MinIO -> DuckDB bronze)
  >> build_silver           (dim + fact)
  >> build_gold             (gold.*)
  >> run_analytic_queries   (contoh insight)
```

---

## Langkah 6 — Build Ingestion Engine (Airflow DAG)

**Tujuan:** Mengambil data dari API secara otomatis.

- Peserta menggunakan DAG `elnino_etl_pipeline` + modul `elnino_common.py` di folder `dags/`.
- Ingestion memakai **Dynamic Task Mapping**: di-expand menjadi `2 dataset × 15 lokasi = 30 task paralel`.
- Dataset: `forecast` (14 hari ke depan), `archive` (1 tahun ke belakang). *(Peserta bisa menambah `air_quality` / `flood` secara mandiri.)*
- 15 lokasi Indonesia:
  `jakarta, surabaya, bandung, semarang, yogyakarta, medan, palembang, pekanbaru, palangkaraya, samarinda, makassar, manado, jayapura, kupang, mataram`
- Tiap task fetch Open-Meteo lalu tulis JSON ke MinIO:
  `raw/open-meteo/{dataset}/location={location}/ingestion_date=YYYY-MM-DD/{timestamp}.json`

Catatan: Di stack lokal, ingestion menulis JSON harian yang sudah di-flatten (1 row per tanggal) agar Bronze mudah dibaca DuckDB.

---

## Langkah 7 — Implement Transform Logic

**Tujuan:** Mengolah data menjadi insight.

Transform diimplementasikan sebagai SQL DuckDB di `elnino_common.py` (konstanta `SILVER_SQL` / `GOLD_SQL`). Metric yang dihitung:

### Drought Index (drought_risk_score, 0–100)
```
drought_risk_score =
    low_rainfall_score (precipitation_30d) +
    high_temperature_score +
    low_soil_moisture_score +
    high_evapotranspiration_score (evapotranspiration_30d)
```
Disimpan di `fact_drought_risk_daily` dengan level `low / medium / high / extreme`.

### Fire Weather Index / karhutla (fire_risk_score, 0–100)
```
fire_risk_score =
    0.4 * drought_risk_score +
    high_temperature_score +
    low_humidity_score +
    high_wind_score +
    peatland_bonus (5 jika peatland_flag = true)
```
Disimpan di `fact_fire_risk_daily`.

### Crop Stress Score
Dihitung di `fact_weather_daily` (`crop_stress_score`) dari kombinasi suhu tinggi + kelembapan tanah rendah + evapotranspiration tinggi. Masuk ke prioritas mitigasi:
`mitigation_priority_score = greatest(drought_risk_score, fire_risk_score)`.

---

## Langkah 8 — Load Data ke Data Warehouse (DuckDB)

**Tujuan:** Menyimpan hasil transformasi.

- **Bronze** (`build_bronze`): baca semua JSON MinIO untuk `ingestion_date` lalu buat `bronze.open_meteo_daily` (DuckDB table).
- **Silver** (`build_silver`): jalankan `SILVER_SQL` → `dim_date`, `dim_location`, `fact_weather_daily`, `fact_drought_risk_daily`, `fact_fire_risk_daily`, `fact_mitigation_daily`.
- **Gold** (`build_gold`): jalankan `GOLD_SQL` → `gold.drought_risk_by_region`, `gold.el_nino_weather_trend`, `gold.karhutla_risk_by_region`, `gold.mitigation_priority_daily`.
- Task `run_analytic_queries` mencetak contoh insight & row count per layer (validasi).

---

## Langkah 9 — Menjalankan Pipeline End-to-End

**Tujuan:** Menjalankan workflow lengkap.

1. Pastikan stack lokal (Airflow + MinIO) dari repo `e2e-docker-adventureworks-airflow-minio-duckdb` sudah jalan:
   ```bash
   # terminal 1 — Airflow
   cd airflow && docker compose up -d airflow-webserver airflow-scheduler
   # terminal 2 — MinIO
   docker compose -f docker-compose-minio.yaml up -d
   ```
2. Rebuild image Airflow sekali (karena kita menambahkan `requests` ke `requirements.txt`):
   ```bash
   docker compose build airflow-webserver airflow-scheduler
   docker compose up -d airflow-webserver airflow-scheduler
   ```
3. Set Airflow Variables MinIO (sama seperti DAG AdventureWorks):
   ```bash
   docker compose exec airflow-webserver airflow variables set MINIO_ENDPOINT "elt_minio:9000"
   docker compose exec airflow-webserver airflow variables set MINIO_ACCESS_KEY "minioadmin"
   docker compose exec airflow-webserver airflow variables set MINIO_SECRET_KEY "minioadmin123"
   docker compose exec airflow-webserver airflow variables set MINIO_BUCKET "adventureworks-elt"
   ```
4. Buka UIs:
   - Airflow: http://localhost:8080 (`airflow` / `airflow`)
   - MinIO Console: http://localhost:9001 (`minioadmin` / `minioadmin123`)
5. Trigger DAG `elnino_etl_pipeline` di Airflow, monitor task & log.

Cek hasil di DuckDB (via container Airflow):
```bash
docker compose exec airflow-webserver python -c "
import duckdb
c = duckdb.connect('/opt/airflow/dags/repo/elnino.duckdb')
for t in ['bronze.open_meteo_daily','silver.fact_weather_daily','silver.fact_drought_risk_daily','silver.fact_fire_risk_daily','gold.karhutla_risk_by_region']:
    print(t, c.execute('SELECT COUNT(*) FROM '+t).fetchone()[0])
"
```

---

## Langkah 10 — Analisis Insight dari Data

**Tujuan:** Menghasilkan insight bisnis.

Peserta menganalisis hasil query, mis.:

**1. Wilayah dengan risiko kekeringan tertinggi**
```sql
select l.province, l.location_name,
       avg(f.drought_risk_score) as avg_drought_risk,
       max(f.drought_risk_score) as peak_drought_risk
from fact_drought_risk_daily f
join dim_location l on f.location_key = l.location_key
join dim_date d on f.date_key = d.date_key
where d.full_date between date '2026-06-01' and date '2026-09-30'
group by l.province, l.location_name
order by peak_drought_risk desc;
```

**2. Pola curah hujan & suhu saat El Niño**
```sql
select d.full_date, l.province,
       avg(f.precipitation_sum_mm) as avg_rainfall_mm,
       avg(f.temperature_2m_mean_c) as avg_temperature_c
from fact_weather_daily f
join dim_location l on f.location_key = l.location_key
join dim_date d on f.date_key = d.date_key
where d.climate_event = 'El Nino'
group by d.full_date, l.province
order by d.full_date, l.province;
```

**3. Potensi karhutla tertinggi**
```sql
select l.province, l.location_name,
       max(f.fire_risk_score) as peak_fire_risk
from fact_fire_risk_daily f
join dim_location l on f.location_key = l.location_key
join dim_date d on f.date_key = d.date_key
where d.full_date between date '2026-06-01' and date '2026-10-31'
group by l.province, l.location_name
order by peak_fire_risk desc;
```

**4. Periode mitigasi terbaik**
```sql
select d.full_date, l.province, l.location_name,
       f.mitigation_priority_score, f.recommended_action
from fact_mitigation_daily f
join dim_location l on f.location_key = l.location_key
join dim_date d on f.date_key = d.date_key
where f.mitigation_priority_score >= 75
order by d.full_date, f.mitigation_priority_score desc;
```

Insight yang dihasilkan:
- Risiko kekeringan per provinsi/lokasi
- Potensi karhutla (terutama area `peatland_flag = true`)
- Cara data digunakan untuk decision making (alokasi air, patroli api, peringatan dini)

---

## Referensi & Resource

- Repo solusi asli (Postgres): https://github.com/rydteguh/public-training/tree/public-training/etl-case-study
- Data model asli: `docs/data_model.md`, `sql/weather_risk_schema.sql`
- Stack lokal peserta: `e2e-docker-adventureworks-airflow-minio-duckdb/airflow` (Airflow + MinIO + DuckDB)
- DAG case study: `dags/elnino_etl_pipeline.py`
- Modul & logika: `dags/elnino_common.py` (config lokasi, fetch Open-Meteo, SQL Silver/Gold)
- Konvensi MinIO (Airflow Variables `MINIO_*`) & DuckDB (`dags/repo/elnino.duckdb`) mengikuti DAG AdventureWorks yang sudah jalan.
