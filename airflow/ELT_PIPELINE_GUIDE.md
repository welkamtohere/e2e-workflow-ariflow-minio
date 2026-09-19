# 🚀 Panduan Setup ELT Pipeline — MinIO + DuckDB

Panduan langkah demi langkah untuk menjalankan stack ELT pipeline dari
[github.com/bangkit-pambudi/elt_pipeline](https://github.com/bangkit-pambudi/elt_pipeline).

---

## 📋 Daftar Isi

1. [Prerequisites](#1-prerequisites)
2. [Clone & Struktur Project](#2-clone--struktur-project)
3. [Setup MinIO (Object Storage)](#3-setup-minio-object-storage)
4. [Setup DuckDB + DBeaver (Database GUI)](#4-setup-duckdb--dbeaver-database-gui)
5. [Setup Airflow + DAG Ingestion](#5-setup-airflow--dag-ingestion)
   - [5.10 Bonus: Dynamic Task Mapping DAG](#510-bonus-dynamic-task-mapping-dag-)
   - [5.11 Dynamic Task Mapping untuk Ingestion](#511-dynamic-task-mapping-untuk-ingestion-)
   - [5.12 XCom: Komunikasi Antar Task](#512-xcom-komunikasi-antar-task-)
   - [5.13 Ingestion + Validasi dengan XCom](#513-ingestion--validasi-dengan-xcom-)
   - [5.14 Transform DuckDB: Bronze → Silver → Gold](#514-transform-duckdb-bronze--silver--gold-)
6. [Troubleshooting](#6-troubleshooting)
7. [Referensi Cepat](#7-referensi-cepat)

---

## 1. Prerequisites

Pastikan tools berikut sudah terinstall sebelum memulai.

| Tools | Keterangan | Download |
|-------|-----------|----------|
| **Docker Desktop** | Menjalankan container MinIO | [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/) |
| **DBeaver Community** | GUI untuk koneksi ke DuckDB | [dbeaver.io/download](https://dbeaver.io/download/) |
| **Browser modern** | Akses MinIO Console | Chrome / Firefox / Edge (sudah ada) |

> 💡 **Tips**: Setelah install Docker Desktop, pastikan sudah running (ada icon Docker di taskbar/status bar).

---

## 2. Clone & Struktur Project

Buka **terminal / command prompt**, lalu jalankan:

```bash
# Clone repo
git clone https://github.com/bangkit-pambudi/elt_pipeline.git

# Masuk ke folder project
cd elt_pipeline
```

Pastikan struktur foldernya seperti ini:

```
📁 elt_pipeline/
└── 📄 docker-compose.yml     ← file utama (MinIO + init)
```

> 📄 Hanya ada 1 file — simpel! Semua konfigurasi MinIO, bucket, dan volume sudah diatur di sini.

---

## 3. Setup MinIO (Object Storage)

MinIO adalah **penyimpanan object storage** (mirip Amazon S3) yang akan menampung file Parquet/CSV dari AdventureWorks sebelum diolah DuckDB.

### 3.1 Jalankan Container

```bash
# Dari folder elt_pipeline/
docker compose up -d
```

Tunggu **10–15 detik** sampai container siap. Cek statusnya:

```bash
docker ps --filter name=elt_minio
```

**Hasil yang diharapkan:**
```
CONTAINER ID   IMAGE         ...   STATUS                  NAMES
abc12345       minio/minio   ...   Up X minutes (healthy)   elt_minio
```

Perhatikan status harus **`(healthy)`** — bukan `starting` atau `unhealthy`.

> 🔍 Ada 2 container yang berjalan:
> - `elt_minio` — server MinIO utama
> - `elt_minio_init` — cuma jalan sekali untuk bikin bucket, lalu mati

### 3.2 Akses MinIO Console

Buka browser dan akses:

```
http://localhost:9001
```

**Login dengan kredensial berikut:**

| Field | Value |
|-------|-------|
| Username | `minioadmin` |
| Password | `minioadmin123` |

### 3.3 Verifikasi Bucket

Setelah berhasil login:

1. Klik menu **Buckets** di sidebar kiri
2. Pastikan bucket **`adventureworks-elt`** sudah muncul
3. Klik bucket tersebut
4. Klik tombol **Upload** → pilih file `.csv` kecil untuk test
5. Setelah upload, klik file dan coba **Download**

> ✅ **Tanda MinIO Berhasil**
>
> - [ ] Bucket `adventureworks-elt` terlihat di dashboard
> - [ ] Upload & download file berhasil tanpa error
> - [ ] Status container `elt_minio` adalah **healthy**

### 3.4 Test via CLI (Opsional)

Alternatif pakai MinIO Client dari dalam container:

```bash
# Masuk ke container minio-init
docker exec -it elt_minio_init sh

# Di dalam container:
mc ls local/

# Upload file test
echo 'id,name' > test.csv
mc cp test.csv local/adventureworks-elt/

# Verifikasi
mc ls local/adventureworks-elt/

# Keluar dari container
exit
```

---

## 4. Setup DuckDB + DBeaver (Database GUI)

DuckDB adalah database analitik yang akan memproses data dari MinIO. Kita akses via DBeaver (GUI gratis).

### 4.1 Install DBeaver

1. Buka [dbeaver.io/download](https://dbeaver.io/download/)
2. Pilih **Community Edition** sesuai OS kamu (Windows / macOS / Linux)
3. Install seperti biasa, lalu buka DBeaver

### 4.2 Install Driver DuckDB di DBeaver

DBeaver butuh driver JDBC khusus untuk bisa terhubung ke DuckDB.

**Langkah-langkah:**

1. Klik menu **Database** → **Driver Manager**

   ![visual](https://img.shields.io/badge/step-1-blue)

2. Klik tombol **New** (pojok kanan atas)

3. Isi field berikut di tab **Settings**:

   | Field | Value |
   |-------|-------|
   | Driver Name | `DuckDB` |
   | Class Name | `org.duckdb.DuckDBDriver` |
   | URL Template | `jdbc:duckdb:{file}` |

4. Klik tab **Libraries** → klik **Add Artifact**

5. Paste koordinat Maven ini:
   ```
   org.duckdb:duckdb_jdbc:0.10.3
   ```

6. Klik **Find Class** — pastikan muncul `org.duckdb.DuckDBDriver` di hasil pencarian

7. Klik **OK** untuk menyimpan driver

### 4.3 Buat Koneksi DuckDB

1. Klik **Database** → **New Database Connection**
2. Cari dan pilih **DuckDB** dari daftar → klik **Next**
3. Klik tombol **Create** di samping field path
4. Pilih folder untuk menyimpan file database (misalnya `~/elt_pipeline/`)
5. Beri nama file, contoh: **`sales_performance.duckdb`**
6. Klik **Test Connection**
7. Jika muncul pesan **"Connected"** → klik **Finish**

**Verifikasi koneksi berhasil:**

```sql
SELECT version();
```

Jalankan query di atas — seharusnya mengembalikan versi DuckDB.

> ✅ **Tanda DuckDB Berhasil**
>
> - [ ] Test Connection menampilkan **Connected**
> - [ ] Koneksi DuckDB muncul di panel kiri DBeaver
> - [ ] Query `SELECT version();` berhasil & mengembalikan hasil

### 4.4 Hubungkan DuckDB ke MinIO (httpfs + S3)

Agar DuckDB bisa membaca/menulis file Parquet/CSV dari MinIO, aktifkan ekstensi **httpfs** dan konfigurasi koneksi S3.

Di DBeaver, buka **SQL Editor** (klik kanan koneksi DuckDB → **SQL Editor → New SQL Editor**), lalu jalankan perintah berikut **satu per satu**:

```sql
-- 1. Aktifkan ekstensi httpfs
INSTALL httpfs;
LOAD httpfs;

-- 2. Konfigurasi koneksi ke MinIO
SET s3_endpoint = 'localhost:9000';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin123';
SET s3_use_ssl = false;
SET s3_url_style = 'path';
```

> ⚠️ **Catatan penting:**
> - `INSTALL httpfs` cukup dijalankan **sekali seumur hidup** — ekstensi akan tersimpan permanen
> - `LOAD httpfs` dan `SET ...` harus dijalankan **setiap kali** buka koneksi baru (atau simpan ke `.duckdbrc`)
> - `s3_endpoint` pakai `localhost:9000` karena DuckDB dan MinIO berjalan di mesin yang sama

**Verifikasi koneksi S3:**

```sql
-- List file di bucket adventureworks-elt (via S3 API)
SELECT * FROM read_parquet('s3://adventureworks-elt/*.parquet');
```

Atau jika belum ada file Parquet, coba test sederhana:

```sql
-- Tulis file test dari DuckDB ke MinIO
COPY (SELECT 1 AS id, 'test' AS name) TO 's3://adventureworks-elt/test_duckdb.parquet';

-- Baca kembali untuk verifikasi
SELECT * FROM read_parquet('s3://adventureworks-elt/test_duckdb.parquet');

-- Hapus file test (kalau mau)
-- SET s3_endpoint = 'localhost:9000';
-- SELECT s3_delete('adventureworks-elt', 'test_duckdb.parquet');
```

> ✅ **Tanda Koneksi S3 Berhasil**
> - Query `read_parquet` berjalan tanpa error
> - File hasil `COPY ... TO 's3://...'` muncul di MinIO Console (bucket `adventureworks-elt`)

### 4.5 Sanity Check — Apakah DuckDB + S3 Sudah Siap?

Jalankan query berikut di DBeaver untuk memastikan **DuckDB, httpfs, dan koneksi S3 ke MinIO** semuanya berfungsi dengan baik.

#### 4.5.1 Setup Data Sample

```sql
LOAD httpfs;
SET s3_endpoint = 'localhost:9000';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin123';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

-- Buat sample data
CREATE OR REPLACE TABLE raw_sales AS
SELECT * FROM (VALUES
    (1, 'Laptop', 15000000, 'Electronics', '2024-01-15'),
    (2, 'Mouse', 250000, 'Electronics', '2024-01-16'),
    (3, 'Keyboard', 500000, 'Electronics', '2024-01-17'),
    (4, 'Monitor', 3500000, 'Electronics', '2024-01-20'),
    (5, 'Buku', 75000, 'Education', '2024-02-01'),
    (6, 'Pulpen', 15000, 'Education', '2024-02-03')
) AS t(id, product, price, category, sale_date);

-- Export ke MinIO sebagai Parquet
COPY raw_sales TO 's3://adventureworks-elt/raw_sales.parquet';
```

**Hasil yang diharapkan:** Tidak ada error — data berhasil terupload ke MinIO.

#### 4.5.2 Verifikasi — Baca & Transformasi

Masih di sesi yang sama, jalankan:

```sql
-- Baca data dari MinIO dan lakukan agregasi
SELECT category, 
       count(*) AS total_transaksi, 
       sum(price) AS total_omzet,
       round(avg(price), 0) AS avg_harga
FROM read_parquet('s3://adventureworks-elt/raw_sales.parquet')
GROUP BY category
ORDER BY total_omzet DESC;
```

**Hasil yang diharapkan:**

| category | total_transaksi | total_omzet | avg_harga |
|----------|----------------|-------------|-----------|
| Electronics | 4 | 19250000 | 4812500.0 |
| Education | 2 | 90000 | 45000.0 |

#### 4.5.3 Simpan Hasil Transformasi ke DuckDB

```sql
-- Simpan hasil transformasi sebagai tabel lokal
CREATE OR REPLACE TABLE sales_summary AS
SELECT category, 
       count(*) AS total_transaksi, 
       sum(price) AS total_omzet
FROM read_parquet('s3://adventureworks-elt/raw_sales.parquet')
GROUP BY category;

-- Cek tabel
SELECT * FROM sales_summary;
```

> ✅ **Sanity Check Lulus** jika semua query di atas berjalan tanpa error.
>
> Sekarang DuckDB + MinIO siap digunakan untuk ELT pipeline sesungguhnya! 🚀

---

## 5. Setup Airflow + DAG Ingestion

Bagian ini akan menyambungkan **Airflow** ke MinIO dan PostgreSQL AdventureWorks, lalu menjalankan DAG ingestion `dag_single_ingestion_sales` yang mengekstrak data dari PostgreSQL ke MinIO dalam format Parquet.

### 5.1 Prasyarat

Pastikan folder project `airflow_docker` sudah ada:

```
📁 airflow_docker/
├── 📄 docker-compose.yaml
├── 📄 Dockerfile
├── 📄 requirements.txt
├── 📁 dags/repo/        ← tempat DAG files
└── 📁 logs/
```

Clone jika belum:
```bash
git clone https://github.com/bangkit-pambudi/airflow_docker.git
cd airflow_docker
```

### 5.2 Tambahkan Library ke requirements.txt

DAG `dag_single_ingestion_sales` membutuhkan `minio` dan `pyarrow`. Kita juga tambahkan `duckdb` untuk transformasi nanti.

Edit `requirements.txt` dan tambahkan di baris paling akhir:

```
duckdb
minio
pyarrow
apache-airflow-providers-postgres
```

### 5.3 Build Docker Image

```bash
# Dari folder airflow_docker/
podman build -t airflow_image .
```

> Image ini berbasis `apache/airflow:2.9.3` dan akan menginstall semua library dari `requirements.txt` (termasuk duckdb, minio, pyarrow).

### 5.4 Jalankan Airflow Stack

```bash
# Inisialisasi database (cukup sekali)
podman compose up airflow-init

# Jalankan webserver & scheduler
podman compose up -d airflow-webserver airflow-scheduler
```

**Cek status container:**
```bash
podman ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Hasil yang diharapkan:
| Container | Status | Port |
|-----------|--------|------|
| `airflow_docker-postgres-1` | Up | 5432 |
| `airflow_webserver` | Up | 8080 |
| `airflow_scheduler` | Up | |

Akses Airflow UI: [http://localhost:8080](http://localhost:8080)
- Username: `airflow`
- Password: `airflow`

> ⚠️ **Penting — Networking**: Container Airflow dan MinIO harus berada di **network yang sama** agar bisa berkomunikasi. Jangan gunakan `host.docker.internal` — itu tidak portable. Gunakan **container name** dan **shared network**.

### 5.5 Setup Shared Network

Agar Airflow bisa mengakses MinIO, kedua stack harus berada di network Docker yang sama.

#### 5.5.1 Update Docker Compose MinIO

Pastikan `elt_pipeline/docker-compose.yml` sudah ada definisi network:

```yaml
networks:
  elt-net:
    driver: bridge
```

#### 5.5.2 Update Docker Compose Airflow

Di `airflow_docker/docker-compose.yaml`, tambahkan network MinIO sebagai **external network**:

```yaml
networks:
  airflow-net:
    driver: bridge
  elt-net:
    external: true
    name: elt_pipeline_elt-net
```

Lalu tambahkan `elt-net` ke semua service Airflow:

```yaml
x-airflow-common:
  &airflow-common
  ...
  networks:
    - airflow-net
    - elt-net
```

> Dengan ini, container Airflow bisa mengakses MinIO via container name `elt_minio`.

#### 5.5.3 Recreate Kedua Stack

```bash
# Recreate MinIO dengan network baru
cd elt_pipeline
podman compose down
podman compose up -d

# Recreate Airflow dengan network baru
cd ../airflow_docker
podman compose up -d
```

### 5.6 Set MinIO Variables di Airflow

DAG membaca konfigurasi MinIO dari **Airflow Variables**. Gunakan **container name** `elt_minio` sebagai endpoint:

```bash
podman compose exec airflow-webserver airflow variables set MINIO_ENDPOINT "elt_minio:9000"
podman compose exec airflow-webserver airflow variables set MINIO_ACCESS_KEY "minioadmin"
podman compose exec airflow-webserver airflow variables set MINIO_SECRET_KEY "minioadmin123"
podman compose exec airflow-webserver airflow variables set MINIO_BUCKET "adventureworks-elt"
```

> DAG secara otomatis meresolve `elt_minio:9000` ke IP address-nya di runtime. Ini portable — tidak perlu hardcode IP.

### 5.7 Setup PostgreSQL AdventureWorks

```bash
# Jalankan PostgreSQL dengan AdventureWorks
podman run -d --name adventureworks-pg \
  -p 5433:5432 \
  -e POSTGRES_PASSWORD=My_password1 \
  docker.io/chriseaton/adventureworks:postgres

# Hubungkan ke network yang sama dengan Airflow & MinIO
podman network connect elt_pipeline_elt-net adventureworks-pg
```

Verifikasi tabel:
```bash
podman exec adventureworks-pg psql -U postgres -c "
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('Sales', 'Production')
ORDER BY table_schema, table_name;"
```

### 5.8 Set Airflow Connection ke PostgreSQL

Cari IP PostgreSQL di shared network (IP berbeda tiap mesin — tidak perlu hardcode):

```bash
# Dapatkan IP PostgreSQL di shared network
PG_IP=$(podman inspect adventureworks-pg --format \
  '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' | \
  awk '{print $NF}')

echo "PG IP: $PG_IP"
```

Buat koneksi Airflow dengan IP tersebut:

```bash
# Hapus jika sudah ada
podman compose exec airflow-webserver airflow connections delete adventure_works

# Buat koneksi baru
podman compose exec airflow-webserver airflow connections add 'adventure_works' \
  --conn-type 'postgres' \
  --conn-login 'postgres' \
  --conn-password 'My_password1' \
  --conn-host "$PG_IP" \
  --conn-port '5432' \
  --conn-schema 'postgres'
```

Verifikasi:
```bash
podman compose exec airflow-webserver bash -c "
  PGPASSWORD=My_password1 psql \
    -h $PG_IP -p 5432 -U postgres \
    -c 'SELECT count(*) FROM \"Sales\".\"SalesOrderHeader\";'
"
```

Hasil: `31465` rows.

### 5.9 Restart Scheduler & Jalankan DAG

Restart scheduler agar DAG di-parse ulang dengan Variables yang baru:

```bash
podman restart airflow_scheduler
```

**Opsi A — via Airflow UI:**
1. Buka [http://localhost:8080](http://localhost:8080)
2. Login: `airflow` / `airflow`
3. Cari DAG **`dag_single_ingestion_sales`**
4. Klik tombol ▶ (Play) → **Trigger DAG**
5. Klik DAG → tab **Graph** untuk lihat progress

**Opsi B — via CLI:**
```bash
podman compose exec airflow-webserver airflow dags trigger dag_single_ingestion_sales
```

**Cek file di MinIO (via mc CLI):**
```bash
podman exec elt_minio sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin123 &&
  mc ls -r local/adventureworks-elt/raw/
"
```

Atau via **DuckDB**:
```bash
echo "
LOAD httpfs;
SET s3_endpoint = 'localhost:9000';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin123';
SET s3_use_ssl = false;
SET s3_url_style = 'path';
SELECT count(*) AS total_files FROM read_parquet('s3://adventureworks-elt/**/*.parquet');
" | duckdb /tmp/check.duckdb
```

Atau via **MinIO Console** di [http://localhost:9001](http://localhost:9001) → Bucket `adventureworks-elt`.

**Hasil yang diharapkan:**
```
[2026-06-20 23:49:52 UTC]  51KiB STANDARD product/dt=2026-06-18/data.parquet
[2026-06-20 23:49:52 UTC]  51KiB STANDARD product/dt=2026-06-19/data.parquet
[2026-06-20 23:49:52 UTC] 3.5KiB STANDARD product_category/dt=2026-06-18/data.parquet
... (12 file Parquet dari 6 tabel × 2 partisi)
```

> ✅ **DAG Berhasil** jika semua task berwarna hijau di UI Airflow dan file Parquet muncul di bucket `adventureworks-elt`.

### 5.10 Bonus: Dynamic Task Mapping DAG 🗺️

Airflow 2.3+ mendukung **Dynamic Task Mapping** — membuat task secara dinamis berdasarkan output task sebelumnya. Contoh ada di `4a-simple_dynamic_task_mapping.py`:

```python
# get_regions → mengembalikan list region
def get_regions_func():
    return [
        {"region": "US"},
        {"region": "EU"},
        {"region": "APAC"},
        {"region": "LATAM"}
    ]

# process_regional_data → di-expand jadi 4 task (1 per region)
task_process_data = PythonOperator.partial(
    task_id="process_regional_data",
    python_callable=process_regional_data_func,
    op_args=["global-sales-datalake", "parquet"]  # argumen STATIS
).expand(
    op_kwargs=task_get_regions.output  # argumen DINAMIS dari task sebelumnya
)
```

**Jalankan:**
```bash
podman compose exec airflow-webserver airflow dags trigger 4a-simple_dynamic_task_mapping
```

**Cek task di Airflow UI**:  
Buka DAG → tab **Grid** → task `process_regional_data` akan memiliki **4 map_index** (0=US, 1=EU, 2=APAC, 3=LATAM), masing-masing running paralel dengan argumen `region` berbeda.

> ⚠️ **Catatan**: DAG ini hanya contoh demonstrasi — tidak perlu MinIO/PostgreSQL. Cukup trigger dan lihat log tiap mapped task.

### 5.11 Dynamic Task Mapping untuk Ingestion 🗺️📦

DAG `dag_multiple_ingestion_sales` menggunakan teknik yang sama untuk **meng-ingest semua 6 tabel sekaligus secara paralel**:

```python
INGEST_TABLES = [
    {"schema": "Sales",      "table": "SalesOrderHeader",   "folder": "sales_order_header"},
    {"schema": "Sales",      "table": "SalesOrderDetail",   "folder": "sales_order_detail"},
    {"schema": "Sales",      "table": "SalesTerritory",     "folder": "sales_territory"},
    {"schema": "Production", "table": "Product",            "folder": "product"},
    {"schema": "Production", "table": "ProductSubcategory", "folder": "product_subcategory"},
    {"schema": "Production", "table": "ProductCategory",    "folder": "product_category"},
]

task_check = PythonOperator(...)       # verifikasi koneksi
task_ingest = PythonOperator.partial(
    task_id="ingest_table",
    python_callable=ingest_table,
).expand(
    op_kwargs=INGEST_TABLES             # 6 tabel → 6 task paralel
)

task_check >> task_ingest
```

**Cara kerja:**
1. `check_connections` → cek koneksi PostgreSQL + MinIO
2. `ingest_table` → di-expand jadi **6 task paralel** (1 per tabel)
3. Tiap task: `SELECT * FROM "Schema"."Table"` → upload Parquet ke MinIO

**Jalankan:**
```bash
podman compose exec airflow-webserver airflow dags trigger dag_multiple_ingestion_sales
```

**Cek file di MinIO:**
```bash
podman exec elt_minio mc ls -r local/adventureworks-elt/raw/
```

**Hasil yang diharapkan:**
```
raw/product/dt=2026-06-20/data.parquet
raw/product_category/dt=2026-06-20/data.parquet
raw/product_subcategory/dt=2026-06-20/data.parquet
raw/sales_order_detail/dt=2026-06-20/data.parquet
raw/sales_order_header/dt=2026-06-20/data.parquet
raw/sales_territory/dt=2026-06-20/data.parquet
... (6 tabel × partisi tanggal)
```

> ⚠️ **Catatan**: DAG ini membutuhkan koneksi ke PostgreSQL (`adventure_works`) dan MinIO (`MINIO_*` variables) yang sudah di-set di langkah 5.6–5.8.

### 5.12 XCom: Komunikasi Antar Task 🔄

**XCom** (Cross-Communication) adalah mekanisme Airflow untuk bertukar data kecil antar task dalam satu DAG. Contoh di `4b-simple-xcom.py`:

```python
def push_function(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(key='my_message', value="Ini pesan rahasia")

def pull_function(**kwargs):
    ti = kwargs['ti']
    pulled_data = ti.xcom_pull(task_ids='push_task', key='my_message')
    print(f"Pesan yang diterima: {pulled_data}")

with DAG('4b-simple-xcom', ...) as dag:
    push_task = PythonOperator(task_id='push_task', python_callable=push_function)
    pull_task = PythonOperator(task_id='pull_task', python_callable=pull_function)
    push_task >> pull_task
```

**Cara kerja:**
1. `push_task` → menyimpan string `"Ini pesan rahasia"` ke XCom dengan key `my_message`
2. `pull_task` → mengambil data dari XCom milik `push_task` menggunakan key yang sama
3. Data dilewatkan via database Airflow (backend), **bukan** via Python in-memory

**Jalankan:**
```bash
podman compose exec airflow-webserver airflow dags trigger 4b-simple-xcom
```

**Cek hasil di task log:**
```bash
podman exec airflow_webserver cat \
  /opt/airflow/logs/dag_id=4b-simple-xcom/run_id=manual__*/task_id=pull_task/attempt=1.log \
  | grep "Pesan"
# Output: Pesan yang diterima: Ini pesan rahasia
```

> ⚠️ **Catatan**: XCom cocok untuk data kecil (< 1 MB). Untuk data besar (DataFrame, file), simpan di storage eksternal (MinIO, S3) dan operasikan path-nya via XCom.

### 5.13 Ingestion + Validasi dengan XCom ✅📊

DAG `elt_01_ingestion` menggabungkan dynamic task mapping + XCom untuk **ingest 6 tabel lalu memvalidasi hasilnya**:

```python
# Task 1: check koneksi
task_check = PythonOperator(task_id="check_connections", ...)

# Task 2: ingest paralel — setiap mapped task return dict hasil
task_ingest = PythonOperator.partial(
    task_id="ingest_tables",
    python_callable=ingest_table,
).expand(op_kwargs=INGEST_TABLES)

# Task 3: validasi — pull hasil dari XCom
task_validate = PythonOperator(
    task_id="validate_ingestion",
    python_callable=validate_ingestion,
)

task_check >> task_ingest >> task_validate
```

**Cara kerja XCom di DAG ini:**
1. `ingest_tables` (6 mapped task) masing-masing `return {"table": ..., "path": ..., "rows": ...}`
2. Airflow otomatis menyimpan return value tiap mapped task ke XCom
3. `validate_ingestion` mengambil **semua hasil** via `ti.xcom_pull(task_ids="ingest_tables")` → dapat **list of 6 dicts**
4. Validasi membaca schema Parquet dari MinIO untuk tiap file → cetak summary

**Jalankan:**
```bash
podman compose exec airflow-webserver airflow dags trigger elt_01_ingestion
```

**Cek log validasi:**
```bash
podman exec airflow_webserver cat \
  /opt/airflow/logs/dag_id=elt_01_ingestion/run_id=*/task_id=validate_ingestion/attempt=1.log \
  | grep -E "✅|🎉"
```

**Hasil yang diharapkan:**
```
✅ SalesOrderHeader: 31,465 rows | 26 cols | raw/sales_order_header/dt=.../data.parquet
✅ SalesOrderDetail: 121,317 rows | 11 cols | raw/sales_order_detail/dt=.../data.parquet
✅ SalesTerritory: 10 rows | 10 cols | raw/sales_territory/dt=.../data.parquet
✅ Product: 504 rows | 25 cols | raw/product/dt=.../data.parquet
✅ ProductSubcategory: 37 rows | 5 cols | raw/product_subcategory/dt=.../data.parquet
✅ ProductCategory: 4 rows | 4 cols | raw/product_category/dt=.../data.parquet
🎉 Semua tabel berhasil diingest ke MinIO!
```

### 5.14 Transform DuckDB: Bronze → Silver → Gold 🦆🏗️

DAG `elt_02_transform_duckdb` membaca Parquet dari MinIO (hasil DAG 1) dan mentransformasikannya di DuckDB menjadi 3 layer:

```
bronze.*  → VIEW langsung ke file Parquet di MinIO (zero-copy)
silver.*  → stg_sales_orders (JOIN header+detail+territory)
             stg_products (JOIN product+subcategory+category)
             trf_sales_summary (agregasi + margin per produk/territory)
gold.*    → fact_sales_performance (final, siap BI)
```

**Pipeline:**
```python
task_setup >> task_bronze >> task_silver >> task_gold
```

**Cara kerja:**
1. `setup_duckdb_s3` — buat schema bronze/silver/gold, verifikasi akses S3 ke MinIO
2. `create_bronze_layer` — buat VIEW yang baca langsung dari `read_parquet('s3://...')`
3. `create_silver_layer` — buat tabel fisik hasil JOIN + agregasi + kalkulasi margin
4. `create_gold_layer` — buat `fact_sales_performance` final dengan metadata audit

**Jalankan setelah DAG 1 selesai:**
```bash
podman compose exec airflow-webserver airflow dags trigger elt_02_transform_duckdb
```

**Cek hasil tiap layer:**
```bash
podman exec airflow_webserver sh -c '
for task in setup_duckdb_s3 create_bronze_layer create_silver_layer create_gold_layer; do
  echo "=== $task ==="
  cat /opt/airflow/logs/dag_id=elt_02_transform_duckdb/run_id=*/task_id=$task/attempt=1.log \
    | grep -E "✅|🎉|rows"
done'
```

**Hasil yang diharapkan:**
```
=== setup_duckdb_s3 ===
✅ sales_order_header: 31,465 rows accessible via S3
✅ sales_order_detail: 121,317 rows accessible via S3
✅ product: 504 rows accessible via S3

=== create_bronze_layer ===
✅ bronze.sales_order_header: 31,465 rows
✅ bronze.sales_order_detail: 121,317 rows

=== create_silver_layer ===
✅ silver.stg_sales_orders: 121,317 rows
✅ silver.stg_products: 304 rows
✅ silver.trf_sales_summary: 2,446 rows

=== create_gold_layer ===
✅ gold.fact_sales_performance: 2,446 rows
```

> ⚠️ DuckDB hanya mengizinkan **satu koneksi writer** dalam satu waktu. Jangan trigger DAG ini bersamaan dengan DAG lain yang juga pakai DuckDB file yang sama.

---

## 6. Troubleshooting

| Masalah | Solusi |
|---------|--------|
| **MinIO Console tidak bisa dibuka** | Cek apakah port 9001 bentrok: `docker ps \| grep 9001`. Kalau bentrok, stop container lain yang pakai port itu. |
| **Bucket tidak muncul otomatis** | Jalankan ulang container init: `docker restart elt_minio_init` |
| **DBeaver gagal Test Connection** | Pastikan path file `.duckdb` benar dan bisa ditulis. |
| **Port 9000 sudah dipakai** | Edit `docker-compose.yml`, ganti `9000:9000` jadi `9002:9000` (atau port bebas lain). |
| **Container `elt_minio_init` error** | Jalankan `docker logs elt_minio_init` untuk lihat pesan error detail. |
| **Docker compose tidak ditemukan** | Pastikan Docker Desktop sudah terinstall dan running. Coba `docker compose version` untuk verifikasi. |
| **`LOAD httpfs;` error — extension tidak ditemukan** | Jalankan `INSTALL httpfs;` terlebih dahulu (cukup sekali) untuk mendownload extension, baru `LOAD httpfs;` |
| **Airflow DAG tidak muncul di UI** | Restart scheduler: `podman restart airflow_scheduler`. Pastikan Airflow Variables sudah di-set. |
| **DAG error: Variable MINIO_* does not exist** | Set variabel dulu (lihat langkah 5.6), lalu restart scheduler. |
| **DAG error: Connection adventure_works not found** | Set koneksi dulu (lihat langkah 5.8). |
| **PostgreSQL connection refused** | Pastikan container `adventureworks-pg` running: `podman ps \| grep adventureworks-pg`. Pastikan sudah di-connect ke shared network. |
| **DAG stuck di "queued" selamanya** | Scheduler mungkin hang akibat fork+thread deadlock di Python 3.12. Restart: `podman restart airflow_scheduler`. Kalau sering terjadi, ganti executor ke `SequentialExecutor` di `airflow.cfg`. |
| **DAG error: MinIO BadStatusLine / hostname issue** | MinIO Python SDK butuh path-style request. DAG sudah auto-resolve container name → IP via `socket.gethostbyname()`. Pastikan endpoint pakai container name (`elt_minio:9000`), bukan IP. |
| **DuckDB: "Could not set lock on file"** | DuckDB hanya izinkan 1 writer. Jangan trigger DAG yang sama 2× bersamaan. Hapus file lock: `podman exec airflow_webserver rm -f /opt/airflow/dags/repo/dwh.duckdb*` |

---

## 7. Referensi Cepat

| Service | URL / Akses |
|---------|-------------|
| 🌐 **Airflow UI** | [http://localhost:8080](http://localhost:8080) (`airflow` / `airflow`) |
| 🌐 **MinIO Console** | [http://localhost:9001](http://localhost:9001) |
| 🔌 **MinIO S3 API** | `http://localhost:9000` |
| 👤 **MinIO Username** | `minioadmin` |
| 🔑 **MinIO Password** | `minioadmin123` |
| 📦 **MinIO Bucket** | `adventureworks-elt` |
| 🦆 **DuckDB JDBC Driver** | `org.duckdb:duckdb_jdbc:0.10.3` |
| 🐘 **PostgreSQL AdventureWorks** | Container: `adventureworks-pg`, Password: `My_password1` |

---

### 🎉 Selamat!

Stack ELT pipeline kamu sudah siap. Data dari AdventureWorks nantinya akan:
1. **Diekstrak** → oleh **Airflow DAG** → disimpan sebagai Parquet di **MinIO** (bucket `adventureworks-elt`)
2. **Dimuat & ditransformasi** → oleh **DuckDB** (via koneksi DBeaver atau DAG transformasi)

Akses:
- 🌐 **Airflow UI**: [http://localhost:8080](http://localhost:8080) (`airflow` / `airflow`)
- 🌐 **MinIO Console**: [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin123`)
- 🦆 **DuckDB**: via DBeaver ke file `.duckdb` atau CLI

Siap untuk langkah selanjutnya? 🚀
