"""
DAG 2 — TRANSFORM (DuckDB)
MinIO (raw Parquet) → DuckDB [bronze → silver → gold]

Pertanyaan bisnis:
  "Siapa customer paling valuable kita, dan bagaimana kita bisa
   segmentasi mereka berdasarkan perilaku belanja?"

Arsitektur DuckDB:
  ┌─────────────────────────────────────────────────────┐
  │                   DuckDB                            │
  │                                                     │
  │  bronze.*   ← VIEW langsung ke Parquet di MinIO     │
  │     ↓                                               │
  │  silver.*   ← stg_* (bersih) dan trf_* (R/F/M)      │
  │     ↓                                               │
  │  gold.*     ← fact_customer_rfm (skor + segment)    │
  └─────────────────────────────────────────────────────┘
        ↓
  Export gold.fact_customer_rfm → MinIO Parquet

Pipeline:
  setup_duckdb_s3
        ↓
  create_bronze_layer      ← 5 VIEW ke Parquet di MinIO
        ↓
  create_silver_layer      ← stg_sales_orders, stg_customers,
                             stg_territories, trf_customer_rfm
        ↓
  create_gold_layer        ← skor R/F/M + Segment → fact_customer_rfm
        ↓
  export_gold_to_minio     ← simpan fact sebagai Parquet di MinIO

Query analitik untuk menjawab pertanyaan bisnis dijalankan terpisah
lewat SQL (lihat analytics_customer_rfm.sql), bukan di dalam DAG.

Dynamic Task Mapping + XCom ada di DAG 1 (ingestion).
"""

import os
import socket
import logging
from datetime import datetime, timedelta

import duckdb

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
AW_CONN_ID     = "adventure_works"
MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT")    # elt_minio:9000
MINIO_ACCESS   = Variable.get("MINIO_ACCESS_KEY")  # minioadmin
MINIO_SECRET   = Variable.get("MINIO_SECRET_KEY")  # minioadmin123
MINIO_BUCKET   = Variable.get("MINIO_BUCKET")      # adventureworks-elt
MINIO_SECURE   = False
DUCKDB_PATH    = "/opt/airflow/dags/repo/dwh.duckdb"

# S3 prefix untuk DuckDB (tanpa endpoint — endpoint di-resolve di runtime)
S3_PREFIX      = f"s3://{MINIO_BUCKET}"

# Ambang segment. RFM_Score = R + F + M, rentang 3..12.
# Urut dari yang paling tinggi; entri pertama yang cocok yang dipakai.
SEGMENT_RULES = [(10, "Champions"), (7, "Loyal"), (4, "At Risk")]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ── Helpers ───────────────────────────────────────────────────
def _resolve_endpoint(endpoint: str, port: int) -> str:
    host = endpoint.split(":")[0]
    try:
        ip = socket.gethostbyname(host)
        log.info(f"Resolved {host} → {ip}")
        return f"{ip}:{port}"
    except OSError:
        log.warning(f"Cannot resolve {host}, falling back to {endpoint}")
        return endpoint


def get_duckdb_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Buka koneksi DuckDB dan konfigurasi S3 extension agar bisa
    baca/tulis Parquet di MinIO.
    """
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    conn = duckdb.connect(DUCKDB_PATH, read_only=read_only)

    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    # Resolve container name → IP agar DuckDB httpfs pakai path-style
    s3_endpoint = _resolve_endpoint(MINIO_ENDPOINT, 9000)

    conn.execute(f"SET s3_endpoint='{s3_endpoint}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_ACCESS}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_SECRET}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")   # MinIO pakai path-style

    return conn


def get_latest_partition(folder: str, logical_date: str) -> str:
    """
    Path Parquet di MinIO untuk partisi tanggal tertentu.
    Format ini adalah KONTRAK dengan DAG 1 — jangan diubah sepihak.
    """
    return f"{S3_PREFIX}/raw/{folder}/dt={logical_date}/data.parquet"


def _segment_case(score_expr: str) -> str:
    """Bangun CASE WHEN segment dari SEGMENT_RULES."""
    whens = "\n".join(
        f"                WHEN {score_expr} >= {cut} THEN '{label}'"
        for cut, label in SEGMENT_RULES
    )
    return f"CASE\n{whens}\n                ELSE 'Lost'\n            END"


# ════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ════════════════════════════════════════════════════════════

def setup_duckdb_s3(**context):
    """
    Setup DuckDB: test koneksi S3/MinIO, buat schema bronze/silver/gold,
    verifikasi file Parquet dari DAG 1 bisa diakses.

    Verifikasi terakhir itu penting — dia gagal cepat kalau DAG 1
    ternyata belum selesai, alih-alih menghasilkan tabel kosong.
    """
    logical_date = context["ds"]
    conn = get_duckdb_conn()

    try:
        for schema in ("bronze", "silver", "gold"):
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            log.info(f"✅ Schema '{schema}' ready.")

        tables_to_check = [
            "sales_order_header",
            "sales_order_detail",
            "sales_territory",
            "customer",
            "person",
        ]

        for folder in tables_to_check:
            path = get_latest_partition(folder, logical_date)
            log.info(f"🔍 Verifying access to {path} ...")
            cnt = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{path}')"
            ).fetchone()[0]
            log.info(f"✅ {folder}: {cnt:,} rows accessible via S3")

        log.info("✅ DuckDB S3 setup complete.")
    finally:
        conn.close()


def create_bronze_layer(**context):
    """
    Bronze Layer: VIEW yang membaca langsung Parquet mentah dari MinIO.

    Prinsip bronze:
    - Data apa adanya dari source — NOL transformasi, NOL filter bisnis
    - Hanya VIEW, bukan tabel fisik (hemat storage, selalu fresh)
    - Kalau bronze dihapus kamu harus pukul Postgres lagi; kalau silver
      dihapus cukup hitung ulang dari bronze. Itu sebabnya filter
      bisnis tidak boleh turun ke sini.
    """
    logical_date = context["ds"]
    conn = get_duckdb_conn()

    try:
        bronze_views = [
            "sales_order_header",
            "sales_order_detail",
            "sales_territory",
            "customer",
            "person",
        ]

        for view_name in bronze_views:
            path = get_latest_partition(view_name, logical_date)

            conn.execute(f"DROP VIEW IF EXISTS bronze.{view_name};")
            conn.execute(f"""
                CREATE VIEW bronze.{view_name} AS
                SELECT * FROM read_parquet('{path}');
            """)

            cnt = conn.execute(f"SELECT COUNT(*) FROM bronze.{view_name}").fetchone()[0]
            log.info(f"✅ bronze.{view_name}: {cnt:,} rows (VIEW → {path})")

        log.info("✅ Bronze layer complete — semua VIEW siap.")
    finally:
        conn.close()


def create_silver_layer(**context):
    """
    Silver Layer:
    - silver.stg_sales_orders  : grain 1 ORDER  (header ⋈ detail, LineTotal diagregasi)
    - silver.stg_customers     : grain 1 CUSTOMER (customer ⋈ person)
    - silver.stg_territories   : grain 1 TERRITORY
    - silver.trf_customer_rfm  : grain 1 CUSTOMER — Recency/Frequency/Monetary MENTAH

    Prinsip silver: data sudah bisa dipercaya, tapi belum punya opini.
    Belum ada skor, belum ada label segment — itu urusan gold.
    """
    logical_date = context["ds"]
    conn = get_duckdb_conn()

    try:
        # ── stg_sales_orders — grain: 1 baris per ORDER ───────────────
        # Agregasi LineTotal dilakukan DI SINI supaya fan-out
        # header × detail tidak bocor ke perhitungan RFM.
        #
        # LineTotal di beberapa port AdventureWorks adalah computed
        # column yang tidak ikut ter-dump (isinya NULL). Karena itu
        # dipakai COALESCE ke rumus aslinya:
        #     OrderQty * UnitPrice * (1 - UnitPriceDiscount)
        log.info("Creating silver.stg_sales_orders ...")
        conn.execute("DROP TABLE IF EXISTS silver.stg_sales_orders;")
        conn.execute("""
            CREATE TABLE silver.stg_sales_orders AS
            SELECT
                soh."SalesOrderID",
                soh."CustomerID",
                soh."OrderDate"::DATE            AS "OrderDate",
                soh."TerritoryID",
                st."Name"                        AS "TerritoryName",
                st."CountryRegionCode",
                SUM(
                    COALESCE(
                        sod."LineTotal"::DOUBLE,
                        sod."OrderQty"::DOUBLE
                            * sod."UnitPrice"::DOUBLE
                            * (1 - COALESCE(sod."UnitPriceDiscount"::DOUBLE, 0))
                    )
                )                                AS "OrderAmount",
                COUNT(*)                         AS "LineCount"
            FROM bronze.sales_order_header AS soh
            INNER JOIN bronze.sales_order_detail AS sod
                ON soh."SalesOrderID" = sod."SalesOrderID"
            INNER JOIN bronze.sales_territory AS st
                ON soh."TerritoryID" = st."TerritoryID"
            -- Filter bisnis: hanya order yang sudah selesai (Status = 5)
            WHERE soh."Status" = 5
            GROUP BY 1, 2, 3, 4, 5, 6;
        """)
        cnt = conn.execute("SELECT COUNT(*) FROM silver.stg_sales_orders").fetchone()[0]
        log.info(f"✅ silver.stg_sales_orders: {cnt:,} rows")

        # Assertion grain — lebih baik gagal keras daripada salah diam-diam
        dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT "SalesOrderID" FROM silver.stg_sales_orders
                GROUP BY 1 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        if dupes:
            raise ValueError(f"stg_sales_orders punya {dupes} duplikat SalesOrderID")

        # Assertion nilai — kalau OrderAmount semuanya NULL, seluruh
        # perhitungan Monetary di bawah jadi sia-sia. Gagal di sini
        # jauh lebih mudah didiagnosis daripada di layer gold.
        null_amount = conn.execute("""
            SELECT COUNT(*) FROM silver.stg_sales_orders
            WHERE "OrderAmount" IS NULL
        """).fetchone()[0]
        if null_amount == cnt:
            raise ValueError(
                "Semua OrderAmount NULL — cek kolom LineTotal / UnitPrice "
                "di parquet hasil DAG 1."
            )
        if null_amount:
            log.warning(f"⚠️  {null_amount:,} order punya OrderAmount NULL")

        # ── stg_customers — grain: 1 baris per CUSTOMER ───────────────
        # LEFT JOIN, bukan INNER: di AdventureWorks mayoritas Customer
        # adalah toko dan PersonID-nya NULL. INNER JOIN akan membuang
        # mereka semua sebelum sempat dinilai.
        log.info("Creating silver.stg_customers ...")
        conn.execute("DROP TABLE IF EXISTS silver.stg_customers;")
        conn.execute("""
            CREATE TABLE silver.stg_customers AS
            SELECT
                c."CustomerID",
                c."PersonID",
                COALESCE(
                    NULLIF(TRIM(p."FirstName" || ' ' || p."LastName"), ''),
                    'Store Customer #' || c."CustomerID"
                )                                   AS "CustomerName",
                (p."BusinessEntityID" IS NOT NULL)  AS "IsIndividual"
            FROM bronze.customer AS c
            LEFT JOIN bronze.person AS p
                ON c."PersonID" = p."BusinessEntityID"
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY c."CustomerID" ORDER BY c."CustomerID"
            ) = 1;
        """)
        cnt = conn.execute("SELECT COUNT(*) FROM silver.stg_customers").fetchone()[0]
        log.info(f"✅ silver.stg_customers: {cnt:,} rows")

        # ── stg_territories — grain: 1 baris per TERRITORY ────────────
        log.info("Creating silver.stg_territories ...")
        conn.execute("DROP TABLE IF EXISTS silver.stg_territories;")
        conn.execute("""
            CREATE TABLE silver.stg_territories AS
            SELECT
                "TerritoryID",
                "Name"              AS "TerritoryName",
                "CountryRegionCode",
                "Group"             AS "RegionGroup"
            FROM bronze.sales_territory
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY "TerritoryID" ORDER BY "TerritoryID"
            ) = 1;
        """)
        cnt = conn.execute("SELECT COUNT(*) FROM silver.stg_territories").fetchone()[0]
        log.info(f"✅ silver.stg_territories: {cnt:,} rows")

        # ── trf_customer_rfm — grain: 1 baris per CUSTOMER ────────────
        # Recency dihitung dari `ds` (tanggal logis run), BUKAN NOW().
        # Dengan NOW() hasilnya berubah tiap kali di-run dan backfill
        # jadi tidak bermakna.
        log.info("Creating silver.trf_customer_rfm ...")
        conn.execute("DROP TABLE IF EXISTS silver.trf_customer_rfm;")
        conn.execute(f"""
            CREATE TABLE silver.trf_customer_rfm AS
            WITH agg AS (
                SELECT
                    so."CustomerID",
                    MIN(so."OrderDate")               AS "FirstPurchaseDate",
                    MAX(so."OrderDate")               AS "LastPurchaseDate",
                    COUNT(DISTINCT so."SalesOrderID") AS "Frequency",
                    SUM(so."OrderAmount")             AS "Monetary",
                    -- territory dari order TERAKHIR (customer bisa
                    -- berpindah wilayah antar-order)
                    ARG_MAX(so."TerritoryID", so."OrderDate") AS "TerritoryID"
                FROM silver.stg_sales_orders AS so
                GROUP BY 1
            )
            SELECT
                a."CustomerID",
                c."CustomerName",
                c."IsIndividual",
                a."TerritoryID",
                t."TerritoryName",
                a."FirstPurchaseDate",
                a."LastPurchaseDate",
                DATE_DIFF('day', a."LastPurchaseDate", DATE '{logical_date}')
                                                      AS "Recency",
                a."Frequency",
                ROUND(a."Monetary", 2)                AS "Monetary",
                -- atribut perilaku tambahan (bukan bagian skor)
                ROUND(a."Monetary" / a."Frequency", 2) AS "AvgOrderValue",
                DATE_DIFF('day', a."FirstPurchaseDate", a."LastPurchaseDate")
                                                      AS "TenureDays"
            FROM agg AS a
            LEFT JOIN silver.stg_customers   AS c ON a."CustomerID"  = c."CustomerID"
            LEFT JOIN silver.stg_territories AS t ON a."TerritoryID" = t."TerritoryID";
        """)
        cnt = conn.execute("SELECT COUNT(*) FROM silver.trf_customer_rfm").fetchone()[0]
        log.info(f"✅ silver.trf_customer_rfm: {cnt:,} rows")

        dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT "CustomerID" FROM silver.trf_customer_rfm
                GROUP BY 1 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        if dupes:
            raise ValueError(f"trf_customer_rfm punya {dupes} duplikat CustomerID")

        log.info("✅ Silver layer complete.")
    finally:
        conn.close()


def create_gold_layer(**context):
    """
    Gold Layer: fact table final dengan skor RFM dan label segment.

    Scoring pakai NTILE(4) atas SELURUH populasi customer:
      R_Score : makin baru belanja  → makin tinggi (ORDER BY Recency DESC)
      F_Score : makin sering        → makin tinggi (ORDER BY Frequency ASC)
      M_Score : makin besar nilainya → makin tinggi (ORDER BY Monetary ASC)

    Konvensi: 4 = terbaik, 1 = terburuk. Konsisten untuk ketiganya,
    supaya RFM_Score (jumlahnya, rentang 3..12) bisa langsung dibaca
    "makin besar makin bagus".

    Prinsip gold:
    - Menjawab satu pertanyaan bisnis spesifik
    - Ada audit trail (LoadTimestamp, BatchDate)
    - Grain jelas: 1 baris per CustomerID
    """
    logical_date = context["ds"]
    conn = get_duckdb_conn()

    total = '("R_Score" + "F_Score" + "M_Score")'

    try:
        log.info("Creating gold.fact_customer_rfm ...")
        conn.execute("DROP TABLE IF EXISTS gold.fact_customer_rfm;")
        conn.execute(f"""
            CREATE TABLE gold.fact_customer_rfm AS
            WITH scored AS (
                SELECT
                    *,
                    NTILE(4) OVER (ORDER BY "Recency"   DESC) AS "R_Score",
                    NTILE(4) OVER (ORDER BY "Frequency" ASC)  AS "F_Score",
                    NTILE(4) OVER (ORDER BY "Monetary"  ASC)  AS "M_Score"
                FROM silver.trf_customer_rfm
            )
            SELECT
                "CustomerID",
                "CustomerName",
                "IsIndividual",
                "TerritoryID",
                "TerritoryName",
                "FirstPurchaseDate",
                "LastPurchaseDate",
                "Recency",
                "Frequency",
                "Monetary",
                "AvgOrderValue",
                "TenureDays",
                "R_Score",
                "F_Score",
                "M_Score",
                {total} AS "RFM_Score",
                -- Pola mentah R-F-M. RFM_Score 8 bisa berarti 4-3-1
                -- (sering belanja nilai kecil) atau 1-3-4 (whale yang
                -- lama menghilang) — dua perilaku berlawanan, skor sama.
                CAST("R_Score" AS VARCHAR) || '-' ||
                CAST("F_Score" AS VARCHAR) || '-' ||
                CAST("M_Score" AS VARCHAR)              AS "RFM_Cell",
                {_segment_case(total)}                  AS "Segment",
                -- Metadata audit
                CURRENT_TIMESTAMP                       AS "LoadTimestamp",
                '{logical_date}'::DATE                  AS "BatchDate"
            FROM scored;
        """)

        cnt = conn.execute("SELECT COUNT(*) FROM gold.fact_customer_rfm").fetchone()[0]
        log.info(f"✅ gold.fact_customer_rfm: {cnt:,} rows")

        dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT "CustomerID" FROM gold.fact_customer_rfm
                GROUP BY 1 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        if dupes:
            raise ValueError(f"gold.fact_customer_rfm punya {dupes} duplikat CustomerID")

        log.info("=== Distribusi segment ===")
        for r in conn.execute("""
            SELECT "Segment", COUNT(*), ROUND(SUM("Monetary"), 2)
            FROM gold.fact_customer_rfm GROUP BY 1 ORDER BY 3 DESC
        """).fetchall():
            log.info(f"  {r[0]:<12} {r[1]:>6,} customer   ${r[2]:>15,.2f}")
    finally:
        conn.close()


def export_gold_to_minio(**context):
    """Export tabel gold final ke MinIO sebagai satu file Parquet."""
    logical_date = context["ds"]
    path = f"{S3_PREFIX}/gold/fact_customer_rfm/dt={logical_date}/data.parquet"

    conn = get_duckdb_conn()
    try:
        conn.execute(f"""
            COPY (SELECT * FROM gold.fact_customer_rfm)
            TO '{path}' (FORMAT PARQUET, COMPRESSION SNAPPY);
        """)
        log.info(f"✅ Exported gold.fact_customer_rfm → {path}")
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════
# DAG DEFINITION
# ════════════════════════════════════════════════════════════
with DAG(
    dag_id="dag_customer_rfm_transform_giwang",
    default_args=default_args,
    description="DAG 2 — Transform RFM di DuckDB: bronze → silver → gold",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # DuckDB berbasis file: jangan sampai dua DAG run menulis bersamaan
    max_active_runs=1,
    tags=["elt", "duckdb", "transform", "bronze", "silver", "gold", "rfm"],
    doc_md=__doc__,
) as dag:

    task_setup = PythonOperator(
        task_id="setup_duckdb_s3",
        python_callable=setup_duckdb_s3,
    )

    task_bronze = PythonOperator(
        task_id="create_bronze_layer",
        python_callable=create_bronze_layer,
    )

    task_silver = PythonOperator(
        task_id="create_silver_layer",
        python_callable=create_silver_layer,
    )

    task_gold = PythonOperator(
        task_id="create_gold_layer",
        python_callable=create_gold_layer,
    )

    task_export = PythonOperator(
        task_id="export_gold_to_minio",
        python_callable=export_gold_to_minio,
    )

    (
        task_setup
        >> task_bronze
        >> task_silver
        >> task_gold
        >> task_export
    )
