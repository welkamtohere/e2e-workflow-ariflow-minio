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
  compute_rfm_cutoffs      ← [XCom] batas kuartil GLOBAL + list territory
        ↓
  build_gold_layer         ← [DYNAMIC TASK MAPPING] 1 task per territory
        ↓
  publish_gold_layer       ← [REDUCE] gabung jadi gold.fact_customer_rfm
        ↓
  export_gold_to_minio     ← simpan fact sebagai Parquet di MinIO
        ↓
  run_analytic_queries     ← jawab pertanyaan bisnis

Kenapa cutoff dihitung terpisah (compute_rfm_cutoffs), bukan NTILE di
dalam tiap mapped task? Karena pertanyaannya "customer paling valuable
KITA" — skornya harus dibandingkan lintas seluruh customer. Kalau tiap
mapped task menghitung NTILE-nya sendiri, "Champions" jadi berarti
"top-kuartil di wilayahnya saja", dan customer $8rb di wilayah sepi bisa
mengalahkan customer $15rb di wilayah ramai.

Jalankan setelah DAG 1 (elt_01_ingestion) selesai.
"""

import io
import os
import socket
import logging
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

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

# Territory dengan customer di bawah ini dilewati — kuartil tidak
# bermakna kalau populasinya cuma belasan orang.
MIN_CUSTOMERS_PER_TERRITORY = 30

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


def get_minio() -> Minio:
    endpoint = _resolve_endpoint(MINIO_ENDPOINT, 9000)
    return Minio(
        endpoint=endpoint,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=MINIO_SECURE,
    )


def get_duckdb_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """
    Buka koneksi DuckDB dan konfigurasi S3 extension agar bisa
    baca/tulis Parquet di MinIO.

    read_only=True WAJIB dipakai oleh mapped task yang jalan paralel.
    DuckDB itu single-writer: kalau dua proses sama-sama membuka file
    ini untuk menulis, yang kalah rebutan lock langsung gagal.
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


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        buffer,
        compression="snappy",
    )
    return buffer.getvalue()


def upload_parquet(client: Minio, object_path: str, data: bytes) -> None:
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_path,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/octet-stream",
    )
    log.info(f"Uploaded → s3://{MINIO_BUCKET}/{object_path} ({len(data)/1024:.1f} KB)")


def read_parquet_from_minio(client: Minio, object_path: str) -> pd.DataFrame:
    resp = client.get_object(MINIO_BUCKET, object_path)
    try:
        return pq.read_table(io.BytesIO(resp.read())).to_pandas()
    finally:
        resp.close()
        resp.release_conn()


def _score_expr(col: str, cuts: list, higher_is_better: bool) -> str:
    """
    Bangun CASE WHEN skor 1-4 dari batas kuartil GLOBAL.

    Kenapa pakai cutoff eksplisit, bukan NTILE(4)?
    1. NTILE dihitung per-query, jadi nilainya beda kalau populasinya
       dipecah per territory. Cutoff bisa dibagikan lewat XCom.
    2. NTILE memecah nilai kembar ke bucket berbeda secara sembarang —
       dua customer dengan Frequency sama persis bisa dapat skor beda.
       Dengan cutoff, nilai sama selalu dapat skor sama.
    """
    q25, q50, q75 = cuts
    if higher_is_better:   # Frequency, Monetary: makin besar makin bagus
        return (f'CASE WHEN {col} >= {q75} THEN 4 '
                f'WHEN {col} >= {q50} THEN 3 '
                f'WHEN {col} >= {q25} THEN 2 ELSE 1 END')
    # Recency (jumlah hari): makin kecil makin bagus
    return (f'CASE WHEN {col} <= {q25} THEN 4 '
            f'WHEN {col} <= {q50} THEN 3 '
            f'WHEN {col} <= {q75} THEN 2 ELSE 1 END')


def _segment_case(score_expr: str) -> str:
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
    - silver.stg_sales_orders  : grain 1 ORDER  (header - detail, LineTotal diagregasi)
    - silver.stg_customers     : grain 1 CUSTOMER (customer - person)
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
                SUM(sod."LineTotal"::DOUBLE)     AS "OrderAmount",
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


def compute_rfm_cutoffs(**context):
    """
    Hitung batas kuartil SEKALI atas SELURUH populasi customer,
    lalu bagikan ke semua mapped task lewat XCom.

    Task ini mengembalikan DUA hal:
    - xcom_push(key="cutoffs")  → 9 angka batas, dibaca tiap mapped task
    - return [...]              → daftar territory, jadi sumber .expand()

    Inilah yang menjaga skor tetap comparable lintas wilayah. Territory
    di bawah cuma jadi unit paralelisasi, bukan unit perbandingan.
    """
    conn = get_duckdb_conn(read_only=True)
    try:
        q = conn.execute("""
            SELECT
                QUANTILE_CONT("Recency",   [0.25, 0.50, 0.75]),
                QUANTILE_CONT("Frequency", [0.25, 0.50, 0.75]),
                QUANTILE_CONT("Monetary",  [0.25, 0.50, 0.75])
            FROM silver.trf_customer_rfm
        """).fetchone()

        territories = conn.execute(f"""
            SELECT "TerritoryID", "TerritoryName", COUNT(*) AS n_customers
            FROM silver.trf_customer_rfm
            WHERE "TerritoryID" IS NOT NULL
            GROUP BY 1, 2
            HAVING COUNT(*) >= {MIN_CUSTOMERS_PER_TERRITORY}
            ORDER BY 1
        """).fetchall()
    finally:
        conn.close()

    if not territories:
        raise ValueError(
            f"Tidak ada territory dengan >= {MIN_CUSTOMERS_PER_TERRITORY} customer."
        )

    cutoffs = {
        "recency":   [float(x) for x in q[0]],
        "frequency": [float(x) for x in q[1]],
        "monetary":  [float(x) for x in q[2]],
    }

    log.info("=== Cutoff GLOBAL (q25 / q50 / q75) ===")
    for dim, cuts in cutoffs.items():
        log.info(f"  {dim:<10} {cuts[0]:>12,.2f} {cuts[1]:>12,.2f} {cuts[2]:>12,.2f}")
    log.info(f"=== {len(territories)} territory akan diproses ===")
    for t in territories:
        log.info(f"  {t[1]:<16} {t[2]:>6,} customer")

    # Broadcast ke semua mapped task
    context["ti"].xcom_push(key="cutoffs", value=cutoffs)

    # return_value dipakai sebagai sumber .expand(op_kwargs=...)
    return [{"territory_id": t[0], "territory_name": t[1]} for t in territories]


def build_gold_layer(territory_id: int, territory_name: str, **context):
    """
    [DYNAMIC TASK MAPPING] Satu task per territory.

    Task ini TIDAK menghitung kuartilnya sendiri — dia menarik cutoff
    global dari XCom dan menerapkannya. Hasilnya ditulis ke MinIO
    sebagai Parquet, BUKAN ke DuckDB, karena DuckDB single-writer dan
    task-task ini jalan paralel.
    """
    logical_date = context["ds"]

    cutoffs = context["ti"].xcom_pull(task_ids="compute_rfm_cutoffs", key="cutoffs")
    if not cutoffs:
        raise ValueError("Cutoff global tidak tersedia di XCom.")

    r_expr = _score_expr('"Recency"',   cutoffs["recency"],   higher_is_better=False)
    f_expr = _score_expr('"Frequency"', cutoffs["frequency"], higher_is_better=True)
    m_expr = _score_expr('"Monetary"',  cutoffs["monetary"],  higher_is_better=True)
    total  = '("R_Score" + "F_Score" + "M_Score")'

    sql = f"""
        WITH scored AS (
            SELECT
                *,
                {r_expr} AS "R_Score",
                {f_expr} AS "F_Score",
                {m_expr} AS "M_Score"
            FROM silver.trf_customer_rfm
            WHERE "TerritoryID" = {territory_id}
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
            {_segment_case(total)}                  AS "Segment"
        FROM scored
    """

    # read_only=True: mapped task TIDAK boleh menulis ke file DuckDB
    conn = get_duckdb_conn(read_only=True)
    try:
        df = conn.execute(sql).df()
    finally:
        conn.close()

    if df.empty:
        raise ValueError(f"Territory {territory_name} menghasilkan 0 baris.")

    object_path = (
        f"gold/customer_rfm/dt={logical_date}/territory_id={territory_id}/data.parquet"
    )
    upload_parquet(get_minio(), object_path, df_to_parquet_bytes(df))
    log.info(f"✅ {territory_name}: {len(df):,} customer → {object_path}")

    return {
        "territory_id": territory_id,
        "territory_name": territory_name,
        "path": object_path,
        "rows": len(df),
    }


def publish_gold_layer(**context):
    """
    [REDUCE] Satu-satunya task yang MENULIS ke DuckDB.

    xcom_pull ke task yang di-map otomatis mengembalikan LIST berisi
    semua map index — itu reduce step gratis, tidak perlu task pengumpul.
    """
    logical_date = context["ds"]
    parts = context["ti"].xcom_pull(task_ids="build_gold_layer")
    if not parts:
        raise ValueError("Tidak ada partisi gold yang dihasilkan.")

    client = get_minio()
    df = pd.concat(
        [read_parquet_from_minio(client, p["path"]) for p in parts],
        ignore_index=True,
    )

    expected = sum(p["rows"] for p in parts)
    if len(df) != expected:
        raise ValueError(f"Row mismatch: XCom bilang {expected}, hasil concat {len(df)}")

    df["LoadTimestamp"] = pd.Timestamp.utcnow().tz_localize(None)
    df["BatchDate"]     = pd.Timestamp(logical_date).date()

    conn = get_duckdb_conn()
    try:
        conn.execute("DROP TABLE IF EXISTS gold.fact_customer_rfm;")
        conn.register("src", df)
        conn.execute("CREATE TABLE gold.fact_customer_rfm AS SELECT * FROM src;")
        conn.unregister("src")

        dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT "CustomerID" FROM gold.fact_customer_rfm
                GROUP BY 1 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        if dupes:
            raise ValueError(f"gold.fact_customer_rfm punya {dupes} duplikat CustomerID")
    finally:
        conn.close()

    log.info(f"✅ gold.fact_customer_rfm: {len(df):,} rows dari {len(parts)} territory")
    return {"rows": len(df), "territories": len(parts)}


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


def run_analytic_queries(**context):
    """Jawab pertanyaan bisnisnya secara eksplisit di log."""
    conn = get_duckdb_conn(read_only=True)
    try:
        log.info("=" * 78)
        log.info("Q1 — SIAPA CUSTOMER PALING VALUABLE? (top 15 global)")
        log.info("=" * 78)
        q = """
            SELECT "CustomerName", "TerritoryName", "Recency", "Frequency",
                   "Monetary", "RFM_Cell", "RFM_Score", "Segment"
            FROM gold.fact_customer_rfm
            ORDER BY "RFM_Score" DESC, "Monetary" DESC
            LIMIT 15
        """
        for r in conn.execute(q).fetchall():
            log.info(f"  {r[0]:<30} {r[1]:<15} R={r[2]:<5} F={r[3]:<4} "
                     f"M=${r[4]:>12,.2f}  {r[5]}  score={r[6]:<3} {r[7]}")

        log.info("=" * 78)
        log.info("Q2 — BAGAIMANA SEGMENTASINYA?")
        log.info("=" * 78)
        q = """
            SELECT
                "Segment",
                COUNT(*)                                     AS n_customer,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_customer,
                ROUND(SUM("Monetary"), 2)                    AS total_value,
                ROUND(100.0 * SUM("Monetary")
                      / SUM(SUM("Monetary")) OVER (), 1)     AS pct_value,
                ROUND(AVG("AvgOrderValue"), 2)               AS avg_order_value
            FROM gold.fact_customer_rfm
            GROUP BY 1
            ORDER BY total_value DESC
        """
        for r in conn.execute(q).fetchall():
            log.info(f"  {r[0]:<12} {r[1]:>6,} cust ({r[2]:>4.1f}%)  "
                     f"${r[3]:>15,.2f} ({r[4]:>4.1f}% nilai)  AOV=${r[5]:>10,.2f}")

        log.info("=" * 78)
        log.info("Q3 — AT RISK (HIGH VALUE): dulu besar, sekarang menghilang")
        log.info("=" * 78)
        q = """
            SELECT "CustomerName", "TerritoryName", "Recency", "Frequency",
                   "Monetary", "RFM_Cell"
            FROM gold.fact_customer_rfm
            WHERE "R_Score" <= 2 AND "M_Score" >= 3
            ORDER BY "Monetary" DESC
            LIMIT 15
        """
        rows = conn.execute(q).fetchall()
        for r in rows:
            log.info(f"  {r[0]:<30} {r[1]:<15} {r[2]:>5} hari lalu  "
                     f"F={r[3]:<4} M=${r[4]:>12,.2f}  {r[5]}")

        at_risk_value = conn.execute("""
            SELECT ROUND(SUM("Monetary"), 2) FROM gold.fact_customer_rfm
            WHERE "R_Score" <= 2 AND "M_Score" >= 3
        """).fetchone()[0]
        log.info(f"  → Total nilai historis yang berisiko hilang: ${at_risk_value:,.2f}")

        log.info("=" * 78)
        log.info("Q4 — DISTRIBUSI SEGMENT PER TERRITORY")
        log.info("=" * 78)
        q = """
            SELECT "TerritoryName", "Segment", COUNT(*) AS n
            FROM gold.fact_customer_rfm
            GROUP BY 1, 2
            ORDER BY "TerritoryName", n DESC
        """
        for r in conn.execute(q).fetchall():
            log.info(f"  {r[0]:<16} {r[1]:<12} {r[2]:>6,}")
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

    task_cutoffs = PythonOperator(
        task_id="compute_rfm_cutoffs",
        python_callable=compute_rfm_cutoffs,
    )

    # ── DYNAMIC TASK MAPPING ──────────────────────────────────
    # .expand() dari XComArg: jumlah task ditentukan saat RUNTIME
    # dari isi data, bukan dari list statis saat DAG di-parse.
    task_gold = PythonOperator.partial(
        task_id="build_gold_layer",
        python_callable=build_gold_layer,
        max_active_tis_per_dag=4,
    ).expand(op_kwargs=task_cutoffs.output)

    task_publish = PythonOperator(
        task_id="publish_gold_layer",
        python_callable=publish_gold_layer,
    )

    task_export = PythonOperator(
        task_id="export_gold_to_minio",
        python_callable=export_gold_to_minio,
    )

    task_analytics = PythonOperator(
        task_id="run_analytic_queries",
        python_callable=run_analytic_queries,
    )

    (
        task_setup
        >> task_bronze
        >> task_silver
        >> task_cutoffs
        >> task_gold
        >> task_publish
        >> task_export
        >> task_analytics
    )
