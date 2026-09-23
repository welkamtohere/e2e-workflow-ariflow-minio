"""
DAG: adventure_works_sales_performance
Deskripsi: Pipeline ETL Sales Performance dari AdventureWorks
           ke Data Warehouse schema dwh.
           Skenario: "Produk apa yang paling laris per wilayah?"

Pipeline  : check_connection → extract → transform → load
Source    : Sales, Production schema (AdventureWorks)
Target    : schema dwh
"""

from datetime import datetime, timedelta

import duckdb
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

# ──────────────────────────────────────────────
# Konstanta
# ──────────────────────────────────────────────
CONN_ID       = "adventure_works"
TARGET_SCHEMA = "example"
DUCKDB_PATH    = "/opt/airflow/dags/repo/dwh_assignment.duckdb"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ──────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────
def get_hook():
    return PostgresHook(postgres_conn_id=CONN_ID)


# ──────────────────────────────────────────────
# Task 1 — Check Connection
# ──────────────────────────────────────────────
def check_connection(**kwargs):
    """Verifikasi koneksi ke PostgreSQL adventure_works."""
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"[check_connection] Koneksi berhasil!")
    print(f"[check_connection] {version[0]}")

    cursor.close()
    conn.close()


# ──────────────────────────────────────────────
# Task 2 — Extract
# Source  → dwh.stg_sales_orders
#         → dwh.stg_products
# ──────────────────────────────────────────────
def extract(**kwargs):
    """
    Ekstrak data Order, Customer, dan Territory dari AdventureWorks.
    Hasil disimpan ke:
      - dwh.stg_customer_orders  (data customer + order mentah)
    """
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # ── Setup schema & tabel staging ──────────────────────────────
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

        #stg_customer_orders
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."stg_customer_orders" (
                "SalesOrderID"   INT,
                "CustomerID"     INT,
                "OrderDate"      TIMESTAMP,
                "TerritoryID"    INT,
                "TerritoryName"  VARCHAR(100),
                "OrderAmount"    NUMERIC(15, 2),
                "CustomerName"   VARCHAR(255),
                PRIMARY KEY ("SalesOrderID", "CustomerID")
            );
        """)

        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."stg_customer_orders";')


        # ── Extract: Customer Orders Territory ──────────────────────────────────────
        # JOIN: SalesOrderHeader → SalesOrderDetail → SalesTerritory -> Customer
        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."stg_customer_orders" (
                "SalesOrderID",
                "CustomerID",
                "OrderDate",
                "TerritoryID",
                "TerritoryName",
                "OrderAmount",
                "CustomerName"
            )
            SELECT
                soh."SalesOrderID",
                soh."CustomerID",
                soh."OrderDate",
                soh."TerritoryID",
                st."Name" AS "TerritoryName",
                SUM(sod."LineTotal") AS "OrderAmount",
                p."FirstName" || ' ' || p."LastName" AS "CustomerName"
            FROM "Sales"."SalesOrderHeader" AS soh
            INNER JOIN "Sales"."SalesOrderDetail" AS sod
                ON soh."SalesOrderID" = sod."SalesOrderID"
            INNER JOIN "Sales"."SalesTerritory" AS st
                ON soh."TerritoryID" = st."TerritoryID"
            INNER JOIN "Sales"."Customer" AS c
                ON soh."CustomerID" = c."CustomerID"
            LEFT JOIN "Person"."Person" AS p
                ON c."PersonID" = p."BusinessEntityID"
            WHERE st."Name" IN ('Northwest', 'Southwest')
            GROUP BY
                soh."SalesOrderID",
                soh."CustomerID",
                soh."OrderDate",
                soh."TerritoryID",
                st."Name",
                p."FirstName",
                p."LastName";
        """)


        conn.commit()
        print(f"[extract] stg_customer_orders berhasil dibuat.")

    except Exception as e:
        conn.rollback()
        print(f"[extract] ERROR: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# ──────────────────────────────────────────────
# Task 3 — Transform
# stg_customer_orders → dwh.trf_customer_summary
# ──────────────────────────────────────────────
def transform(**kwargs):
    """
    Gabungkan stg_customer_orders, lalu hitung:
      - Recency
      - Frequency
      - Monetary
      - R_Score
      - F_Score
      - M_Score
      - RFM Score = R_Score + F_Score + M_Score
    Hasil disimpan ke dwh.trf_customer_rfm
    """
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."trf_customer_rfm" (
                "CustomerID"     INT,
                "TerritoryName"  VARCHAR(100),
                "OrderAmount"    NUMERIC(15, 2),
                "CustomerName"   VARCHAR(255),
                "Recency"        INT,
                "Frequency"      INT,
                "Monetary"       NUMERIC(15, 2),
                "R_Score"        INT,   
                "F_Score"        INT,
                "M_Score"        INT,
                "RFM_Score"      INT,   
                PRIMARY KEY ("CustomerID"),
                "Segment"        VARCHAR(10)
            );
        """)

        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."trf_customer_rfm";')

        # Transformasi: JOIN staging + agregasi + kalkulasi margin
        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."trf_customer_rfm" (
                "CustomerID",
                "TerritoryName",
                "CustomerName",
                "Recency",
                "Frequency",
                "Monetary",
                "R_Score",
                "F_Score",
                "M_Score",
                "RFM_Score",
                "Segment"
            )

            with customer_orders as (
                SELECT
                    "CustomerID",
                    "TerritoryName",
                    "OrderAmount",
                    "CustomerName",
                    EXTRACT(DAY FROM (NOW() - Max("OrderDate"))) AS "Recency",
                    COUNT("SalesOrderID")  AS "Frequency",
                    SUM("OrderAmount") AS "Monetary"
                    FROM "{TARGET_SCHEMA}"."stg_customer_orders"
                    GROUP BY "CustomerID", "TerritoryName", "OrderAmount", "CustomerName"
            ),
            scored as (
                SELECT *
                , 
                    NTILE(4) OVER (ORDER BY "Recency" ASC) AS "R_Score",
                    NTILE(4) OVER (ORDER BY "Frequency" DESC) AS "F_Score",
                    NTILE(4) OVER (ORDER BY "Monetary" DESC) AS "M_Score"
                FROM customer_orders
            )
            SELECT
                "CustomerID",
                "TerritoryName",
                "CustomerName",
                "Recency",
                "Frequency",
                "Monetary" ,
                "R_Score",
                "F_Score",
                "M_Score",
                ("R_Score" + "F_Score" + "M_Score") AS "RFM_Score",
                CASE
                    WHEN ("R_Score" + "F_Score" + "M_Score") >= 9 THEN 'Champions'
                    WHEN ("R_Score" + "F_Score" + "M_Score") >= 6 THEN 'Loyal'
                    WHEN ("R_Score" + "F_Score" + "M_Score") >= 3 THEN 'At Risk'
                    ELSE 'Lost'
                END AS "Segment"
            FROM scored;
        """)

        conn.commit()
        print(f"[transform] trf_customer_rfm berhasil dibuat.")

    except Exception as e:
        conn.rollback()
        print(f"[transform] ERROR: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# ──────────────────────────────────────────────
# Task 4 — Load
# trf_customer_rfm → dwh.fact_customer_rfm
# ──────────────────────────────────────────────
def load(**kwargs):
    """
    Load data final ke dwh.fact_customer_rfm.
    Tambahkan load_timestamp untuk audit trail.
    """
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."fact_customer_rfm" (
                "CustomerID"         INT            NOT NULL,
                "CustomerName"       VARCHAR(255),
                "TerritoryName"      VARCHAR(100),
                "Recency"            INT,
                "Frequency"          INT,
                "Monetary"           NUMERIC(15, 2),
                "R_Score"            INT,
                "F_Score"            INT,
                "M_Score"            INT,
                "RFM_Score"          INT,
                "Segment"            VARCHAR(50),
                "LoadTimestamp"      TIMESTAMP      DEFAULT NOW(),
                PRIMARY KEY ("CustomerID")
            );
        """)

        cursor.execute(f'TRUNCATE TABLE "{TARGET_SCHEMA}"."fact_customer_rfm";')

        cursor.execute(f"""
            INSERT INTO "{TARGET_SCHEMA}"."fact_customer_rfm" (
                "CustomerID", "CustomerName", "TerritoryName", "Recency", "Frequency", "Monetary",
                "R_Score", "F_Score", "M_Score", "RFM_Score", "Segment", "LoadTimestamp"
            )
            SELECT
                "CustomerID", "CustomerName", "TerritoryName", "Recency", "Frequency", "Monetary",
                "R_Score", "F_Score", "M_Score", "RFM_Score", "Segment", NOW()
            FROM "{TARGET_SCHEMA}"."trf_customer_rfm";
        """)

        cursor.execute(f'SELECT COUNT(*) FROM "{TARGET_SCHEMA}"."fact_customer_rfm";')
        total_rows = cursor.fetchone()[0]

        conn.commit()
        print(f"[load] Berhasil memuat {total_rows} baris ke {TARGET_SCHEMA}.fact_customer_rfm.")

    except Exception as e:
        conn.rollback()
        print(f"[load] ERROR: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# ──────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="dag_customer_rfm_giwang",
    default_args=default_args,
    description="ETL Customer RFM AdventureWorks → dwh.fact_customer_rfm",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["postgres", "adventure_works", "etl", "sales"],
) as dag:
    task_check_connection = PythonOperator(
        task_id="check_connection",
        python_callable=check_connection,
    )

    task_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    task_check_connection >> task_extract >> task_transform >> task_load
