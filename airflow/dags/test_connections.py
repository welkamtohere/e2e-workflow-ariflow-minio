from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def test_mysql_connection(**context):
    """Test MySQL connection"""
    try:
        logger.info("=" * 80)
        logger.info("🔍 Testing MySQL Target Connection...")
        logger.info("=" * 80)

        mysql_hook = MySqlHook(mysql_conn_id='mysql_target')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("SELECT VERSION();")
        version = cursor.fetchone()[0]
        logger.info(f"✅ MySQL connection successful!\n   Version: {version}")

        cursor.execute("SELECT COUNT(*) FROM dim_customers;")
        customer_count = cursor.fetchone()[0]
        logger.info(f"📊 Found {customer_count} customers in target database")

        cursor.close()
        connection.close()
        logger.info("=" * 80)
        return {"status": "success", "version": version, "customer_count": customer_count}

    except Exception as e:
        logger.error(f"❌ MySQL connection failed: {str(e)}")
        raise


def test_postgres_connection(**context):
    """Test PostgreSQL connection"""
    try:
        logger.info("=" * 80)
        logger.info("🔍 Testing PostgreSQL Source Connection...")
        logger.info("=" * 80)

        pg_hook = PostgresHook(postgres_conn_id='postgres_source')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        cursor.execute("SELECT VERSION();")
        version = cursor.fetchone()[0]
        logger.info(f"✅ PostgreSQL connection successful!\n   Version: {version}")

        cursor.close()
        connection.close()
        logger.info("=" * 80)
        return {"status": "success", "version": version}

    except Exception as e:
        logger.error(f"❌ PostgreSQL connection failed: {str(e)}")
        raise


with DAG(
    'test_database_connections',
    default_args=default_args,
    description='Test PostgreSQL and MySQL database connections',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'connections', 'database'],
) as dag:

    test_mysql = PythonOperator(
        task_id='test_mysql_connection',
        python_callable=test_mysql_connection,
    )

    test_postgres = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection,
    )

    test_mysql
    test_postgres
