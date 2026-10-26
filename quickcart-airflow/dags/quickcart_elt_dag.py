
import logging
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum # type: ignore
from sqlalchemy import text

from airflow.sdk import dag, task, BaseSensorOperator  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.providers.smtp.notifications.smtp import send_smtp_notification # type: ignore


# -------------------------------------------------
# 1. Configuration
# -------------------------------------------------

DATA_DIR = Path("/opt/airflow/data")

EXPECTED_COLUMNS = {
    "customers": [
        "customer_id",
        "customer_name",
        "email",
        "city",
        "signup_date",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "payment_status",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "supplier_id",
        "cost_price",
        "selling_price",
    ],
    "clickstream": [
        "event_id",
        "customer_id",
        "event_type",
        "page_url",
        "event_timestamp",
        "device_type",
    ],
}


# -------------------------------------------------
# 2. Sensor: Wait for CSV files
# -------------------------------------------------

class CSVFilesSensor(BaseSensorOperator):

    def poke(self, context):
        missing_files = []

        for name in EXPECTED_COLUMNS:
            file_path = DATA_DIR / f"{name}.csv"

            if not file_path.is_file():
                missing_files.append(file_path.name)

        if missing_files:
            self.log.info(
                "Waiting for files: %s",
                missing_files,
            )
            return False

        self.log.info("All CSV files are available")
        return True


# -------------------------------------------------
# 3. Failure email notification
# -------------------------------------------------

failure_email = send_smtp_notification(
    from_email="shlok140795@gmail.com",
    to="shlok140795@gmail.com",
    subject="QuickCart DAG Task Failed",
    html_content="""
        <h3>QuickCart Pipeline Failure</h3>
        <p>DAG: {{ dag.dag_id }}</p>
        <p>Task: {{ ti.task_id }}</p>
        <p>Run: {{ run_id }}</p>
        <p>Check Airflow logs for details.</p>
    """,
    smtp_conn_id="smtp_default",
)


# -------------------------------------------------
# 4. DAG Definition
# -------------------------------------------------

@dag(
    dag_id="quickcart_daily_elt",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(
        2026, 1, 1, tz="Europe/Berlin"
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "quickcart",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_email,
    },
    tags=["quickcart", "elt", "csv"],
)
def quickcart_daily_elt():

    # ---------------------------------------------
    # Task 1: Wait for files
    # ---------------------------------------------

    wait_for_files = CSVFilesSensor(
        task_id="wait_for_csv_files",
        poke_interval=60,
        timeout=1800,
        mode="reschedule",
    )

    # ---------------------------------------------
    # Task 2: Extract CSV files
    # ---------------------------------------------

    @task
    def extract_csv():

        for name in EXPECTED_COLUMNS:

            file_path = DATA_DIR / f"{name}.csv"

            df = pd.read_csv(
                file_path,
                dtype=str,
            )

            logging.info(
                "Extracted %s: %d rows",
                name,
                len(df),
            )

        return "Extraction completed"

    # ---------------------------------------------
    # Task 3: Validate CSV files
    # ---------------------------------------------

    @task
    def validate_csv():

        primary_keys = {
            "customers": "customer_id",
            "orders": "order_id",
            "products": "product_id",
            "clickstream": "event_id",
        }

        for name, expected_columns in EXPECTED_COLUMNS.items():

            df = pd.read_csv(
                DATA_DIR / f"{name}.csv",
                dtype=str,
            )

            if list(df.columns) != expected_columns:
                raise ValueError(
                    f"Schema mismatch in {name}.csv"
                )

            if df.empty:
                raise ValueError(
                    f"{name}.csv is empty"
                )

            key = primary_keys[name]

            missing_ids = df[key].isna().sum()

            if missing_ids > 0:
                raise ValueError(
                    f"{name}.csv has {missing_ids} missing IDs"
                )

            duplicates = df[key].duplicated().sum()

            logging.info(
                "%s has %d duplicate IDs",
                name,
                duplicates,
            )

            logging.info(
                "%s validation completed",
                name,
            )

        return "Validation completed"

    
    # ---------------------------------------------
    # Task 4: Load staging tables
    # ---------------------------------------------

    @task
    def load_staging():

        hook = PostgresHook(
            postgres_conn_id="quickcart_postgres"
        )

        engine = hook.get_sqlalchemy_engine()

        try:
            with engine.begin() as conn:
                conn.execute(
                    text("CREATE SCHEMA IF NOT EXISTS staging")
                )

            for name in EXPECTED_COLUMNS:

                df = pd.read_csv(
                    DATA_DIR / f"{name}.csv",
                    dtype=str,
                    keep_default_na=False,
                )

                table_name = f"stg_{name}"

                # Create table if it does not exist
                df.head(0).to_sql(
                    name=table_name,
                    con=engine,
                    schema="staging",
                    if_exists="append",
                    index=False,
                )

                # Refresh data without dropping the table
                with engine.begin() as conn:

                    conn.execute(
                        text(
                            f'DELETE FROM staging."{table_name}"'
                        )
                    )

                    df.to_sql(
                        name=table_name,
                        con=conn,
                        schema="staging",
                        if_exists="append",
                        index=False,
                        chunksize=1000,
                    )

                logging.info(
                    "Loaded %d rows into staging.%s",
                    len(df),
                    table_name,
                )

        finally:
            engine.dispose()

        return "Staging load completed"


    # ---------------------------------------------
    # Task 5: SQL Transformations
    # ---------------------------------------------

    @task
    def transform_data():

        hook = PostgresHook(
            postgres_conn_id="quickcart_postgres"
        )

        engine = hook.get_sqlalchemy_engine()

        with engine.begin() as conn:

            conn.execute(
                text(
                    "CREATE SCHEMA IF NOT EXISTS analytics"
                )
            )

            # Clean customers
            conn.execute(text("""
                CREATE OR REPLACE VIEW
                analytics.clean_customers AS

                SELECT
                    customer_id,
                    TRIM(customer_name) AS customer_name,
                    LOWER(TRIM(email)) AS email,
                    INITCAP(TRIM(city)) AS city,
                    signup_date

                FROM staging.stg_customers

                WHERE
                    NULLIF(TRIM(customer_id), '') IS NOT NULL
            """))

            # Daily sales summary
            conn.execute(text("""
                CREATE OR REPLACE VIEW
                analytics.daily_sales AS

                SELECT
                    order_date,
                    COUNT(*) AS total_orders,
                    SUM(
                        CAST(quantity AS NUMERIC)
                        *
                        CAST(unit_price AS NUMERIC)
                    ) AS total_sales

                FROM staging.stg_orders

                WHERE
                    LOWER(TRIM(payment_status)) = 'paid'
                    AND NULLIF(TRIM(quantity), '') IS NOT NULL
                    AND NULLIF(TRIM(unit_price), '') IS NOT NULL

                GROUP BY order_date
            """))

            # Most visited pages
            conn.execute(text("""
                CREATE OR REPLACE VIEW
                analytics.page_visits AS

                SELECT
                    page_url,
                    COUNT(*) AS total_events

                FROM staging.stg_clickstream

                WHERE
                    NULLIF(TRIM(page_url), '') IS NOT NULL

                GROUP BY page_url
            """))

        engine.dispose()

        logging.info(
            "SQL transformations completed"
        )

     # ---------------------------------------------
     # 5. Exporting the files
     # ---------------------------------------------

    @task
    def export_results():

        hook = PostgresHook(
            postgres_conn_id="quickcart_postgres"
        )

        engine = hook.get_sqlalchemy_engine()

        output_dir = Path("/opt/airflow/data/output")
        output_dir.mkdir(parents=True, exist_ok=True)

        tables = [
            "clean_customers",
            "daily_sales",
            "page_visits",
        ]

        try:
            for table in tables:

                df = pd.read_sql(
                    text(f"SELECT * FROM analytics.{table}"),
                    con=engine,
                )

                output_file = output_dir / f"{table}.csv"

                df.to_csv(output_file, index=False)

                logging.info(
                    "Exported %s with %d rows",
                    output_file,
                    len(df),
                )
        finally:
            engine.dispose()

    # ---------------------------------------------
    # 6. Task Dependencies
    # ---------------------------------------------

    extracted = extract_csv()
    validated = validate_csv()
    loaded = load_staging()
    transformed = transform_data()
    exported = export_results()

    (
        wait_for_files
        >> extracted
        >> validated
        >> loaded
        >> transformed
        >> exported
    )


# Create DAG instance
quickcart_daily_elt()
