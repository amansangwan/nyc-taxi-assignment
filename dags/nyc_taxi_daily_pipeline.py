import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR
DBT_TARGET = "snowflake"


SNOWFLAKE_CONN_ID = "snowflake_default"

PROD_SCHEMA = "DBT"
AUDIT_SCHEMA = "DBT_AUDIT"

# --- default_args -----------------------------------------------------------
default_args = {
    "owner": "yellow_trip",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["amansingh58037@gmail.com"],
}

SIMULATE_FRESHNESS = True



def dbt_env(schema: str = PROD_SCHEMA) -> dict[str, str]:

    return {
        "SNOWFLAKE_ACCOUNT":   "{{ conn.snowflake_default.extra_dejson.account }}",
        "SNOWFLAKE_USER":      "{{ conn.snowflake_default.login }}",
        "SNOWFLAKE_PASSWORD":  "{{ conn.snowflake_default.password }}",
        "SNOWFLAKE_ROLE":      "{{ conn.snowflake_default.extra_dejson.get('role', 'SYSADMIN') }}",
        "SNOWFLAKE_WAREHOUSE": "{{ conn.snowflake_default.extra_dejson.get('warehouse', 'COMPUTE_WH') }}",
        "SNOWFLAKE_DATABASE":  "{{ conn.snowflake_default.extra_dejson.get('database', 'NYC_TAXI') }}",
        "SNOWFLAKE_SCHEMA":    schema,
    }


def _dbt_command(select: str | None = None, command: str = "run") -> str:
    """Build a `dbt <command>` invocation pinned to the project dir + target."""
    select_clause = f" --select {select}" if select else ""
    return (
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt {command}{select_clause} "
        f"--target {DBT_TARGET} --profiles-dir {DBT_PROFILES_DIR}"
    )


def check_source_freshness(logical_date: str, **_) -> None:

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    day_count = hook.get_first(
        """
        select count(*)
        from NYC_TAXI.RAW.YELLOW_TRIPDATA
        where cast(tpep_pickup_datetime as date) = %(run_date)s
        """,
        parameters={"run_date": logical_date},
    )[0]
    log.info("Source freshness: %s raw rows for %s", day_count, logical_date)

    if day_count > 0:
        return

    if SIMULATE_FRESHNESS:
        total = hook.get_first("select count(*) from NYC_TAXI.RAW.YELLOW_TRIPDATA")[0]
        if total == 0:
            raise ValueError("Raw YELLOW_TRIPDATA is empty -- nothing to transform.")
        log.warning(
            "No rows for %s; static 2023 dataset -- passing (simulated).",
            logical_date,
        )
        return

    raise ValueError(
        f"No raw rows for {logical_date}; refusing to run the transform."
    )


def publish_swap(**_) -> None:
    """STUB -- blue/green publish (write-audit-publish).

    Reaching this task means run_dbt_tests passed (a non-zero `dbt test` exit
    fails the DAG upstream), so promotion is safe. The full design lives in the
    README; the real implementation atomically swaps each freshly-built+tested
    mart from AUDIT_SCHEMA into PROD_SCHEMA, e.g. per object:

        ALTER TABLE NYC_TAXI.DBT.FCT_TRIPS
            SWAP WITH NYC_TAXI.DBT_AUDIT.FCT_TRIPS;

    Snowflake SWAP is atomic and metadata-only, so readers never see a partial
    publish. Left as a logged stub here.
    """
    log.info("publish_swap stub: would promote marts %s -> %s", AUDIT_SCHEMA, PROD_SCHEMA)


def notify_success(logical_date: str, **_) -> None:
    """Log the day's headline numbers from agg_daily_revenue for the run date."""

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    row = hook.get_first(
        """
        select total_trips, total_fare
        from NYC_TAXI.DBT.AGG_DAILY_REVENUE
        where trip_date = %(run_date)s
        """,
        parameters={"run_date": logical_date},
    )
    if row:
        log.info(
            "nyc_taxi_daily_pipeline OK for %s: %s trips, $%.2f total fare.",
            logical_date, row[0], row[1],
        )
    else:
        log.info(
            "nyc_taxi_daily_pipeline OK for %s: no rows in agg_daily_revenue "
            "(expected for the static 2023 dataset on a non-2023 date).",
            logical_date,
        )


with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    description="Daily dbt transform of NYC TLC Yellow Taxi data on Snowflake.",
    schedule="0 2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["nyc_taxi", "dbt", "snowflake"],
) as dag:

    check_source_freshness_task = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=_dbt_command(select="staging"),
        env=dbt_env(AUDIT_SCHEMA),
        append_env=True,
    )

    run_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=_dbt_command(select="intermediate"),
        env=dbt_env(AUDIT_SCHEMA),
        append_env=True,
    )


    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=_dbt_command(select="marts"),
        env=dbt_env(AUDIT_SCHEMA),
        append_env=True,
    )


    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=_dbt_command(command="test"),
        env=dbt_env(AUDIT_SCHEMA),
        append_env=True,
    )

    publish_swap_task = PythonOperator(
        task_id="publish_swap",
        python_callable=publish_swap,
    )


    notify_success_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    (
        check_source_freshness_task
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_marts
        >> run_dbt_tests
        >> publish_swap_task
        >> notify_success_task
    )
