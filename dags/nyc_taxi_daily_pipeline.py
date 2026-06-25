"""NYC TLC Yellow Taxi -- daily transformation pipeline (Airflow 2.x).

Orchestrates the dbt build on Snowflake once per day:

    check_source_freshness
        -> run_dbt_staging
        -> run_dbt_intermediate
        -> run_dbt_marts        (built into an AUDIT schema)
        -> run_dbt_tests
        -> publish_swap         (audit -> prod, only if tests passed)
        -> notify_success

Design notes:
  * dbt only TRANSFORMS data already in Snowflake; this DAG just sequences the
    dbt invocations and guards them (freshness pre-check, test gate, blue/green
    publish). Raw ingestion is a separate concern.
  * Every date-dependent task keys off the Airflow logical date ({{ ds }} /
    data_interval_start), never datetime.now(), so historical backfills are
    deterministic and idempotent.
  * Snowflake credentials are pulled at runtime from an Airflow Connection and
    injected as env vars into the dbt BashOperators, so dbt's profiles.yml
    env_var() placeholders resolve. Nothing is hardcoded in this file.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# --- Config -----------------------------------------------------------------
# Path to the dbt project on the Airflow worker (mount the repo's dbt/ here).
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR
DBT_TARGET = "snowflake"

# Airflow Connection holding the Snowflake secrets (no credentials in code).
SNOWFLAKE_CONN_ID = "snowflake_default"

# Blue/green publish (WAP): dbt builds the marts into AUDIT, tests run there,
# and only a green test run promotes AUDIT -> PROD via an atomic schema swap.
PROD_SCHEMA = "DBT"
AUDIT_SCHEMA = "DBT_AUDIT"

# --- default_args -----------------------------------------------------------
default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@example.com"],
}

# When True, the freshness check tolerates a date with no rows as long as the
# raw table is non-empty -- needed because our data is a STATIC 2023 bulk load,
# so a 2026 execution date legitimately has no partition. Set False for a real
# daily feed, where a missing partition for the run date should hard-fail.
SIMULATE_FRESHNESS = True


# --- Credential injection ---------------------------------------------------
def dbt_env(schema: str = PROD_SCHEMA) -> dict[str, str]:
    """Env-var mapping consumed by the dbt BashOperators.

    Values are Airflow templates that resolve from the Snowflake Connection at
    RUN time, not parse time -- so the scheduler never reads secrets while
    parsing the DAG, nothing is hardcoded here, and dbt's profiles.yml
    env_var() placeholders pick these up. The literal conn id below matches
    SNOWFLAKE_CONN_ID. `schema` lets a task redirect its build (e.g. to the
    audit schema for blue/green).
    """
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


# --- Python callables -------------------------------------------------------
def check_source_freshness(logical_date: str, **_) -> None:
    """Fail fast if the raw source has no data for the run's logical date.

    NOTE: our NYC TLC data is a STATIC 2023 bulk load, so this is a
    parameterized / simulated guard (see SIMULATE_FRESHNESS). For a live daily
    feed the same per-date count would detect a missing or late-arriving
    partition and stop the build before dbt runs. Keys off the Airflow logical
    date ({{ ds }}), never datetime.now(), so backfills check the right day.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

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
        # Static-dataset fallback: a real feed would raise here. Verify the bulk
        # load is present (table non-empty) and pass, so the demo DAG can run
        # for any execution date.
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
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

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


# --- DAG --------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    description="Daily dbt transform of NYC TLC Yellow Taxi data on Snowflake.",
    schedule="0 2 * * *",          # 02:00 UTC every day
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["nyc_taxi", "dbt", "snowflake"],
) as dag:

    # a) guard: is the source ready for this run's date?
    check_source_freshness_task = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    # b-d) dbt build, layer by layer. The whole transform is built into the
    # AUDIT schema (write-audit-publish): nothing touches PROD until tests pass.
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

    # marts built into the audit schema for blue/green promotion.
    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=_dbt_command(select="marts"),
        env=dbt_env(AUDIT_SCHEMA),
        append_env=True,
    )

    # e) test gate: a non-zero `dbt test` exit fails the DAG and blocks publish.
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=_dbt_command(command="test"),
        env=dbt_env(AUDIT_SCHEMA),
        append_env=True,
    )

    # f) promote audit -> prod (atomic SWAP). Stub; full design in README.
    publish_swap_task = PythonOperator(
        task_id="publish_swap",
        python_callable=publish_swap,
    )

    # g) success notification with the day's headline metrics.
    notify_success_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        op_kwargs={"logical_date": "{{ ds }}"},
    )

    # --- Wiring: strict linear pipeline -------------------------------------
    # freshness gate -> layered dbt build -> test gate -> publish -> notify.
    (
        check_source_freshness_task
        >> run_dbt_staging
        >> run_dbt_intermediate
        >> run_dbt_marts
        >> run_dbt_tests
        >> publish_swap_task
        >> notify_success_task
    )
