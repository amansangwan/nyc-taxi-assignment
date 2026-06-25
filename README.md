# NYC TLC Yellow Taxi — Analytics Engineering (dbt + Airflow on Snowflake)

A production-grade transformation pipeline that models **NYC TLC Yellow Taxi 2023**
trip data (~38.3M rows) into a clean, tested, analytics-ready **star schema** on
Snowflake, orchestrated by an Airflow DAG.

The guiding principle throughout: **dbt only transforms data already in the
warehouse.** Raw data lives in Snowflake; dbt reads it, builds
`staging → intermediate → marts`, and writes the results back. Each layer does
exactly one job.

---

## 1. Repository layout

```
.
├── dbt/                         # the transformation project (Task 1)
│   ├── models/
│   │   ├── staging/             # 1:1 cleanup views over raw + seed
│   │   ├── intermediate/        # enrich + validity filter (view)
│   │   └── marts/               # star schema (tables)
│   ├── macros/                  # custom generic test: value_in_range
│   ├── tests/                   # custom singular test
│   ├── seeds/                   # taxi_zone_lookup.csv (265 zones)
│   ├── dbt_project.yml
│   ├── packages.yml             # dbt_utils
│   └── profiles.yml.example     # env_var() placeholders only
├── dags/
│   └── nyc_taxi_daily_pipeline.py   # Airflow orchestration (Task 2)
├── queries/                         # standalone analytical SQL (Task 3)
│   ├── q1_top_zones_by_revenue.sql      # top 10 zones / month (window fn)
│   ├── q2_hour_of_day_pattern.sql       # hourly pattern + 3h rolling avg
│   └── q3_consecutive_gap_analysis.sql  # per-day max same-zone idle gap (LAG)
├── spark/
│   └── process_historical.py        # PySpark all-years batch (Task 4, bonus)
└── README.md
```

---

## 2. Warehouse setup (Snowflake)

Pre-existing objects (not created by dbt):

| Object | Purpose |
|---|---|
| Warehouse `COMPUTE_WH` (XSMALL, auto-suspend) | Compute |
| Database `NYC_TAXI`, role `SYSADMIN` | Container |
| `NYC_TAXI.RAW.YELLOW_TRIPDATA` (~38.3M rows) | Raw source, 2023 full year |
| `NYC_TAXI.DBT` | Target schema dbt builds into |

dbt reads from `RAW`, builds into `DBT`. The source is declared in
`models/staging/_staging__sources.yml` and pointed at `NYC_TAXI.RAW.YELLOW_TRIPDATA`.

---

## 3. Local setup & running

```bash
# 1. Python env + dbt adapters
python -m venv .venv && source .venv/bin/activate
pip install -r dbt/requirements.txt          # dbt-snowflake

# 2. Credentials. Copy the example and fill it, OR export the SNOWFLAKE_* env
#    vars the example references. Never commit the real profiles.yml.
cp dbt/profiles.yml.example dbt/profiles.yml  # then edit

cd dbt

# 3. Install packages (dbt_utils) and load the zone seed
dbt deps --profiles-dir .
dbt seed   --target snowflake --profiles-dir .   # -> 265 zones

# 4. Build everything and run the tests
dbt build  --target snowflake --profiles-dir .   # models + 22 data tests

# Useful subsets
dbt run  --select staging      --target snowflake --profiles-dir .
dbt test --select fct_trips     --target snowflake --profiles-dir .
dbt source freshness            --target snowflake --profiles-dir .
```

> **Secrets:** the real `profiles.yml` is gitignored (`**/profiles.yml`). The
> committed `profiles.yml.example` contains `env_var()` placeholders only — no
> real credentials anywhere in the repo.

### Running the analytical queries (Task 3)

The three files in `queries/` are plain Snowflake SQL that read the built marts
(`NYC_TAXI.DBT.*`). Paste any of them into a Snowsight worksheet and run — no
dbt involved. See §7a and §8 (q1 / q3 carry the performance brainstormer
comments inline).

### Running the Spark job (Task 4, bonus)

`spark/process_historical.py` reproduces the dbt staging + `agg_daily_revenue`
logic at all-years (~1.5B-row) scale. It does not run against the warehouse —
it reads/writes Parquet — and is not required for the dbt pipeline:

```bash
spark-submit spark/process_historical.py \
    --input  s3://my-bucket/nyc-tlc/ \
    --output s3://my-bucket/nyc-tlc-marts/agg_daily_revenue/
# paths also accept env vars NYC_TAXI_INPUT_PATH / NYC_TAXI_OUTPUT_PATH
```

---

## 4. Architecture & why

### Layers and materializations

| Layer | Materialization | Why |
|---|---|---|
| **staging** | `view` | Thin 1:1 cleanup (rename, cast, light derived cols). Cheap, always live, no business logic. |
| **intermediate** | `view` | Business logic (joins + validity filter). Built as a real object so Airflow can build/inspect it as its own step. |
| **marts** | `table` | The public star schema. BI hits it repeatedly → build once. |

> **Note:** intermediate was originally `ephemeral` (inlined into the marts).
> It was promoted to `view` so the DAG's `run_dbt_intermediate` task builds a
> concrete, inspectable object — a small trade-off of one extra view for
> operational visibility and a cleaner task boundary.

### Lineage (build order)

```
RAW.YELLOW_TRIPDATA (source) ─> stg_yellow_trips ─┐
                                                  ├─> int_trips_enriched ─> fct_trips ─┬─> agg_daily_revenue
taxi_zone_lookup (seed) ─────> stg_taxi_zones ────┘                                   └─> agg_zone_performance
taxi_zone_lookup (seed) ─────> stg_taxi_zones ───────────────────────────> dim_zones
```

`dim_zones` is built from the zone **seed**, not from facts. `fct_trips` relates
to it by FK (`pickup_location_id` / `dropoff_location_id`) — a *tested
relationship*, not a build dependency.

### The star schema (marts)

| Model | Grain / PK | Contents |
|---|---|---|
| `dim_zones` | 1 row per zone / `location_id` | Zone, borough, service zone |
| `fct_trips` | 1 row per valid trip / `trip_id` | FKs + measures (fare, tip, total, distance, duration, …) |
| `agg_daily_revenue` | 1 row per day / `trip_date` | Trips, fare, tips, `tip_rate_pct` |
| `agg_zone_performance` | 1 row per (zone, month) / composite | Trips, avg distance/fare, revenue, `revenue_rank`, `is_high_volume_zone` |

### Row counts (full 2023 build)

| Stage | Rows |
|---|---|
| Raw `YELLOW_TRIPDATA` | 38,310,226 |
| `stg_yellow_trips` (after exact-dup dedup) | 38,310,220 |
| `int_trips_enriched` / `fct_trips` (after validity filter) | 35,468,129 |
| `agg_daily_revenue` | 374 |
| `agg_zone_performance` | 3,076 |
| `dim_zones` | 265 |

---

## 5. Testing

`dbt build` runs **22 data tests**, all passing:

- **PK integrity** — `not_null` + `unique` on every single-column PK
  (`stg_*`, `int_trips_enriched`, `dim_zones`, `fct_trips`, `agg_daily_revenue`);
  composite PK on `agg_zone_performance` via `dbt_utils.unique_combination_of_columns`.
- **Referential integrity** — `relationships` from both `fct_trips` FKs to
  `dim_zones.location_id`. Kept as **hard** tests: empirically there are no
  out-of-range LocationIDs, and a passing hard test is a stronger guarantee than
  a warn.
- **Domain** — `accepted_values` on `payment_type` (1–6), placed on `fct_trips`
  (post-filter) rather than staging, where raw TLC can carry `payment_type = 0`.
- **Custom singular test** (`tests/assert_total_amount_gte_fare_amount.sql`) —
  asserts no trip has `total_amount < fare_amount`.
- **Custom generic test** (`macros/test_value_in_range.sql`) — parametrised
  `value_in_range(min_value, max_value)` on `fct_trips.trip_duration_minutes`,
  fed by the project vars so the test and the validity filter share one source
  of truth.

---

## 6. Data-quality fix: raw timestamp re-ingestion

While wiring source freshness, `max(tpep_pickup_datetime)` came back as the year
**54,009,434**. Investigation showed the TLC parquet stores both datetimes as
**INT64 microseconds since epoch**, but the original `COPY INTO` read them as
**seconds** — so *every* timestamp was ~1,000,000× too large.

Because dbt only transforms (it doesn't own raw ingestion), the raw layer was
re-ingested correctly from the 12 monthly parquet files already staged in
Snowflake:

```sql
-- corrected timestamp transform (scale 6 = microseconds)
to_timestamp_ntz($1:tpep_pickup_datetime::number, 6)
```

The fix was applied via a side table + **atomic `ALTER TABLE … SWAP WITH`**
(validated identical row count 38,310,226 and a sane 2001→2024 date range
first), so `RAW.YELLOW_TRIPDATA` is now correct and `stg_yellow_trips` uses a
plain timestamp cast — no scaling workaround in dbt. Freshness now correctly
reports `STALE` (the 2023 data is static, so it is intentionally always older
than the 24h/48h thresholds — expected and documented, not a bug).

---

## 7. Brainstormers

### 7a. Revenue rank — within-month vs across-the-year

**Question:** `agg_zone_performance` needs a revenue rank. `RANK()` across the
whole dataset gives a different answer than ranking within each month. Which is
more useful, and why?

**Answer — rank *within each month*:**

```sql
rank() over (partition by trip_month order by sum(total_amount) desc)
```

- The table drives **monthly operational decisions** (where to push drivers and
  vehicles), and NYC demand is **strongly seasonal** — a zone that ranks #3 in
  December can sit at #15 in July. A whole-year rank collapses those swings into
  a single stale annual number that hides exactly the seasonality ops plans
  around.
- Per-month ranks **compose upward**: they roll up into an annual leaderboard.
  But a single annual rank **cannot be split back** into months. Partitioning by
  month therefore carries strictly more information — it is the composable,
  actionable choice.

### 7b. Deployment — blue/green (write-audit-publish)

The Airflow DAG builds the **entire** transform into an **audit schema**
(`DBT_AUDIT`), runs all tests there, and only then promotes to the **prod
schema** (`DBT`). Consumers always read `DBT`, which only ever flips to a fully
built-and-tested copy.

- **Write** — `staging`, `intermediate`, `marts` all build into `DBT_AUDIT`.
  (Auditing the *whole* build, not just marts, keeps cross-schema `ref()`s
  coherent — flipping only the marts' schema would break their references to
  staging/intermediate.)
- **Audit** — `dbt test` runs against `DBT_AUDIT`. A non-zero exit fails the DAG
  and blocks publish.
- **Publish** — `publish_swap` promotes `DBT_AUDIT → DBT` via Snowflake's atomic
  `ALTER TABLE … SWAP WITH …` (metadata-only, so readers never see a partial
  publish). Implemented as a clearly-commented stub; this section is the full
  design.

This gives **zero-downtime, all-or-nothing** deploys: bad data never reaches
consumers, and rollback is just another swap.

---

## 8. Orchestration (Airflow DAG)

`dags/nyc_taxi_daily_pipeline.py` — `dag_id="nyc_taxi_daily_pipeline"`,
`schedule="0 2 * * *"` (02:00 UTC), `catchup=False`,
`default_args` with `retries=2`, `retry_delay=5m`, `email_on_failure=True`.

```
check_source_freshness → run_dbt_staging → run_dbt_intermediate
    → run_dbt_marts → run_dbt_tests → publish_swap → notify_success
```

- **`check_source_freshness`** — `PythonOperator` keyed off the logical date
  (`{{ ds }}`); per-date row-count guard. Since the dataset is a static 2023
  bulk load, it runs as a parameterized/simulated check (`SIMULATE_FRESHNESS`):
  it passes if the bulk load is present, with a comment marking where a real
  daily feed would hard-fail on a missing partition.
- **`run_dbt_*`** — `BashOperator`s invoking dbt per layer, building into the
  audit schema.
- **`run_dbt_tests`** — the test gate; non-zero exit fails the DAG.
- **`publish_swap`** — audit→prod promotion (stub; see §7b).
- **`notify_success`** — logs the day's `total_trips` + revenue from
  `agg_daily_revenue` for the logical date.

**Credentials** are pulled from an Airflow **Connection** at run time and
injected as `SNOWFLAKE_*` env vars into the `BashOperator`s (so dbt's
`profiles.yml` `env_var()` placeholders resolve) — nothing hardcoded in the DAG.

**Backfill-safe:** every date-dependent task uses the Airflow logical date
(`{{ ds }}` / `data_interval_start`), never `datetime.now()`, so historical
runs are deterministic and idempotent.

---

## 9. Key trade-offs & decisions

| Decision | Rationale |
|---|---|
| **`tip_rate_pct = tips / fare`** (not / total) | Measures tipping behaviour against the metered fare. Tips-over-total would shrink as tolls/surcharges grow, conflating tipping with trip composition. Divide-by-zero guarded with `nullif`. |
| **Keep unknown zones** (LEFT JOIN + `coalesce(…, 'Unknown')`) | A trip with an unmatched LocationID is real revenue; dropping it would silently understate totals. We retain it labelled `Unknown` instead. |
| **`passenger_count > 0`** in the validity filter | Intentionally also drops NULLs (`NULL > 0` is not true). A trip with zero/unknown passengers is treated as implausible for the analytical marts. |
| **`trip_id` dedup** (surrogate key + `qualify row_number() = 1`) | TLC ships exact-duplicate rows; deduping on the surrogate key keeps the grain at one row per trip and lets the `unique` test hold. |
| **`NUMBER(38,2)` for money** | Exact decimal cents — avoids binary-float rounding drift in `SUM()`/`AVG()` across 35M rows. Distance also fixed-precision. |
| **`datediff('second', …)/60.0`** for duration | Fractional minutes. `datediff('minute', …)` truncates, which would distort the duration validity bounds. |
| **intermediate `view` (not `ephemeral`)** | One extra cheap view buys an inspectable object and a clean Airflow task boundary. |
| **Hard `relationships` tests** | No out-of-range IDs in practice; a passing hard test beats a warn. |

---

## 10. AI-tools note

This project was built with AI assistance (Claude Code) used as a pair-programming
and review aid — scaffolding models, drafting tests and the DAG, and pressure-testing
edge cases. Every model, materialization, and LOCKED decision was specified by me
and reviewed file-by-file before acceptance; the AI did not make modelling or
business-logic choices unilaterally. The raw-timestamp data-quality issue (§6) is a
good example: the AI surfaced the anomaly during freshness setup, but the decision to
re-ingest raw (vs. patch in staging) was made explicitly and confirmed before any
change to the warehouse.
