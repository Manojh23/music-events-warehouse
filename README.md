# Music Events Warehouse

A portfolio pipeline that turns simulated catalog and listening events into a queryable daily artist mart. The same SQL plan runs against DuckDB locally and has a BigQuery adapter for an existing Google Cloud dataset. The fixtures are fictional and contain no customer data.

### Run the pipeline

From the project root with Python 3.12:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m music_warehouse.pipeline --date 2026-11-08
python -m unittest discover -s tests -v
```

Inspect the mart:

```bash
python -c "import duckdb; print(duckdb.connect('state/music.duckdb').sql('SELECT * FROM artist_daily ORDER BY event_date, artist_id'))"
```

Re-run the same date: the event merge and artist mart rebuild leave totals unchanged. To load another date, supply a different ISO date. Each run retains raw JSON, accepted records, quarantined rows, quality metrics, and a run report under `state/runs/`.

### Airflow

```bash
pip install -r requirements-airflow.txt
export AIRFLOW_HOME="$PWD/airflow-home"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
airflow db migrate
python -m dags.music_events_daily
```

The DAG uses its UTC data interval start to select the source partition. Three tasks extract the API batch, validate the records, and load and reconcile the mart. The test logical date of November 8 reads the November 7 partition. The filesystem handoff and DuckDB backend are intended for a local single writer. Distributed workers would need shared object storage for staging.

### BigQuery adapter

The adapter creates the target tables in an **existing** dataset, loads cleaned JSON into per-run staging tables with typed schemas, and executes SQL merges and a daily mart refresh in a transaction. The events table is partitioned by date and clustered by track ID. Staging tables are cleaned up when the run exits. In an environment where your application credentials can write to that dataset and submit BigQuery jobs:

```bash
pip install -r requirements-bigquery.txt
python -m music_warehouse.pipeline --date 2026-11-08 --bigquery-dataset your-project.analytics
```

The BigQuery SQL is parsed in tests. The shared transformation logic and Airflow workflow were executed locally on DuckDB. The BigQuery adapter was not run against a live GCP project; using it can incur Google Cloud charges.

### Data flow and checks

- **Extract:** Python reads two paginated HTTP endpoints, retries transient failures and rejects a repeated cursor. The default source is a local synthetic REST API; `--api` can point to a compatible endpoint.
- **Validate:** The pipeline deduplicates event keys, checks track references, validates durations and the batch date, and quarantines invalid rows with a reason. A batch with no accepted events or more than 10% rejected events is blocked.
- **Load and reconcile:** SQL merges dimensions and event facts, rebuilds artist plays, unique listeners and listening seconds for the batch date, and reconciles play totals to event counts.
- **Verified run:** [The saved report](examples/run_report.json) shows 65 source events, 60 accepted, two duplicates, three quarantined, and 60 reconciled plays. All three Airflow tasks and eight local tests passed. The CI configuration is included for future repository runs.
- **AI-assisted workflow:** Codex helped scaffold the pipeline and draft failure-path checks. Runtime results and SQL parsing were verified separately.
