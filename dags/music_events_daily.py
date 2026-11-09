"""Daily API extraction -> validated staging -> warehouse publication."""
import os
from datetime import timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from pendulum import datetime

from music_warehouse.pipeline import land, curate, publish

@dag(dag_id="music_events_daily", schedule="@daily", start_date=datetime(2026,1,1,tz="UTC"),
     catchup=False, max_active_runs=1, default_args={"retries":1,"retry_delay":timedelta(seconds=10)},
     tags=["data-engineering","music","warehouse"])
def music_events_daily():
    @task
    def extract_api():
        ctx=get_current_context()
        day=ctx["data_interval_start"].date().isoformat()
        root=os.getenv("MUSIC_RUN_ROOT","state/runs")
        directory=land(day,root,ctx["run_id"],os.getenv("MUSIC_API_BASE"))
        return {"date":day,"directory":directory}

    @task
    def validate_events(batch):
        curate(batch["date"],batch["directory"])
        return batch

    @task
    def load_and_reconcile(batch):
        database=Path(os.getenv("MUSIC_DUCKDB_PATH","state/music.duckdb"))
        database.parent.mkdir(parents=True,exist_ok=True)
        return publish(batch["date"],batch["directory"],database,os.getenv("MUSIC_BIGQUERY_DATASET"))

    load_and_reconcile(validate_events(extract_api()))

daily=music_events_daily()
if __name__=="__main__":
    run=daily.test(execution_date=datetime(2026,11,8,tz="UTC"))
    if run.state!="success":
        raise SystemExit(f"DAG run failed: {run.state}")
