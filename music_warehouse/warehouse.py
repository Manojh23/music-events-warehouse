"""Shared warehouse SQL with local DuckDB and optional BigQuery adapters."""
from datetime import date
import re
import uuid

SCHEMAS={
    "tracks":[("track_id","STRING"),("artist_id","STRING"),("title","STRING"),("duration_seconds","INT64")],
    "events":[("event_id","STRING"),("track_id","STRING"),("listener_id","STRING"),("event_date","DATE"),("seconds_played","INT64")],
    "artist_daily":[("event_date","DATE"),("artist_id","STRING"),("plays","INT64"),("unique_listeners","INT64"),("seconds_played","INT64")],
}
NAMES={"STRING":"VARCHAR","INT64":"BIGINT","DATE":"DATE"}

def ident(table,namespace=None):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*",table):
        raise ValueError("Invalid table identifier")
    if namespace is None:
        return f'"{table}"'
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,62}\.[A-Za-z_][A-Za-z0-9_]*",namespace):
        raise ValueError("Expected a Google Cloud project ID and BigQuery dataset")
    return f"`{namespace}.{table}`"

def ddl(table,kind,namespace=None):
    fields=SCHEMAS[kind]
    types=({k:k for k in NAMES} if namespace else NAMES)
    cols=", ".join(f"{name} {types[typ]}" for name,typ in fields)
    partition = " PARTITION BY event_date CLUSTER BY track_id" if namespace and table=="events" else ""
    return f"CREATE TABLE IF NOT EXISTS {ident(table,namespace)} ({cols}){partition}"

def plan(day,stages,namespace=None):
    day=date.fromisoformat(day).isoformat()
    statements=[]
    for name,key in (("tracks","track_id"),("events","event_id")):
        t=ident(name,namespace)
        s=ident(stages[name],namespace)
        cols=[col for col,_ in SCHEMAS[name]]
        changes=", ".join(f"{c}=s.{c}" for c in cols if c!=key)
        insert=", ".join(cols)
        values=", ".join(f"s.{c}" for c in cols)
        on = f"t.{key}=s.{key}" + (" AND t.event_date=s.event_date" if name=="events" else "")
        statements.append(f"MERGE INTO {t} AS t USING {s} AS s ON {on} "
            f"WHEN MATCHED THEN UPDATE SET {changes} WHEN NOT MATCHED THEN INSERT ({insert}) VALUES ({values})")
    daily=ident("artist_daily",namespace)
    events=ident("events",namespace)
    tracks=ident("tracks",namespace)
    where=f"DATE '{day}'"
    statements += [
        f"DELETE FROM {daily} WHERE event_date={where}",
        f"INSERT INTO {daily} (event_date, artist_id, plays, unique_listeners, seconds_played) "
        f"SELECT e.event_date,t.artist_id,COUNT(*),COUNT(DISTINCT e.listener_id),SUM(e.seconds_played) "
        f"FROM {events} AS e JOIN {tracks} AS t ON e.track_id=t.track_id "
        f"WHERE e.event_date={where} GROUP BY e.event_date,t.artist_id",
    ]
    return statements

def reconciliation(day,namespace=None):
    day=date.fromisoformat(day).isoformat()
    return (f"SELECT (SELECT COUNT(*) FROM {ident('events',namespace)} WHERE event_date=DATE '{day}'),"
            f"(SELECT COALESCE(SUM(plays),0) FROM {ident('artist_daily',namespace)} WHERE event_date=DATE '{day}')")

def local(database,day,clean):
    import duckdb
    stages={kind:f"stage_{kind}" for kind in ("tracks","events")}
    with duckdb.connect(str(database)) as con:
        con.execute("BEGIN TRANSACTION")
        try:
            for kind in SCHEMAS:
                con.execute(ddl(kind,kind))
            for kind,stage in stages.items():
                columns=[name for name,_ in SCHEMAS[kind]]
                con.execute(ddl(stage,kind).replace("CREATE TABLE","CREATE TEMP TABLE",1))
                con.executemany(f"INSERT INTO {ident(stage)} VALUES ({','.join('?' for _ in columns)})",
                                [[row[col] for col in columns] for row in clean[kind]])
            for sql in plan(day,stages):
                con.execute(sql)
            counts=con.execute(reconciliation(day)).fetchone()
            if counts[0]!=counts[1] or counts[0]<len(clean["events"]):
                raise ValueError(f"Warehouse reconciliation failed: {counts}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return {"warehouse_events":counts[0],"mart_plays":counts[1],"engine":"duckdb"}

def bigquery_load(namespace,day,clean):
    from google.cloud import bigquery
    from google.cloud.bigquery.enums import WriteDisposition
    ident("tracks",namespace)
    project,_=namespace.split(".")
    client=bigquery.Client(project=project)
    suffix=uuid.uuid4().hex[:12]
    stages={k:f"stage_{k}_{suffix}" for k in ("tracks","events")}
    # Existing dataset and Application Default Credentials are required.
    try:
        for kind in SCHEMAS:
            client.query(ddl(kind,kind,namespace)).result()
        for kind,stage in stages.items():
            schema=[bigquery.SchemaField(col,typ) for col,typ in SCHEMAS[kind]]
            conf=bigquery.LoadJobConfig(schema=schema,write_disposition=WriteDisposition.WRITE_TRUNCATE)
            client.load_table_from_json(clean[kind],f"{namespace}.{stage}",job_config=conf).result()
        script=";\n".join(["BEGIN TRANSACTION",*plan(day,stages,namespace),"COMMIT TRANSACTION"])+";"
        client.query(script).result()
        row=next(iter(client.query(reconciliation(day,namespace)).result()))
        if row[0]!=row[1] or row[0]<len(clean["events"]):
            raise ValueError(f"BigQuery warehouse reconciliation failed: {row}")
        return {"warehouse_events":row[0],"mart_plays":row[1],"engine":"bigquery"}
    finally:
        for stage in stages.values():
            client.delete_table(f"{namespace}.{stage}",not_found_ok=True)
