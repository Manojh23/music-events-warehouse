"""Raw landing, validation, quality gates, and replayable batch publication."""
from argparse import ArgumentParser
from datetime import date
from hashlib import sha256
import json
from pathlib import Path
from time import time_ns

from .source import extract, mock_api
from .warehouse import local,bigquery_load

def write(path,value):
    path=Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(".tmp")
    temp.write_text(json.dumps(value,indent=2)+"\n")
    temp.replace(path)

def read(path):
    return json.loads(Path(path).read_text())

def land(day,root,run,base=None):
    token=sha256(run.encode()).hexdigest()[:12]
    directory=Path(root)/date.fromisoformat(day).isoformat()/token
    if base:
        raw=extract(base,day)
    else:
        with mock_api(day) as (url,_):
            raw=extract(url,day)
    write(directory/"raw.json",raw)
    return str(directory)

def validate(day,raw):
    required=("track_id","artist_id","title","duration_seconds")
    tracks={}
    for row in raw["tracks"]:
        if not isinstance(row,dict) or any(not isinstance(row.get(k),str) or not row[k].strip() for k in required[:3]):
            raise ValueError("Invalid catalog record")
        if type(row.get("duration_seconds")) is not int or row["duration_seconds"]<=0:
            raise ValueError("Invalid track duration")
        item={k:row[k] for k in required}
        if row["track_id"] in tracks and tracks[row["track_id"]]!=item:
            raise ValueError("Conflicting track ID")
        tracks[row["track_id"]]=item
    if not tracks:
        raise ValueError("Empty catalog")
    events={}
    rejects=[]
    duplicates=0
    for row in raw["events"]:
        reason=None
        if not isinstance(row,dict) or any(not isinstance(row.get(k),str) or not row[k].strip()
                 for k in ("event_id","track_id","listener_id","event_date")):
            reason="missing_or_invalid_identifier"
        elif row["event_date"]!=day:
            reason="outside_processing_date"
        elif row["track_id"] not in tracks:
            reason="unknown_track"
        elif type(row.get("seconds_played")) is not int or not 0<row["seconds_played"]<=tracks[row["track_id"]]["duration_seconds"]:
            reason="invalid_seconds_played"
        if reason:
            rejects.append({"reason":reason,"row":row})
            continue
        item={k:row[k] for k in ("event_id","track_id","listener_id","event_date","seconds_played")}
        key=item["event_id"]
        if key in events:
            if events[key]!=item:
                raise ValueError(f"Conflicting event ID: {key}")
            duplicates+=1
        else:
            events[key]=item
    summary={"date":day,"raw_events":len(raw["events"]),"accepted_events":len(events),
             "duplicates":duplicates,"quarantined":len(rejects),"catalog_tracks":len(tracks)}
    return {"tracks":list(tracks.values()),"events":list(events.values())},rejects,summary

def curate(day,directory):
    directory=Path(directory)
    raw=read(directory/"raw.json")
    clean,rejects,summary=validate(day,raw)
    passed=bool(summary["accepted_events"] and summary["quarantined"]/max(1,summary["raw_events"])<=.10)
    write(directory/"quarantine.json",rejects)
    write(directory/"quality.json",{**summary,"passed":passed})
    if not passed:
        raise ValueError(f"Data quality gate failed: {summary}")
    write(directory/"clean.json",clean)
    return str(directory)

def publish(day,directory,database,namespace=None):
    directory=Path(directory)
    quality=read(directory/"quality.json")
    if quality["date"]!=day or not quality["passed"]:
        raise ValueError("Publication requires a passing quality gate for this date")
    clean=read(directory/"clean.json")
    result=bigquery_load(namespace,day,clean) if namespace else local(database,day,clean)
    report={**quality,**result}
    write(directory/"run_report.json",report)
    return report

if __name__=="__main__":
    parser=ArgumentParser(description=__doc__)
    parser.add_argument("--date",default="2026-11-08")
    parser.add_argument("--root",default="state/runs")
    parser.add_argument("--database",default="state/music.duckdb")
    parser.add_argument("--api")
    parser.add_argument("--bigquery-dataset",help="Optional project.dataset with ADC")
    args=parser.parse_args()
    Path(args.database).parent.mkdir(parents=True,exist_ok=True)
    run=land(args.date,args.root,str(time_ns()),args.api)
    curate(args.date,run)
    report=publish(args.date,run,args.database,args.bigquery_dataset)
    print(json.dumps(report,indent=2))
