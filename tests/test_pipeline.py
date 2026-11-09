import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
from sqlglot import parse

from music_warehouse.source import extract,fixtures,mock_api
from music_warehouse.pipeline import curate,land,publish,validate,write
from music_warehouse.warehouse import ddl,plan,reconciliation

DAY="2026-11-08"

class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.db=self.root/"music.duckdb"

    def run_day(self,day,run):
        directory=land(day,self.root/"runs",run)
        curate(day,directory)
        return publish(day,directory,self.db)

    def test_pagination_and_retry(self):
        with mock_api(DAY,fail_first=True) as (url,requests):
            result=extract(url,DAY)
        self.assertEqual(result,fixtures(DAY))
        self.assertEqual(requests.count("tracks"),2)
        self.assertEqual(requests.count("events"),6)

    def test_repeated_cursor_fails_fast(self):
        with patch("music_warehouse.source.page",return_value={"items":[],"next_cursor":"loop"}):
            with self.assertRaisesRegex(ValueError,"repeated"):
                extract("http://unused",DAY)

    def test_quarantine_and_dedup(self):
        clean,rejects,s=validate(DAY,fixtures(DAY))
        self.assertEqual((s["raw_events"],s["accepted_events"],s["duplicates"],s["quarantined"]),(65,60,2,3))
        self.assertEqual({r["reason"] for r in rejects},{"unknown_track","invalid_seconds_played","outside_processing_date"})
        self.assertEqual(len({r["event_id"] for r in clean["events"]}),60)

    def test_conflicting_duplicate_rejected(self):
        raw=fixtures(DAY)
        raw["events"].append({**raw["events"][0],"seconds_played":177})
        with self.assertRaisesRegex(ValueError,"Conflicting event ID"):
            validate(DAY,raw)

    def test_quality_gate_prevents_publication(self):
        path=self.root/"failed"
        raw=fixtures(DAY)
        raw["events"]=[{"event_id":"bad"}]*10
        write(path/"raw.json",raw)
        with self.assertRaisesRegex(ValueError,"quality gate"):
            curate(DAY,path)
        self.assertEqual(len(json.loads((path/"quarantine.json").read_text())),10)
        with self.assertRaisesRegex(ValueError,"passing quality"):
            publish(DAY,path,self.db)
        self.assertFalse(self.db.exists())

    def test_idempotent_load_and_artist_mart(self):
        a=self.run_day(DAY,"first")
        b=self.run_day(DAY,"retry")
        self.assertEqual(a,b)
        self.assertEqual((b["warehouse_events"],b["mart_plays"]),(60,60))
        with duckdb.connect(str(self.db)) as con:
            counts=con.execute("SELECT artist_id,plays FROM artist_daily ORDER BY artist_id").fetchall()
            self.assertEqual(counts,[("artist-1",20),("artist-2",20),("artist-3",20)])
            self.assertEqual(con.execute("SELECT COUNT(*) FROM events").fetchone()[0],60)

    def test_two_day_partitions_and_backfill(self):
        for i,day in enumerate([DAY,"2026-11-09",DAY]):
            self.run_day(day,f"run-{i}")
        with duckdb.connect(str(self.db)) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM events").fetchone()[0],120)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM artist_daily").fetchone()[0],6)

    def test_bigquery_dialect_and_safe_identifiers(self):
        ns="demo-music.analytics"
        stages={"tracks":"stage_tracks","events":"stage_events"}
        sql=[ddl("events","events",ns),*plan(DAY,stages,ns),reconciliation(DAY,ns)]
        self.assertTrue(all(len(parse(s,read="bigquery"))==1 for s in sql))
        self.assertIn("PARTITION BY event_date",sql[0])
        with self.assertRaises(ValueError):
            ddl("events","events","bad; DROP TABLE events")

if __name__=="__main__":
    unittest.main()
