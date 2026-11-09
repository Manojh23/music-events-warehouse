"""Synthetic paginated REST API and an extractor with bounded retries."""
from contextlib import contextmanager
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Thread
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.request import urlopen

def fixtures(day):
    date.fromisoformat(day)
    tracks = [{"track_id":f"track-{i}","artist_id":f"artist-{(i-1)//2+1}",
               "title":f"Demo track {i}","duration_seconds":180+i*7} for i in range(1,7)]
    events = [{"event_id":f"{day}-{i:03}","track_id":f"track-{i%6+1}",
               "listener_id":f"demo-{i%19:02}","event_date":day,
               "seconds_played":30+(i*17)%145} for i in range(60)]
    events += [events[3].copy(),events[9].copy(),
               {**events[0],"event_id":"bad-track","track_id":"unknown"},
               {**events[1],"event_id":"bad-time","seconds_played":-20},
               {**events[2],"event_id":"bad-date","event_date":"2020-01-01"}]
    return {"tracks":tracks,"events":events}

@contextmanager
def mock_api(day, fail_first=False):
    data=fixtures(day)
    requests=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*_):
            pass
        def do_GET(self):
            path=urlparse(self.path)
            endpoint=path.path.removeprefix("/")
            args=parse_qs(path.query)
            requests.append(endpoint)
            if fail_first and len(requests)==1:
                self.send_error(503)
                return
            if endpoint not in data or args.get("date") != [day]:
                self.send_error(404)
                return
            offset=int(args.get("cursor",["0"])[0])
            rows=data[endpoint]
            payload={"items":rows[offset:offset+11],
                     "next_cursor":str(offset+11) if offset+11<len(rows) else None}
            body=json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    server=ThreadingHTTPServer(("127.0.0.1",0),Handler)
    thread=Thread(target=server.serve_forever,kwargs={"poll_interval":.02},daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}",requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

def page(url):
    for attempt in range(3):
        try:
            with urlopen(url,timeout=8) as response:
                return json.load(response)
        except (HTTPError,URLError,TimeoutError) as err:
            if (isinstance(err,HTTPError) and err.code not in {429,500,502,503,504}) or attempt==2:
                raise
            retry=err.headers.get("Retry-After") if isinstance(err,HTTPError) else None
            sleep(min(5,int(retry)) if retry and retry.isdigit() else .15*2**attempt)

def extract(base,day):
    date.fromisoformat(day)
    result={}
    for endpoint in ("tracks","events"):
        cursor=None
        seen=set()
        rows=[]
        for _ in range(100):
            query={"date":day}
            if cursor is not None:
                query["cursor"]=cursor
            payload=page(f"{base.rstrip('/')}/{endpoint}?{urlencode(query)}")
            if not isinstance(payload.get("items"),list):
                raise ValueError("API page is missing an items array")
            rows.extend(payload["items"])
            cursor=payload.get("next_cursor")
            if cursor is None:
                break
            if not isinstance(cursor,str) or cursor in seen:
                raise ValueError("Invalid or repeated page cursor")
            seen.add(cursor)
        else:
            raise ValueError("API page limit exceeded")
        result[endpoint]=rows
    return result
