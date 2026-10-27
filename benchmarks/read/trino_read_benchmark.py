#!/usr/bin/env python3
"""Benchmark Trino queries against a fixed Iceberg snapshot (stdlib only).

Measures the *whole* Trino -> Iceberg catalog -> Ozone path, not Ozone alone.
"""
from __future__ import annotations
import argparse
import csv
import json
import math
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def percentile(values, q):
    if not values: return None
    v=sorted(values); x=(len(v)-1)*q; lo=math.floor(x); hi=math.ceil(x)
    return v[lo] if lo==hi else v[lo]*(hi-x)+v[hi]*(x-lo)


def get_json(request, timeout):
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def query_trino(endpoint, sql, user, timeout):
    base=endpoint.rstrip('/')
    req=Request(base+'/v1/statement', data=sql.encode('utf-8'), method='POST',
        headers={'X-Trino-User':user, 'X-Trino-Catalog':'lakehouse',
                 'X-Trino-Schema':'bronze','Content-Type':'text/plain; charset=utf-8'})
    obj=get_json(req, timeout); qid=obj.get('id')
    while True:
        if obj.get('error'):
            err=obj['error']
            raise RuntimeError('Trino '+str(qid)+': '+str(err.get('message',err)))
        next_uri=obj.get('nextUri')
        if not next_uri: return {'query_id':qid, 'stats':obj.get('stats') or {}}
        a,b=urlsplit(next_uri),urlsplit(base)
        if (a.scheme,a.netloc)!=(b.scheme,b.netloc):
            raise RuntimeError('Trino nextUri is on a different host; refusing redirect')
        obj=get_json(Request(next_uri, headers={'X-Trino-User':user}),timeout)


def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--endpoint',default='http://localhost:8080')
    p.add_argument('--user',default='benchmark')
    p.add_argument('--table',default='lakehouse.bronze.ami_meter_raw')
    p.add_argument('--snapshot-id',type=int,required=True)
    p.add_argument('--duration-seconds',type=float,default=60)
    p.add_argument('--warmup-queries',type=int,default=3)
    p.add_argument('--modulus',type=int,default=100)
    p.add_argument('--timeout-seconds',type=float,default=60)
    p.add_argument('--output-root',default='benchmarks/results')
    p.add_argument('--run-tag',default='baseline')
    a=p.parse_args(argv)
    if a.duration_seconds<=0 or a.warmup_queries<0 or a.modulus<=0:
        p.error('duration/modulus must be positive, warmup must be nonnegative')
    if not all(p.replace('_','').isalnum() for p in a.table.split('.')):
        p.error('--table must be a qualified identifier (without SQL expressions)')
    if not a.run_tag.replace('-','').replace('_','').isalnum():
        p.error('--run-tag must be alphanumeric, hyphen or underscore')
    return a


def make_sql(args, seq):
    return (f'SELECT COUNT(*) AS row_count FROM {args.table} '
            f'FOR VERSION AS OF {args.snapshot_id} '
            f'WHERE MOD(kafka_offset, {args.modulus}) = {seq % args.modulus}')


def run(args, client=query_trino):
    run_id=datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-trino-'+args.run_tag
    out=Path(args.output_root)/run_id
    out.mkdir(parents=True,exist_ok=False)
    manifest={'run_id':run_id,'created_at_utc':now_iso(),
              'type':'trino-fixed-snapshot-read','snapshot_id':args.snapshot_id,
              'table':args.table,'endpoint':args.endpoint,
              'duration_seconds':args.duration_seconds,'warmup_queries':args.warmup_queries,
              'modulus':args.modulus,'query_pattern':'MOD(kafka_offset, modulus) = residue',
              'interpretation':'End-to-end Trino/Iceberg/Lakekeeper/Ozone read-path latency; single client.'}
    (out/'benchmark_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n',encoding='utf-8')
    for i in range(args.warmup_queries):
        client(args.endpoint, make_sql(args,i),args.user,args.timeout_seconds)
    measurements=[]; seq=0; start=time.monotonic()
    while time.monotonic()-start<args.duration_seconds:
        t0=time.monotonic(); response={}; error=''
        try:
            response=client(args.endpoint,make_sql(args,seq),args.user,args.timeout_seconds)
        except Exception as exc:
            error=f'{type(exc).__name__}: {exc}'
        st=response.get('stats') or {}
        measurements.append({'timestamp':now_iso(),'sequence':seq,'residue':seq%args.modulus,
               'query_id':response.get('query_id') or '','success':not bool(error),
               'wall_latency_ms':round((time.monotonic()-t0)*1000,3),
               'elapsed_time_ms':st.get('elapsedTimeMillis',''),
               'queued_time_ms':st.get('queuedTimeMillis',''),
               'cpu_time_ms':st.get('cpuTimeMillis',''),
               'processed_rows':st.get('processedRows',''),
               'processed_bytes':st.get('processedBytes',''),'error':error})
        seq+=1
    elapsed=time.monotonic()-start
    with (out/'queries.csv').open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(measurements[0]));w.writeheader();w.writerows(measurements)
    latencies=[r['wall_latency_ms'] for r in measurements if r['success']]
    summary={'run_id':run_id,'snapshot_id':args.snapshot_id,
             'actual_duration_s':round(elapsed,3),'query_count':len(measurements),
             'successful_queries':len(latencies),'failed_queries':len(measurements)-len(latencies),
             'qps':round(len(measurements)/elapsed,3) if elapsed else None,
             'latency_avg_ms':round(statistics.mean(latencies),3) if latencies else None,
             'latency_p50_ms':percentile(latencies,0.5),'latency_p95_ms':percentile(latencies,0.95),
             'latency_p99_ms':percentile(latencies,0.99)}
    (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(summary,indent=2));print('Evidence:',out)
    return 0 if latencies and summary['failed_queries']==0 else 1


if __name__=='__main__':
    raise SystemExit(run(parse_args()))
