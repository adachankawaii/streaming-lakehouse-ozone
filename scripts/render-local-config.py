#!/usr/bin/env python3
"""Render ignored local S3 credentials into Trino, Flink and Lakekeeper config.

No network calls. Never commit the three generated outputs or .env.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import re

FILES = (
    'platform/flink/config/init.sql',
    'platform/iceberg/config/create-warehouse.json',
    'platform/trino/catalog/lakehouse.properties',
)
SAFE = re.compile(r'^[A-Za-z0-9._+=/@-]{8,256}$')


def read_env(path):
    if not path.is_file():
        raise ValueError(f'{path} is missing. Copy .env.example to .env and fill in secrets.')
    pairs={}
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        line=line.strip()
        if not line or line.startswith('#') or '=' not in line: continue
        key,value=line.split('=',1)
        pairs[key.strip()]=value.strip().strip('"').strip("'")
    return pairs


def render(root):
    env=read_env(root/'.env')
    keys={}
    for key in ('S3_ACCESS_KEY','S3_SECRET_KEY'):
        val=env.get(key,'')
        if not SAFE.fullmatch(val) or val.startswith('REPLACE'):
            raise ValueError(f'{key} must be set in .env to a real, non-placeholder lab credential')
        keys[key]=val
    for rel in FILES:
        template=root/(rel+'.template')
        content=template.read_text(encoding='utf-8')
        for key,val in keys.items():
            content=content.replace('{{'+key+'}}',val)
        if '{{S3_' in content: raise ValueError(f'Unresolved S3 template placeholder in {template}')
        path=root/rel
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text(content,encoding='utf-8',newline='\n')
        print('Rendered local (git-ignored):',rel)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    args=p.parse_args()
    try: render(args.root.resolve())
    except (ValueError,OSError) as exc: p.error(str(exc))


if __name__=='__main__': main()
