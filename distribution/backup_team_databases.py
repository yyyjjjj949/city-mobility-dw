"""Take logical database backups with the local queue and BI/scheduler stopped."""
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
from publish_marts import ClickHouse,columns,normal
TARGET=Path(r'D:\CodexData\workspaces\city-mobility-share-2026-11-14\work\CityMobility')
DOCKER=r'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'
os.environ['DOCKER_CONFIG']=r'D:\CodexData\app-data\docker-profile\cli'

def main():
    out=TARGET/'backups';(out/'clickhouse').mkdir(parents=True,exist_ok=True)
    with (out/'backup.log').open('w',encoding='utf-8') as log:
        subprocess.run(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File',str(ROOT/'Manage-Orchestration.ps1'),'-Action','Stop'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True)
    state=ROOT/'work/orchestration/worker-state.json'
    for _ in range(120):
        if not json.loads(state.read_text(encoding='utf-8'))['running']:break
        time.sleep(1)
    assert not json.loads(state.read_text(encoding='utf-8'))['running'],'Worker still busy'
    subprocess.run([DOCKER,'compose','stop','airflow','superset'],cwd=ROOT,check=True)
    for name in ['superset','airflow']:
        with (out/(name+'.dump')).open('wb') as stream:
            subprocess.run([DOCKER,'compose','exec','-T','postgres','pg_dump','-U','platform','-Fc','--no-owner','--no-acl',name],cwd=ROOT,stdout=stream,check=True)
        print('POSTGRES_BACKUP_OK '+name,flush=True)
    ch=ClickHouse();pointers=ch.query('SELECT dataset_id,release_id FROM ads.release_pointer ORDER BY dataset_id')
    release=json.loads((ROOT/'outputs/marts-latest.json').read_text(encoding='utf-8'))['release_id']
    assert len(pointers)==2 and all(r['release_id']==release for r in pointers)
    report={'created_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'release_id':release,'pointers':pointers,'tables':{}}
    for name in ['trip_day','trip_hour','zone_hour','od_day','weather_group','quality_batch','quality_rule','release_pointer']:
        table=columns(name);count=total=xor=0
        condition='' if name=='release_pointer' else " WHERE release_id='"+release+"'"
        response=ch.request('SELECT * FROM ads.'+name+condition+' FORMAT JSONEachRow',stream=True)
        try:
            with gzip.open(out/'clickhouse'/(name+'.jsonl.gz'),'wb',compresslevel=1) as stream:
                for line in response.iter_lines():
                    if not line:continue
                    row=json.loads(line);digest=int.from_bytes(hashlib.sha256(normal(row,table)).digest(),'big')
                    count+=1;total=(total+digest)%(1<<256);xor^=digest
                    stream.write(line+b'\n')
        finally:response.close()
        expected=int(ch.query('SELECT count() AS n FROM ads.'+name+condition)[0]['n']);assert expected==count
        report['tables'][name]={'rows':count,'checksum':{'rows':count,'sha256_sum':f'{total:064x}','sha256_xor':f'{xor:064x}'}}
        print('CLICKHOUSE_BACKUP_OK '+name+' rows='+str(count),flush=True)
    assert ch.query('SELECT dataset_id,release_id FROM ads.release_pointer ORDER BY dataset_id')==pointers
    (out/'snapshot.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print('BACKUP_COMPLETE',flush=True)
if __name__=='__main__':main()
