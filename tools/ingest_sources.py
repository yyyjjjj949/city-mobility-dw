"""Download official data on D:, preserve source identities, and prepare bounded ODS shards.

Usage: python tools/ingest_sources.py --month 2025-06 --proxy http://127.0.0.1:7897
Only this process uses --proxy. No Windows or Docker proxy settings are modified.
"""
from pathlib import Path
import argparse
import concurrent.futures
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import shutil
import subprocess
import time
import uuid
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT=Path(__file__).resolve().parents[1]
WORK=ROOT/'work/ingestion'
DOCKER=r'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'
UTC=dt.timezone.utc

def now():return dt.datetime.now(UTC).isoformat()
def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False,default=lambda x:x.isoformat(sep=' ') if isinstance(x,dt.datetime) else x.isoformat())
def key(*values):return hashlib.sha256(canonical(list(values)).encode()).hexdigest()
def file_hash(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(4*1024*1024),b''):h.update(block)
    return h.hexdigest()
def write_json(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str),encoding='utf-8');temp.replace(path)
def session(proxy):
    s=requests.Session();s.trust_env=False
    if proxy:s.proxies={'https':proxy,'http':proxy}
    s.headers['User-Agent']='CityMobilityCourseProject/1.0 (official public-data coursework)'
    return s

def download(spec,proxy):
    target=ROOT/spec['relative_path'];target.parent.mkdir(parents=True,exist_ok=True)
    sidecar=target.with_suffix(target.suffix+'.download.json')
    if target.exists() and sidecar.exists():
        meta=json.loads(sidecar.read_text(encoding='utf-8'))
        if meta['source_url']!=spec['url'] or meta['sha256']!=file_hash(target) or meta['bytes']!=target.stat().st_size:raise ValueError('Existing source checksum mismatch: '+str(target))
        print(f"DOWNLOAD_REUSED {spec['source_id']} {target.stat().st_size} bytes",flush=True)
        return {**spec,**meta,'download_reused':True}
    if target.exists():raise ValueError('Refuse to replace untracked source file: '+str(target))
    part=target.with_suffix(target.suffix+'.part');resume=target.with_suffix(target.suffix+'.resume.json')
    last_error=None
    for attempt in range(1,5):
        try:
            s=session(proxy)
            probe=s.get(spec['url'],headers={'Range':'bytes=0-0','Accept-Encoding':'identity'},timeout=(20,60),stream=True)
            try:
                probe.raise_for_status()
                total=int(probe.headers['Content-Range'].split('/')[-1]) if probe.status_code==206 else int(probe.headers.get('Content-Length',0))
                identity=probe.headers.get('ETag') or probe.headers.get('Last-Modified')
            finally:probe.close()
            if spec.get('expected_bytes') and total!=spec['expected_bytes']:raise ValueError(f"Remote size changed: {total} vs {spec['expected_bytes']}")
            previous=json.loads(resume.read_text()) if resume.exists() else None
            if part.exists() and (not previous or previous.get('identity')!=identity or previous.get('url')!=spec['url']):
                part.replace(part.with_name(part.name+'.stale-'+uuid.uuid4().hex[:8]))
            write_json(resume,{'url':spec['url'],'identity':identity,'total':total})
            offset=part.stat().st_size if part.exists() else 0
            if total and offset>total:raise ValueError('Partial file exceeds source size')
            headers={'Accept-Encoding':'identity'}
            if offset:
                headers['Range']=f'bytes={offset}-'
                if identity:headers['If-Range']=identity
            if not total or offset!=total:
                response=s.get(spec['url'],headers=headers,timeout=(20,90),stream=True)
                try:
                    response.raise_for_status()
                    if offset and (response.status_code!=206 or not response.headers.get('Content-Range','').startswith(f'bytes {offset}-')):
                        raise ValueError('Server did not honor safe resume; partial file preserved')
                    current_identity=response.headers.get('ETag') or response.headers.get('Last-Modified')
                    if identity and current_identity!=identity:raise ValueError('Source changed between probe and download')
                    last=time.monotonic()
                    with part.open('ab' if offset else 'wb') as output:
                        for chunk in response.iter_content(1024*1024):
                            if chunk:output.write(chunk);offset+=len(chunk)
                            if time.monotonic()-last>15:
                                print(f"DOWNLOAD_PROGRESS {spec['source_id']} {offset}/{total or '?'} bytes",flush=True);last=time.monotonic()
                finally:response.close()
            if total and part.stat().st_size!=total:raise ValueError('Incomplete download')
            meta={'source_url':spec['url'],'received_at_utc':now(),'sha256':file_hash(part),'bytes':part.stat().st_size,'etag_or_last_modified':identity}
            part.replace(target);write_json(sidecar,meta)
            print(f"DOWNLOAD_OK {spec['source_id']} {meta['bytes']} bytes SHA256={meta['sha256']}",flush=True)
            return {**spec,**meta,'download_reused':False}
        except Exception as exc:
            last_error=exc;print(f"DOWNLOAD_RETRY {spec['source_id']} attempt={attempt} {exc}",flush=True)
            if attempt<4:time.sleep(min(attempt*2,6))
    raise RuntimeError('Source download failed: '+spec['source_id']) from last_error

def specs(month):
    compact=month.replace('-','')
    catalog=json.loads((ROOT/'docs/data-source-catalog.json').read_text(encoding='utf-8'))
    taxi=next(x for x in catalog['sources'][0]['objects'] if x['month']==month)
    bike=next(x for x in catalog['sources'][1]['objects'] if x['month']==month)
    start=dt.date.fromisoformat(month+'-01');end=(start.replace(day=28)+dt.timedelta(days=4)).replace(day=1)-dt.timedelta(days=1)
    def spec(sid,url,name,kind,expected=None):return dict(source_id=sid,source_period=month,url=url,relative_path=f'data/ods/files/{sid}/{month}/{name}',kind=kind,expected_bytes=expected,poll_id=None)
    return [spec('tlc_yellow',taxi['url'],f'yellow_tripdata_{month}.parquet','parquet',taxi['object_bytes']),
      spec('citibike_trips',bike['url'],f'{compact}-citibike-tripdata.zip','csv_zip',bike['object_bytes']),
      spec('taxi_zones',catalog['sources'][2]['lookup_url'],'taxi_zone_lookup.csv','csv'),
      spec('taxi_zone_geometry',catalog['sources'][2]['geometry_url'],'taxi_zones.zip','geometry_zip'),
      spec('noaa_daily','https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&stations=USW00094728&startDate='+start.isoformat()+'&endDate='+end.isoformat()+'&format=json&units=metric&includeAttributes=true',f'USW00094728_{month}.json','json_array')]

def iter_source(obj):
    """Yield member, first source ordinal and bounded lists of original row dictionaries."""
    path=ROOT/obj['relative_path'];kind=obj['kind']
    if kind=='parquet':
        parquet=pq.ParquetFile(path);ordinal=1
        for batch in parquet.iter_batches(batch_size=32768):
            rows=batch.to_pylist();yield '',ordinal,rows;ordinal+=len(rows)
    elif kind in ('csv','csv_zip'):
        def read_csv(stream,member):
            reader=csv.DictReader(stream,restkey='__extra_fields__',restval=None);rows=[];ordinal=1
            if len(set(reader.fieldnames))!=len(reader.fieldnames):raise ValueError('Duplicate CSV header')
            for row in reader:
                if '__extra_fields__' in row or any(v is None for v in row.values()):raise ValueError('Malformed CSV at '+member+':'+str(ordinal+len(rows)))
                rows.append(row)
                if len(rows)==32768:yield member,ordinal,rows;ordinal+=len(rows);rows=[]
            if rows:yield member,ordinal,rows
        if kind=='csv':
            with path.open(encoding='utf-8-sig',newline='') as f:yield from read_csv(f,'')
        else:
            with zipfile.ZipFile(path) as archive:
                for member in obj['members']:
                    with archive.open(member['name']) as f:
                        with io.TextIOWrapper(f,encoding='utf-8-sig',newline='') as text:yield from read_csv(text,member['name'])
    elif kind=='json_array':
        rows=json.loads(path.read_text(encoding='utf-8-sig'))
        if not isinstance(rows,list):raise ValueError('Expected NOAA array')
        yield '',1,rows
    else:raise ValueError('Not a record source: '+kind)

def inspect_object(obj):
    path=ROOT/obj['relative_path'];kind=obj['kind'];members=[];expected_rows=None
    if kind=='parquet':
        p=pq.ParquetFile(path);expected_rows=p.metadata.num_rows
        mapped=json.loads((ROOT/'config/model/source-field-mapping.json').read_text(encoding='utf-8'))['tlc_yellow']
        if set(mapped)!=set(p.schema_arrow.names):raise ValueError('TLC schema changed')
    elif kind in ('csv_zip','geometry_zip'):
        with zipfile.ZipFile(path) as archive:
            for item in archive.infolist():
                if item.is_dir() or item.filename.startswith('__MACOSX/'):continue
                if kind=='geometry_zip' or item.filename.lower().endswith('.csv'):
                    members.append({'name':item.filename,'bytes':item.file_size,'crc32':f'{item.CRC:08x}'})
            if not members:raise ValueError('Archive has no source members')
            if kind=='geometry_zip':
                if archive.testzip() is not None:raise ValueError('Geometry ZIP CRC failed')
                expected_rows=0
    else:members=[{'name':'','bytes':path.stat().st_size}]
    obj.update(object_id=key(obj['source_id'],obj['url'],obj['sha256']),expected_members=len(members) or 1,members=members,expected_rows=expected_rows)
    return obj

def prepare_object(obj,batch_id):
    stage=WORK/'staging'/obj['object_id'];receipt=stage/'prepared.json'
    if receipt.exists():
        prior=json.loads(receipt.read_text(encoding='utf-8'))
        if prior['object_sha256']!=obj['sha256']:raise ValueError('Staging identity mismatch')
        for part in prior['shards']:
            p=stage/part['name']
            if not p.exists() or p.stat().st_size!=part['bytes'] or file_hash(p)!=part['sha256']:raise ValueError('Staging checksum mismatch')
        print('PREPARE_REUSED '+obj['source_id']+' rows='+str(prior['rows']),flush=True)
        return {**obj,'prepared':prior,'staging_path':str(stage.relative_to(ROOT)).replace('\\','/')}
    if stage.exists():
        stage.rename(stage.with_name(stage.name+'.incomplete-'+uuid.uuid4().hex[:8]))
    stage.mkdir(parents=True)
    if obj['kind']=='geometry_zip':
        summary={'object_sha256':obj['sha256'],'rows':0,'member_counts':{},'shards':[],'crc_verified':True,'batch_id':batch_id}
    else:
        schema=pa.schema([pa.field('object_id',pa.string(),False),pa.field('member_path',pa.string(),False),pa.field('record_ordinal',pa.int64(),False),pa.field('payload_json',pa.string(),False)])
        counts={};shards=[];total=0;last=time.monotonic();writer=None;shard_rows=0;shard_index=0;out=None
        required=json.loads((ROOT/'config/model/source-field-mapping.json').read_text(encoding='utf-8')).get(obj['source_id'])
        try:
            for member,ordinal,rows in iter_source(obj):
                if required and not set(required).issubset(rows[0]):raise ValueError('Required source fields missing')
                if writer is None:
                    out=stage/f'part-{shard_index:05d}.parquet';writer=pq.ParquetWriter(out,schema,compression='zstd');shard_rows=0
                payloads=[canonical(row) for row in rows]
                arrow=pa.Table.from_arrays([pa.array([obj['object_id']]*len(rows)),pa.array([member]*len(rows)),pa.array(range(ordinal,ordinal+len(rows)),type=pa.int64()),pa.array(payloads)],schema=schema)
                writer.write_table(arrow,row_group_size=32768);shard_rows+=len(rows);total+=len(rows);counts[member]=counts.get(member,0)+len(rows)
                if shard_rows>=262144:
                    writer.close();writer=None;shards.append({'name':out.name,'rows':shard_rows,'bytes':out.stat().st_size,'sha256':file_hash(out)});shard_index+=1
                if time.monotonic()-last>15:
                    print(f"PREPARE_PROGRESS {obj['source_id']} rows={total}",flush=True);last=time.monotonic()
            if writer:
                writer.close();writer=None;shards.append({'name':out.name,'rows':shard_rows,'bytes':out.stat().st_size,'sha256':file_hash(out)})
        finally:
            if writer:writer.close()
        if obj['expected_rows'] is not None and total!=obj['expected_rows']:raise ValueError('Parquet source count mismatch')
        if obj['kind']=='csv_zip' and set(counts)!={x['name'] for x in obj['members']}:raise ValueError('Some CSV members were not processed')
        # ZIP member streams must reach EOF: zipfile validates each member CRC during iteration.
        summary={'object_sha256':obj['sha256'],'rows':total,'member_counts':counts,'shards':shards,'crc_verified':obj['kind']=='csv_zip','batch_id':batch_id}
    write_json(receipt,summary)
    print(f"PREPARE_OK {obj['source_id']} rows={summary['rows']} members={obj['expected_members']}",flush=True)
    return {**obj,'prepared':summary,'staging_path':str(stage.relative_to(ROOT)).replace('\\','/')}

def ensure_source_discovery(source_specs, proxy=None, force=False):
    """Discover new main-source links before acquisition; cached files stay offline."""
    requested = [spec['source_id'] for spec in source_specs
                 if spec['source_id'] in ('tlc_yellow', 'citibike_trips')
                 and (force or not (ROOT / spec['relative_path']).exists())]
    if not requested:
        return {'mode': 'existing-files-reuse-check', 'requested_sources': [],
                'note': 'Existing files still require the download sidecar SHA/size/URL checks.'}
    from discover_sources import discover
    months = {spec['source_period'] for spec in source_specs}
    if len(months) != 1:
        raise ValueError('A discovery run requires exactly one source month')
    result = discover(months.pop(), requested, proxy=proxy)
    if not result['passed']:
        raise RuntimeError('Official source discovery failed; no silent catalog fallback. Inspect ' + result['receipt_file'])
    return {'mode': 'live-official-discovery', 'requested_sources': requested,
            'receipt_file': result['receipt_file'], 'passed': True}

def execute(args):
    from compute_lock import exclusive_compute
    WORK.mkdir(parents=True,exist_ok=True)
    batch_id=uuid.uuid4().hex;start=now();run_dir=WORK/'runs'/batch_id;run_dir.mkdir(parents=True)
    status={'run_id':batch_id,'month':args.month,'started_at_utc':start,'status':'downloading','objects':[]}
    status_path=run_dir/'run.json';write_json(status_path,status)
    try:
        source_specs = specs(args.month)
        status['source_discovery'] = ensure_source_discovery(source_specs, args.proxy, getattr(args, 'crawl_first', False))
        write_json(status_path, status)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            objects=list(pool.map(lambda spec:download(spec,args.proxy),source_specs))
        objects=[inspect_object(x) for x in objects]
        previous = ROOT/'outputs/ods-ingestion-latest.json'
        verification = ROOT/'outputs/ods-verification.json'
        if args.reuse_verified and previous.exists() and verification.exists():
            prior=json.loads(previous.read_text(encoding='utf-8'))
            verified=json.loads(verification.read_text(encoding='utf-8'))
            if prior['month']==args.month and {o['source_id']:o['sha256'] for o in prior['objects']}=={o['source_id']:o['sha256'] for o in objects}:
                from ods_snapshot import verified_month_version
                pinned=verified_month_version(ROOT,prior,verified)
                receipt={'passed':True,'run_id':batch_id,'reused_run_id':prior['run_id'],'month':args.month,'verified_ods_version':pinned,'source_sha256_checked':len(objects),'inserted_rows':0,'checked_at_utc':now()}
                status.update(status='verified_reuse',finished_at_utc=now(),objects=objects,reuse=receipt)
                write_json(status_path,status);write_json(ROOT/'outputs/ods-rerun-verification.json',receipt)
                print('ODS_VERIFIED_REUSED '+json.dumps(receipt),flush=True)
                return status
        status.update(status='preparing',objects=objects);write_json(status_path,status)
        # Bounded preparation avoids multiple large Arrow buffers on this 16GB laptop.
        prepared=[prepare_object(obj,batch_id) for obj in objects]
        status.update(status='ready_for_ods',objects=prepared);write_json(status_path,status)
        shutil.copyfile(ROOT/'config/model/schema-contract.json',WORK/'schema-contract.json')
        shutil.copyfile(ROOT/'sql/spark/001_create_lakehouse.sql',WORK/'lake.sql')
        if args.prepare_only:return status
        os.environ['DOCKER_CONFIG']=r'D:\CodexData\app-data\docker-profile\cli'
        command=[DOCKER,'compose','-f',str(ROOT/'compose.yaml'),'exec','-T','spark','/opt/spark/bin/spark-submit','/workspace/jobs/ingest_ods.py','--manifest','/workspace/'+str(status_path.relative_to(ROOT)).replace('\\','/')]
        status['status']='writing_ods';write_json(status_path,status)
        log=run_dir/'spark.log'
        with log.open('w',encoding='utf-8') as output:
            result=exclusive_compute(subprocess.run)(command,stdout=output,stderr=subprocess.STDOUT)
        if result.returncode:raise RuntimeError('ODS Spark job failed; inspect '+str(log))
        report=json.loads((run_dir/'ods-result.json').read_text(encoding='utf-8'))
        status.update(status='succeeded',finished_at_utc=now(),ods_result=report);write_json(status_path,status)
        write_json(ROOT/'outputs/ods-ingestion-latest.json',status)
        print('ODS_RUN_OK '+json.dumps({'run_id':batch_id,'objects':len(prepared),'rows':report['total_ods_rows_for_scope'],'inserted_rows':report['inserted_rows'],'skipped_rows':report['duplicate_skipped_rows']},ensure_ascii=False),flush=True)
        return status
    except Exception as exc:
        status.update(status='failed',finished_at_utc=now(),error=str(exc));write_json(status_path,status);raise

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--month',default='2025-06',choices=['2025-06','2025-07','2025-08'])
    parser.add_argument('--proxy',default=os.environ.get('CITY_MOBILITY_DOWNLOAD_PROXY'))
    parser.add_argument('--prepare-only',action='store_true')
    parser.add_argument('--crawl-first',action='store_true',help='Force live official link discovery even when complete local source files exist')
    parser.add_argument('--discover-only',action='store_true',help='Only crawl/parse official links; do not download business files or write ODS')
    parser.add_argument('--reuse-verified',action='store_true',help='Reuse only after source SHA and the verified monthly Delta files match')
    args=parser.parse_args()
    if args.discover_only:
        from discover_sources import discover
        report = discover(args.month, ('tlc_yellow', 'citibike_trips'), proxy=args.proxy)
        print(json.dumps({'passed': report['passed'], 'receipt_file': report['receipt_file'], 'results': report['results']}, ensure_ascii=False, indent=2))
        if not report['passed']:
            raise SystemExit(1)
        return
    WORK.mkdir(parents=True,exist_ok=True)
    lock=WORK/'ingestion.lock'
    try:fd=os.open(lock,os.O_CREAT|os.O_EXCL|os.O_WRONLY)
    except FileExistsError:raise RuntimeError('Another ingestion is running or left a lock. Inspect work/ingestion/ingestion.lock before retrying.')
    try:
        os.write(fd,json.dumps({'pid':os.getpid(),'started_at_utc':now()}).encode());os.close(fd)
        execute(args)
    finally:lock.unlink(missing_ok=True)

if __name__=='__main__':main()
