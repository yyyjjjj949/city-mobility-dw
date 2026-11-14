"""Install, start and verify the self contained Windows team distribution."""
import os
import argparse
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import webbrowser
import portable_config as cfg

ROOT=cfg.ROOT

def save(path,data):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8');tmp.replace(path)

def docker(*args,**kwargs):
    return subprocess.run([cfg.DOCKER,*args],cwd=ROOT,check=True,**kwargs)

def compose(*args,**kwargs):return docker('compose','-f',str(ROOT/'compose.yaml'),*args,**kwargs)

def preflight():
    try:
        info=docker('info','--format','{{.OSType}}/{{.Architecture}}',capture_output=True,text=True).stdout.strip()
    except (OSError,subprocess.CalledProcessError):
        raise RuntimeError('Docker Desktop is not ready. Install/open Docker Desktop, select Linux containers, wait for Engine running, then retry.')
    assert info in ('linux/x86_64','linux/amd64'),'This package requires Docker Linux containers on a Windows x64 computer: '+info
    assert len(str(ROOT))<160,'Extract to a shorter path such as D:\\CityMobility'
    print('DOCKER_READY '+info,flush=True)

def verify_files():
    manifest=ROOT/'package-manifest.json'
    if not manifest.exists():raise RuntimeError('package-manifest.json is missing')
    data=json.loads(manifest.read_text(encoding='utf-8'))
    for index,row in enumerate(data['files'],1):
        path=(ROOT/row['path']).resolve()
        assert path.is_relative_to(ROOT.resolve()),'Invalid manifest path'
        assert path.is_file() and path.stat().st_size==row['bytes'],'Missing or damaged file: '+row['path']
        digest=hashlib.file_digest(path.open('rb'),'sha256').hexdigest()
        assert digest==row['sha256'],'Checksum mismatch: '+row['path']
        if index%500==0:print('FILES_CHECKED '+str(index),flush=True)
    print('PACKAGE_CHECKSUMS_OK '+str(len(data['files'])),flush=True)

def worker(stop=False):
    work=ROOT/'work/orchestration';work.mkdir(parents=True,exist_ok=True)
    state=work/'worker-state.json'
    if stop:
        (work/'stop.request').write_text('stop',encoding='utf-8')
        deadline=time.monotonic()+3000
        while state.exists() and json.loads(state.read_text(encoding='utf-8')).get('running'):
            if time.monotonic()>deadline:raise RuntimeError('Worker is still active; services have been left running.')
            time.sleep(2)
        return
    if state.exists():
        data=json.loads(state.read_text(encoding='utf-8'))
        if data.get('running') and (dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(data['checked_at_utc'])).total_seconds()<15:return
    (work/'stop.request').unlink(missing_ok=True)
    with (work/'worker-output.log').open('a',encoding='utf-8') as out,(work/'worker-error.log').open('a',encoding='utf-8') as err:
        proc=subprocess.Popen([sys.executable,str(ROOT/'tools/run_airflow_worker.py')],cwd=ROOT,stdin=subprocess.DEVNULL,stdout=out,stderr=err,
            creationflags=subprocess.CREATE_NO_WINDOW|subprocess.CREATE_NEW_PROCESS_GROUP)
    for _ in range(60):
        if state.exists():
            data=json.loads(state.read_text(encoding='utf-8'))
            if data.get('running') and data.get('pid')==proc.pid:
                print('PROJECT_WORKER_READY',flush=True);return
        if proc.poll() is not None:break
        time.sleep(.5)
    raise RuntimeError('Worker did not start; inspect work/orchestration/worker-error.log')

def wait_health():
    import requests
    session=requests.Session();session.trust_env=False
    for _ in range(90):
        try:
            health=session.get(cfg.AIRFLOW+'/api/v2/monitor/health',timeout=5).json()
            if all(health[k]['status']=='healthy' for k in ['metadatabase','scheduler','dag_processor']) and session.get(cfg.SUPERSET+'/health',timeout=5).ok:return
        except (requests.RequestException,ValueError,KeyError):pass
        time.sleep(2)
    raise RuntimeError('Application health did not become ready')

def check():
    import requests
    from publish_marts import ClickHouse
    ch=ClickHouse();backup=json.loads((ROOT/'backups/snapshot.json').read_text(encoding='utf-8'))
    pointers=ch.query('SELECT dataset_id,release_id FROM ads.release_pointer ORDER BY dataset_id')
    assert pointers==backup['pointers'],'Serving release differs from backup'
    assert int(ch.query('SELECT sum(trip_count) AS n FROM ads.bi_trip_day')[0]['n'])==9012795
    for name,receipt in backup['tables'].items():
        if name!='release_pointer':assert int(ch.query('SELECT count() AS n FROM ads.bi_'+name)[0]['n'])==receipt['rows'],name
    s=requests.Session();s.trust_env=False
    login=s.post(cfg.SUPERSET+'/api/v1/security/login',json={'username':'admin','password':os.environ['SUPERSET_ADMIN_PASSWORD'],'provider':'db','refresh':True},timeout=30)
    login.raise_for_status();s.headers['Authorization']='Bearer '+login.json()['access_token']
    metadata={}
    for name,count in [('dashboard',4),('chart',19),('dataset',12)]:
        r=s.get(cfg.SUPERSET+'/api/v1/'+name+'/',params={'q':'(page:0,page_size:100)'},timeout=30);r.raise_for_status()
        assert r.json()['count']==count,name;metadata[name]=sorted(x['id'] for x in r.json()['result'])
    auth=s.post(cfg.AIRFLOW+'/auth/token',json={'username':'admin','password':os.environ['AIRFLOW_ADMIN_PASSWORD']},timeout=30);auth.raise_for_status()
    m=s.get(cfg.SUPERSET+'/city-mobility/map/',timeout=30);assert m.ok and 'map.js' in m.text
    state=json.loads((ROOT/'work/orchestration/worker-state.json').read_text(encoding='utf-8'))
    assert state['running'] and (dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(state['checked_at_utc'])).total_seconds()<15
    report={'passed':True,'checked_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'project':cfg.PROJECT,'root':str(ROOT),
            'valid_trips':9012795,'pointers':pointers,'metadata_ids':metadata,'superset_admin_login':True,'airflow_admin_login':True,'map_route':True}
    save(ROOT/'outputs/member-verification.json',report)
    print('MEMBER_INSTALLATION_OK trips=9012795 dashboards=4 charts=19',flush=True)
    return report

def install():
    preflight();marker=ROOT/'outputs/member-installation.json'
    if marker.exists() and json.loads(marker.read_text(encoding='utf-8')).get('project')==cfg.PROJECT:
        start();return
    verify_files()
    for volume in ['postgres-data','clickhouse-data']:
        exists=subprocess.run([cfg.DOCKER,'volume','inspect',cfg.PROJECT+'_'+volume],cwd=ROOT,capture_output=True).returncode==0
        if exists:raise RuntimeError('Target project already has a database volume. Refusing to overwrite it: '+cfg.PROJECT+'_'+volume)
    print('IMPORTING_DOCKER_IMAGES',flush=True)
    docker('image','load','-i',str(ROOT/'images/services.tar.gz'))
    for row in json.loads((ROOT/'images/images.json').read_text(encoding='utf-8')):
        actual=docker('image','inspect',row['image'],'--format','{{.Id}}',text=True,capture_output=True).stdout.strip()
        assert actual==row['id'],'Image identity mismatch: '+row['image']
    compose('up','-d','--wait','--wait-timeout','300','postgres','clickhouse','spark')
    for name in ['superset','airflow']:
        print('RESTORING_POSTGRES '+name,flush=True)
        with (ROOT/'backups'/(name+'.dump')).open('rb') as stream:
            compose('exec','-T','postgres','pg_restore','-U','platform','--no-owner','--no-acl','--exit-on-error','--dbname',name,stdin=stream)
    from publish_marts import ClickHouse,checksum,columns
    ch=ClickHouse();manifest=json.loads((ROOT/'backups/snapshot.json').read_text(encoding='utf-8'))
    for sql in re.sub(r'^--.*$','',(ROOT/'sql/clickhouse/001_create_serving.sql').read_text(encoding='utf-8'),flags=re.M).split(';'):
        if sql.strip():ch.request(sql)
    for name,expected in manifest['tables'].items():
        assert int(ch.query('SELECT count() AS n FROM ads.'+name)[0]['n'])==0,'Restore target is not empty: '+name
        def chunks():
            with gzip.open(ROOT/'backups/clickhouse'/(name+'.jsonl.gz'),'rb') as stream:
                for block in iter(lambda:stream.read(1024*1024),b''):yield block
        ch.request('INSERT INTO ads.'+name+' FORMAT JSONEachRow',data=chunks())
        assert int(ch.query('SELECT count() AS n FROM ads.'+name)[0]['n'])==expected['rows'],name
        if name!='release_pointer':
            response=ch.request('SELECT * FROM ads.'+name+' FORMAT JSONEachRow',stream=True)
            try:actual=checksum((json.loads(line) for line in response.iter_lines() if line),columns(name))
            finally:response.close()
            assert actual==expected['checksum'],'Restored content checksum differs: '+name
        print('RESTORED_CLICKHOUSE '+name,flush=True)
    # Only mark installed after the entire database restore has succeeded.
    save(marker,{'project':cfg.PROJECT,'restored_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'release_id':manifest['release_id']})
    start()

def start():
    preflight()
    if not (ROOT/'outputs/member-installation.json').exists():raise RuntimeError('Run 01-first-install.cmd first')
    compose('up','-d','--wait','--wait-timeout','600')
    worker();wait_health();check()
    print('SUPERSET '+cfg.SUPERSET+'\nAIRFLOW '+cfg.AIRFLOW,flush=True)

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['install','start','stop','check','status','pipeline','verify-files','open']);args=p.parse_args()
    if args.action=='install':install()
    elif args.action=='start':start()
    elif args.action=='stop':worker(stop=True);compose('stop')
    elif args.action=='check':wait_health();check()
    elif args.action=='status':compose('ps')
    elif args.action=='verify-files':verify_files()
    elif args.action=='open':webbrowser.open(cfg.SUPERSET+'/superset/dashboard/city-mobility-overview/')
    elif args.action=='pipeline':
        from trigger_pipeline import main as trigger
        sys.argv=[sys.argv[0]];trigger()
if __name__=='__main__':
    try:main()
    except Exception as exc:
        print('FAILED: '+str(exc),file=sys.stderr,flush=True);raise SystemExit(1)
