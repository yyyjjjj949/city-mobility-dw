"""Assemble a portable team copy; do not modify the original warehouse."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import io
import tokenize
import zipfile

SOURCE=Path(__file__).resolve().parents[1]
BASE=Path(r'D:\CodexData\workspaces\city-mobility-share-2026-11-14')
TARGET=BASE/'work/CityMobility'
CACHE=Path(r'D:\CodexData\cache\city-mobility-share')
DOCKER=r'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'
IMAGES=['postgres:17-alpine','clickhouse/clickhouse-server:25.8','city-mobility/spark:4.0.5-delta4.0.1','apache/airflow:3.3.2-python3.11','city-mobility/superset:6.1.0-clickhouse']
os.environ['DOCKER_CONFIG']=r'D:\CodexData\app-data\docker-profile\cli'

def copy_dir(relative):
    shutil.copytree(SOURCE/relative,TARGET/relative,dirs_exist_ok=True,ignore=shutil.ignore_patterns('__pycache__','*.pyc','*.lock'))

def images():
    folder=TARGET/'images';folder.mkdir(parents=True,exist_ok=True)
    rows=[]
    for name in IMAGES:
        row=json.loads(subprocess.check_output([DOCKER,'image','inspect',name],text=True,encoding='utf-8'))[0]
        assert row['Architecture']=='amd64' and row['Os']=='linux'
        rows.append({'image':name,'id':row['Id'],'bytes':row['Size']})
    (folder/'images.json').write_text(json.dumps(rows,indent=2),encoding='utf-8')
    temp=folder/'services.tar.gz.part';total=0
    proc=subprocess.Popen([DOCKER,'image','save',*IMAGES],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    with gzip.open(temp,'wb',compresslevel=1) as out:
        while True:
            block=proc.stdout.read(4*1024*1024)
            if not block:break
            out.write(block);total+=len(block)
            if total%(512*1024*1024)<len(block):print('IMAGE_EXPORT_GiB '+str(round(total/1024**3,2)),flush=True)
    error=proc.stderr.read();assert proc.wait()==0,error.decode(errors='replace')[-1500:]
    temp.replace(folder/'services.tar.gz')
    print('IMAGES_EXPORTED '+str((folder/'services.tar.gz').stat().st_size),flush=True)

def runtime():
    out=TARGET/'runtime/python';out.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(CACHE/'python-3.13.9-embed-amd64.zip') as z:z.extractall(out)
    site=out/'Lib/site-packages';site.mkdir(parents=True,exist_ok=True)
    for wheel in (CACHE/'wheels').glob('*.whl'):
        with zipfile.ZipFile(wheel) as z:z.extractall(site)
    (out/'python313._pth').write_text('python313.zip\n.\nLib/site-packages\n../../tools\nimport site\n',encoding='utf-8')
    rows=[]
    for path in [CACHE/'python-3.13.9-embed-amd64.zip',*(CACHE/'wheels').glob('*.whl')]:
        with path.open('rb') as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
        rows.append({'file':path.name,'bytes':path.stat().st_size,'sha256':digest})
    (TARGET/'runtime/provenance.json').write_text(json.dumps({'python_url':'https://www.python.org/ftp/python/3.13.9/python-3.13.9-embed-amd64.zip','wheels_source':'https://pypi.org','files':rows},indent=2),encoding='utf-8')
    print('PYTHON_RUNTIME_READY',flush=True)

def prepare():
    TARGET.mkdir(parents=True,exist_ok=True)
    for name in ['config','dags','docs','jobs','sql','tests','tools','web','build']:
        copy_dir(name)
    shutil.copyfile(SOURCE/'.env',TARGET/'.env')
    copy_dir('data')
    print('LAKE_DATA_COPIED',flush=True)
    # Preserve actual pinned build manifests and exports, not live request queues or caches.
    for name in ['ingestion/runs','ingestion/staging','dwd/python-deps-np126','dwd/official-shapefile']:
        copy_dir('work/'+name)
    dwd=json.loads((SOURCE/'outputs/dwd-latest.json').read_text(encoding='utf-8'))['run_id']
    marts=json.loads((SOURCE/'outputs/marts-latest.json').read_text(encoding='utf-8'))['release_id']
    for name in ['work/dwd/runs/'+dwd,'work/marts/runs/'+marts]:copy_dir(name)
    for file in ['work/dwd/zones.geojson','work/dwd/schema-contract.json','work/dwd/lake.sql','work/dwd/requirements.txt']:
        shutil.copyfile(SOURCE/file,TARGET/file)
    (TARGET/'work/orchestration').mkdir(parents=True,exist_ok=True)
    for name in ['dwd','marts','map']:copy_dir('outputs/'+name)
    for path in (SOURCE/'outputs').iterdir():
        if path.is_file() and (path.suffix in ['.json','.png'] or path.name in ['项目总入口.html','地图筛选与调度验收结果.md','项目实施路线.md','local-credentials.md']):
            shutil.copyfile(path,TARGET/'outputs'/path.name)
    for file in ['portable_config.py','portable.py']:
        shutil.copyfile(SOURCE/'distribution/template'/file,TARGET/'tools'/file)
    # Host orchestration paths are resolved by Docker on PATH, preserving job code fingerprints.
    for path in (TARGET/'tools').glob('*.py'):
        if path.name.startswith('portable'):continue
        text=path.read_text(encoding='utf-8')
        text=re.sub(r"(?m)^DOCKER\s*=\s*r?'[^'\n]*docker.exe'",'DOCKER = _portable.DOCKER',text)
        text=re.sub(r"(?m)^([ \t]*)os\.environ\['DOCKER_CONFIG'\]\s*=\s*r?'[^'\n]*'",r'\1pass  # Use the recipient Docker CLI configuration.',text)
        # Wrap actual Python string literals, preserving HTML/SQL quoting and f-strings.
        tokens=[]
        for token in tokenize.generate_tokens(io.StringIO(text).readline):
            value=token.string
            if token.type==tokenize.STRING and any(url in value for url in ['http://127.0.0.1:18123','http://127.0.0.1:18080','http://127.0.0.1:8088','http://localhost:18080','http://localhost:8088']):
                value='_portable.expand_urls('+value+')'
            tokens.append((token.type,value))
        text=tokenize.untokenize(tokens)
        lines=text.splitlines(True);at=1
        if lines and lines[0].lstrip().startswith('"""'):
            if lines[0].count('"""')>=2:at=1
            else:at=next(i+1 for i,line in enumerate(lines[1:],1) if '"""' in line)
        lines.insert(at,'import portable_config as _portable\n')
        path.write_text(''.join(lines),encoding='utf-8')
    # Team acceptance uses portable entry points and has no retired Kafka container.
    shutil.copyfile(SOURCE/'distribution/template/verify_offline.py',TARGET/'tools/verify_offline.py')
    compose=(SOURCE/'compose.yaml').read_text(encoding='utf-8').replace('name: city-mobility-dw','name: ${MOBILITY_PROJECT_NAME:-city-mobility-team}')
    for number,variable in [('18123','MOBILITY_CLICKHOUSE_PORT'),('18080','MOBILITY_AIRFLOW_PORT'),('8088','MOBILITY_SUPERSET_PORT'),('9000','MOBILITY_NATIVE_PORT'),('4040','MOBILITY_SPARK_PORT')]:
        compose=compose.replace('127.0.0.1:'+number+':','127.0.0.1:${'+variable+':-'+number+'}:')
    # Keep service-internal URLs at container ports; host ports can be changed without source edits.
    (TARGET/'compose.yaml').write_text(compose,encoding='utf-8')
    for name,action in [('01-first-install','install'),('02-start','start'),('03-stop','stop'),('04-check','check'),('05-run-full-pipeline','pipeline'),('06-open-dashboard','open')]:
        (TARGET/(name+'.cmd')).write_text('@echo off\r\ncd /d "%~dp0"\r\n"%~dp0runtime\\python\\python.exe" "%~dp0tools\\portable.py" '+action+'\r\nif errorlevel 1 echo Operation failed. Please read the message above.\r\npause\r\n',encoding='ascii')
    runtime()
    print('PORTABLE_PROJECT_PREPARED '+str(TARGET),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','images']);args=p.parse_args()
    (prepare if args.action=='prepare' else images)()
