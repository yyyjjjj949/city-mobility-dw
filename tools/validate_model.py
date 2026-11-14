"""Verify model contracts and actual engine DDL, using isolated test namespaces."""
import csv
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo
from compute_lock import exclusive_compute

ROOT=Path(__file__).resolve().parents[1]
WORK=ROOT/'work/model-validation'
DOCKER=r'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'
os.environ['DOCKER_CONFIG']=r'D:\CodexData\app-data\docker-profile\cli'
RUN=uuid.uuid4().hex[:10]
BASE=[DOCKER,'compose','-f',str(ROOT/'compose.yaml')]

def run(arguments,text=None,check=True):
    p=subprocess.run(arguments,input=text,text=True,encoding='utf-8',errors='replace',stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    if check and p.returncode:
        raise RuntimeError(f'Command failed ({p.returncode}): '+p.stderr[-7000:]+'\n'+p.stdout[-2000:])
    return p

def ch(sql):
    return run(BASE+['exec','-T','clickhouse','/bin/sh','-c','exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery'],sql).stdout

def classify_local(text):
    naive=dt.datetime.fromisoformat(text)
    zone=ZoneInfo('America/New_York')
    choices={naive.replace(tzinfo=zone,fold=f).astimezone(dt.timezone.utc) for f in [0,1]
             if naive.replace(tzinfo=zone,fold=f).astimezone(dt.timezone.utc).astimezone(zone).replace(tzinfo=None)==naive}
    return 'nonexistent' if not choices else 'ambiguous' if len(choices)>1 else 'unique'

@exclusive_compute
def main():
    WORK.mkdir(parents=True,exist_ok=True)
    contract= json.loads((ROOT/'config/model/schema-contract.json').read_text(encoding='utf-8'))
    byname={t['name']:t for t in contract['tables']}
    for t in byname.values():
        fields={x['name']:x for x in t['columns']}
        assert all(not fields[k]['nullable'] for k in t['logical_key']),t['name']
        for x in t['columns']:
            if x['reference']:
                name,col=x['reference'].rsplit('.',1)
                target=next(c for c in byname[name]['columns'] if c['name']==col)
                assert target['type']==x['type'],(t['name'],x)
    with (ROOT/'docs/数据字典.csv').open(encoding='utf-8-sig') as f:dictionary=list(csv.DictReader(f))
    assert len(dictionary)==sum(len(t['columns']) for t in byname.values())
    assert classify_local('2025-06-01 00:02:50')=='unique'
    assert classify_local('2025-11-02 01:30:00')=='ambiguous'
    assert classify_local('2025-03-09 02:30:00')=='nonexistent'
    samples=ROOT/'data/samples'
    taxi=json.loads((samples/'taxi_2025-06_first25.json').read_text())
    with (samples/'bike_2025-06_first25.csv').open(encoding='utf-8-sig') as f:bike=list(csv.DictReader(f))
    expected_taxi={x['name'] for x in json.loads((ROOT/'docs/data-source-catalog.json').read_text(encoding='utf-8'))['sources'][0]['objects'][0]['schema']}
    assert set(taxi[0])==expected_taxi
    for x in taxi: assert classify_local(x['tpep_pickup_datetime'])=='unique'
    for x in bike:
        assert classify_local(x['started_at'])=='unique'
        assert isinstance(x['start_station_id'],str)
    shutil.copyfile(ROOT/'config/model/schema-contract.json',WORK/'schema-contract.json')
    shutil.copyfile(ROOT/'sql/spark/001_create_lakehouse.sql',WORK/'lake.sql')
    p=run(BASE+['exec','-T','spark','/opt/spark/bin/spark-submit','/workspace/jobs/check_model_schema.py','--run-id',RUN],check=False)
    (WORK/'spark-validation.log').write_text(p.stdout+'\n'+p.stderr,encoding='utf-8')
    if p.returncode:raise RuntimeError('Spark schema validation failed; see work/model-validation/spark-validation.log')
    print('Spark schema validation passed.',flush=True)
    database='modelcheck_'+RUN
    sql=(ROOT/'sql/clickhouse/001_create_serving.sql').read_text(encoding='utf-8')
    sql=re.sub(r'\bads(?=\.)',database,sql)
    sql=sql.replace('CREATE DATABASE IF NOT EXISTS ads ENGINE',f'CREATE DATABASE IF NOT EXISTS {database} ENGINE')
    try:
        ch(sql);ch(sql)
        for t in byname.values():
            if not t['serving'] and t['storage']!='clickhouse':continue
            actual=json.loads(ch(f'DESCRIBE TABLE {database}.{t["name"].split(".")[1]} FORMAT JSON'))['data']
            assert [x['name'] for x in actual]==[x['name'] for x in t['columns']],t['name']
            assert ch(f'SELECT count() FROM {database}.{t["name"].split(".")[1]}').strip()=='0'
        views=[t for t in byname.values() if t['serving']]
        for t in views:ch(f'SELECT * FROM {database}.bi_{t["name"].split(".")[1]} LIMIT 1')
        clickhouse_result={'tables':len(views)+1,'views':len(views),'ddl_idempotent':True,'all_serving_tables_empty':True,'check_database':database}
    finally:
        ch(f'DROP DATABASE IF EXISTS {database} SYNC')
    spark=json.loads((WORK/'spark-schema-check.json').read_text())
    report={'checked_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'passed':True,'model_version':contract['version'],
            'contract_sha256':hashlib.sha256((ROOT/'config/model/schema-contract.json').read_bytes()).hexdigest(),
            'tables':len(byname),'dimensions':7,'core_facts':2,'dictionary_columns':len(dictionary),
            'logical_reference_types_checked':True,'dst_regression_cases':['unique','ambiguous','nonexistent'],
            'source_sample_schema_checked':True,'spark':spark,'clickhouse':clickhouse_result,
            'business_tables_initialized':False,'full_month_business_data_loaded':False}
    (ROOT/'outputs/model-validation.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'passed':True,'tables':len(byname),'dictionary_columns':len(dictionary),'spark_tables':len(spark['tables']),'clickhouse':clickhouse_result},ensure_ascii=False),flush=True)

if __name__=='__main__':main()
