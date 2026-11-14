"""Record only a publication already validated and activated in ClickHouse."""
import argparse
import datetime as dt
import json
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, types as T, functions as F

p=argparse.ArgumentParser();p.add_argument('--release-id',required=True);args=p.parse_args()
assert len(args.release_id)==32 and all(c in '0123456789abcdef' for c in args.release_id)
run=Path('/workspace/work/marts/runs')/args.release_id
m=json.loads((run/'manifest.json').read_text());r=json.loads((run/'result.json').read_text());receipt=json.loads((run/'publication.json').read_text())
assert r['passed'] and receipt['passed'] and receipt['status']=='active'
spark=(SparkSession.builder.appName('CityMobility-RecordPublication').config('spark.sql.session.timeZone','UTC')
       .config('spark.databricks.delta.snapshotPartitions','2').config('spark.sql.warehouse.dir','/workspace/work/marts/publication-warehouse').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
try:
    t=next(t for t in json.loads(Path('/workspace/work/marts/schema-contract.json').read_text())['tables'] if t['name']=='ops.publication')
    schema=T.StructType([T.StructField(c['name'],T._parse_datatype_string(c['type']),c['nullable']) for c in t['columns']])
    rows=[]
    for dataset in ['historical','quality']:
        rows.append(dict(release_id=args.release_id,dataset_id=dataset,scope_json=json.dumps({'month':m['month'],'arrival_complete':False,'mode':'offline'},sort_keys=True),status='active',
                         input_versions_json=json.dumps({'dwd_run_id':m['dwd']['run_id'],'delta_versions':m['versions'],'code_sha256':m['code_sha256']},sort_keys=True),
                         output_counts_json=json.dumps({k:v for k,v in r['counts'].items() if k.startswith('ads.') and (('quality_' in k)==(dataset=='quality'))},sort_keys=True),
                         rule_version=m['policies']['version'],published_at_utc=dt.datetime.fromisoformat(receipt['activated_at_utc']).replace(tzinfo=dt.timezone.utc)))
    table=DeltaTable.forPath(spark,t['location'])
    table.alias('t').merge(spark.createDataFrame(rows,schema).alias('s'),'t.release_id=s.release_id AND t.dataset_id=s.dataset_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    for old in receipt['previous_pointers']:
        if old['dataset_id'] in ['historical','quality'] and old['release_id']!=args.release_id:
            table.update((F.col('release_id')==old['release_id'])&(F.col('dataset_id')==old['dataset_id']),{'status':F.lit('retired')})
    assert table.toDF().where((F.col('release_id')==args.release_id)&(F.col('status')=='active')).count()==2
    report={'passed':True,'release_id':args.release_id,'datasets':['historical','quality'],'ops_publication_version':int(table.history(1).first()['version'])}
    (run/'publication-ops.json').write_text(json.dumps(report,indent=2));print('PUBLICATION_OPS_OK '+json.dumps(report),flush=True)
finally:spark.stop()
