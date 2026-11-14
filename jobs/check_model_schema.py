"""Validate DDL using isolated empty Delta locations on D:, not production tables."""
import argparse
import json
import re
from pathlib import Path
from pyspark.sql import SparkSession

parser=argparse.ArgumentParser()
parser.add_argument('--run-id',required=True)
args=parser.parse_args()
base=Path('/workspace/work/model-validation')
contract=json.loads((base/'schema-contract.json').read_text())
sql=(base/'lake.sql').read_text()
prefix='mc_'+args.run_id+'_'
sql=re.sub(r'\b(ops|ods|dwd|dws|ads)(?=\.)',lambda m:prefix+m.group(1),sql)
sql=re.sub(r'(CREATE DATABASE IF NOT EXISTS )(ops|ods|dwd|dws|ads)\b',lambda m:m.group(1)+prefix+m.group(2),sql)
sql=sql.replace('/workspace/data/',f'/workspace/work/model-validation/{args.run_id}/lake/')
spark=SparkSession.builder.appName('CityMobility-ModelSchemaCheck').config('spark.sql.session.timeZone','UTC').config('spark.sql.warehouse.dir',str(base/args.run_id/'warehouse')).getOrCreate()
results=[]
try:
    for statement in re.sub(r'^--.*$','',sql,flags=re.M).split(';'):
        if statement.strip():spark.sql(statement)
    for t in contract['tables']:
        if t['storage']!='delta':continue
        name=prefix+t['name']
        actual=spark.table(name).schema
        expected=spark.sql('SELECT '+', '.join('CAST(NULL AS '+c['type']+') AS `'+c['name']+'`' for c in t['columns'])).schema
        assert [(x.name,x.dataType) for x in actual]==[(x.name,x.dataType) for x in expected],name
        assert [x.nullable for x in actual]==[x['nullable'] for x in t['columns']],name
        detail=spark.sql('DESCRIBE DETAIL '+name).first().asDict()
        assert detail['format']=='delta',name
        assert list(detail['partitionColumns'])==t['partition_by'],name
        assert spark.table(name).count()==0,name
        results.append({'table':t['name'],'columns':len(actual),'partition':detail['partitionColumns'],'empty':True})
    # Idempotent re-registration must not create extra tables or fail.
    for statement in re.sub(r'^--.*$','',sql,flags=re.M).split(';'):
        if statement.strip():spark.sql(statement)
    report={'passed':True,'spark_version':spark.version,'timezone':spark.conf.get('spark.sql.session.timeZone'),'isolated_run_id':args.run_id,'tables':results,'idempotent_ddl':True,'business_tables_initialized':False}
    (base/'spark-schema-check.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print('MODEL_SCHEMA_OK '+json.dumps({'tables':len(results),'idempotent':True}))
finally:
    spark.stop()
