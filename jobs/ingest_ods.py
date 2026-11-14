"""Publish prepared official source rows into ODS Delta, with idempotency and logs."""
import argparse
import datetime as dt
import hashlib
import json
import re
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F, types as T

BASE=Path('/workspace')
WORK=BASE/'work/ingestion'
def now():return dt.datetime.now(dt.timezone.utc)
def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False,default=str)
def key(*values):return hashlib.sha256(canonical(list(values)).encode()).hexdigest()
def write_json(path,value):
    temp=path.with_suffix('.tmp');temp.write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str));temp.replace(path)
def instant(value):return dt.datetime.fromisoformat(value)

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--manifest',required=True);args=parser.parse_args()
    manifest_path=Path(args.manifest).resolve()
    if not manifest_path.is_relative_to(WORK/'runs'):raise ValueError('Manifest must be inside project ingestion runs')
    manifest=json.loads(manifest_path.read_text())
    spark=SparkSession.builder.appName('CityMobility-ODS-Ingestion').config('spark.sql.session.timeZone','UTC').config('spark.sql.warehouse.dir',str(WORK/'warehouse')).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    contract=json.loads((WORK/'schema-contract.json').read_text())
    tables={t['name']:t for t in contract['tables']}
    sql=(WORK/'lake.sql').read_text()
    selected=['ops.ingest_batch','ops.source_object','ops.quality_result','ods.source_record']
    for statement in re.sub(r'^--.*$','',sql,flags=re.M).split(';'):
        text=statement.strip()
        if text and (text.startswith('SET ') or re.match(r'CREATE DATABASE IF NOT EXISTS (ops|ods)\b',text) or any(text.startswith('CREATE TABLE IF NOT EXISTS '+name+' ') for name in selected)):
            spark.sql(text)
    schema=lambda name:T.StructType([T.StructField(x['name'],T._parse_datatype_string(x['type']),x['nullable']) for x in tables[name]['columns']])
    def upsert(name,rows,keys):
        df=spark.createDataFrame(rows,schema(name))
        condition=' AND '.join(f't.`{x}`=s.`{x}`' for x in keys)
        DeltaTable.forPath(spark,tables[name]['location']).alias('t').merge(df.alias('s'),condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    def records():return spark.read.format('delta').load(tables['ods.source_record']['location'])
    report={'run_id':manifest['run_id'],'month':manifest['month'],'sources':[],'inserted_rows':0,'duplicate_skipped_rows':0,'total_ods_rows_for_scope':0,'started_at_utc':now().isoformat()}
    try:
        for obj in manifest['objects']:
            sid,period=obj['source_id'],obj['source_period']
            expected=obj['prepared']['rows'];batch_id=key(manifest['run_id'],sid,period)
            earlier=spark.read.format('delta').load(tables['ops.ingest_batch']['location']).where((F.col('source_id')==sid)&(F.col('source_period')==period)).count()
            batch=dict(batch_id=batch_id,logical_batch_key=key('ods',sid,period),source_id=sid,source_period=period,attempt_number=earlier+1,status='running',
                       started_at_utc=now(),finished_at_utc=None,input_rows=expected,accepted_rows=0,quarantined_rows=0,duplicate_skipped_rows=0,out_of_scope_rows=0,parse_error_rows=0,rule_version='1.0',error_summary=None)
            upsert('ops.ingest_batch',[batch],['batch_id'])
            print(f'ODS_SOURCE_START {sid} expected_rows={expected}',flush=True)
            try:
                prior=spark.read.format('delta').load(tables['ops.source_object']['location']).where(F.col('object_id')==obj['object_id']).collect()
                old=records().where((F.col('source_id')==sid)&(F.col('source_period')==period))
                existing_count=old.count()
                old_ids={r['object_id'] for r in old.select('object_id').distinct().collect()}
                reused=bool(prior and prior[0]['is_complete'] and existing_count==expected and (old_ids=={obj['object_id']} if expected else not old_ids))
                before_version=DeltaTable.forPath(spark,tables['ods.source_record']['location']).history(1).select('version').first()['version']
                if not reused and expected:
                    paths=[str(BASE/obj['staging_path']/p['name']) for p in obj['prepared']['shards']]
                    source=spark.read.parquet(*paths)
                    summary=source.groupBy('member_path').agg(F.count('*').alias('rows'),F.min('record_ordinal').alias('first'),F.max('record_ordinal').alias('last'),F.countDistinct('record_ordinal').alias('unique_ordinals')).collect()
                    bymember={x['member_path']:x.asDict() for x in summary}
                    if set(bymember)!=set(obj['prepared']['member_counts']):raise ValueError('Member coverage mismatch')
                    for member,n in obj['prepared']['member_counts'].items():
                        actual=bymember[member]
                        if (actual['rows'],actual['first'],actual['last'],actual['unique_ordinals'])!=(n,1,n,n):raise ValueError('Ordinal/count uniqueness mismatch: '+member)
                    if source.where(F.col('object_id')!=obj['object_id']).limit(1).count():raise ValueError('Foreign object in staging')
                    array_prefix=F.regexp_replace(F.to_json(F.array(F.col('object_id'),F.col('member_path'))),r'\]$','')
                    identity_json=F.concat(array_prefix,F.lit(','),F.col('record_ordinal').cast('string'),F.lit(']'))
                    df=source.withColumn('record_key',F.sha2(identity_json,256)).withColumn('payload_sha256',F.sha2('payload_json',256))
                    df=(df.withColumn('source_id',F.lit(sid)).withColumn('source_period',F.lit(period)).withColumn('poll_id',F.lit(obj['poll_id']).cast('string'))
                        .withColumn('received_at_utc',F.lit(instant(obj['received_at_utc']))).withColumn('batch_id',F.lit(batch_id)))
                    df=df.select(*[x['name'] for x in tables['ods.source_record']['columns']])
                    sample=df.select('record_key','object_id','member_path','record_ordinal').first()
                    if sample['record_key']!=key(sample['object_id'],sample['member_path'],sample['record_ordinal']):raise ValueError('Cross-language key encoding differs')
                    predicate=f"source_id='{sid}' AND source_period='{period}'"
                    # Entire source/month is validated first. Atomic replace keeps revised source
                    # objects from coexisting and double-counting. Other sources/periods remain intact.
                    (df.repartition(4).write.format('delta').mode('overwrite').option('replaceWhere',predicate)
                       .partitionBy('source_id','source_period').save(tables['ods.source_record']['location']))
                final=records().where((F.col('source_id')==sid)&(F.col('source_period')==period))
                final_count=final.count()
                if final_count!=expected:raise ValueError(f'ODS count mismatch: {final_count} != {expected}')
                after_version=DeltaTable.forPath(spark,tables['ods.source_record']['location']).history(1).select('version').first()['version']
                if reused and before_version!=after_version:raise ValueError('Idempotent retry modified ODS')
                source_object=dict(object_id=obj['object_id'],source_id=sid,source_period=period,source_url=obj['url'],relative_path=obj['relative_path'],object_sha256=obj['sha256'],object_bytes=obj['bytes'],
                                   poll_id=obj['poll_id'],received_at_utc=instant(obj['received_at_utc']),first_batch_id=prior[0]['first_batch_id'] if prior else batch_id,
                                   expected_members=obj['expected_members'],processed_members=obj['expected_members'],is_complete=True)
                upsert('ops.source_object',[source_object],['object_id'])
                batch.update(status='validated',finished_at_utc=now(),accepted_rows=0 if reused else expected,duplicate_skipped_rows=expected if reused else 0)
                upsert('ops.ingest_batch',[batch],['batch_id'])
                quality=[]
                for rule in ['Q001','Q002','Q013']:
                    quality.append(dict(result_id=key(batch_id,rule),batch_id=batch_id,table_name='ods.source_record',partition_spec=canonical({'source_id':sid,'source_period':period}),
                           rule_id=rule,severity='block',checked_rows=expected,failed_rows=0,passed=True,details_json=canonical({'source_members':obj['prepared']['member_counts'],'raw_asset_only':obj['kind']=='geometry_zip','object_sha256':obj['sha256']}),checked_at_utc=now()))
                upsert('ops.quality_result',quality,['result_id'])
                result=dict(source_id=sid,source_period=period,object_id=obj['object_id'],raw_sha256=obj['sha256'],raw_bytes=obj['bytes'],rows=final_count,expected_members=obj['expected_members'],processed_members=obj['expected_members'],
                            member_counts=obj['prepared']['member_counts'],action='reused' if reused else 'written',ods_version_before=before_version,ods_version_after=after_version,batch_id=batch_id)
                report['sources'].append(result);report['inserted_rows']+=batch['accepted_rows'];report['duplicate_skipped_rows']+=batch['duplicate_skipped_rows'];report['total_ods_rows_for_scope']+=final_count
                write_json(manifest_path.parent/'ods-result.json',report)
                print(f'ODS_SOURCE_OK {sid} rows={final_count} action={result["action"]}',flush=True)
            except Exception as exc:
                batch.update(status='failed',finished_at_utc=now(),error_summary=str(exc)[:2000]);upsert('ops.ingest_batch',[batch],['batch_id']);raise
        report.update(passed=True,finished_at_utc=now().isoformat(),ods_version=DeltaTable.forPath(spark,tables['ods.source_record']['location']).history(1).select('version').first()['version'])
        write_json(manifest_path.parent/'ods-result.json',report)
        print('ODS_ALL_SOURCES_OK '+json.dumps({'rows':report['total_ods_rows_for_scope'],'inserted':report['inserted_rows'],'skipped':report['duplicate_skipped_rows']}),flush=True)
    finally:spark.stop()

if __name__=='__main__':main()
