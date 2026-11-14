"""Independent committed-Delta audit using actual log actions and Parquet footers."""
from pathlib import Path
import datetime as dt
import hashlib
import json
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[1]
def file_hash(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(4*1024*1024),b''):h.update(chunk)
    return h.hexdigest()

def committed(path):
    active={};versions=[]
    for log in sorted((path/'_delta_log').glob('[0-9]'*20+'.json')):
        versions.append(int(log.stem))
        for line in log.read_text(encoding='utf-8').splitlines():
            action=json.loads(line)
            if 'add' in action:active[action['add']['path']]=action['add']
            if 'remove' in action:active.pop(action['remove']['path'],None)
    if not versions:raise ValueError('No Delta commits: '+str(path))
    return active,max(versions)

def main():
    latest=json.loads((ROOT/'outputs/ods-ingestion-latest.json').read_text(encoding='utf-8'))
    objects={x['object_id']:x for x in latest['objects']}
    for obj in objects.values():
        path=ROOT/obj['relative_path']
        assert path.stat().st_size==obj['bytes'] and file_hash(path)==obj['sha256'],path
    base=ROOT/'data/ods/source_record';active,version=committed(base)
    totals={};footers=0;sample_checks=0
    from urllib.parse import unquote
    for relative,add in active.items():
        path=base/unquote(relative)
        parquet=pq.ParquetFile(path);rows=parquet.metadata.num_rows
        stats=json.loads(add['stats']);assert stats['numRecords']==rows
        assert path.stat().st_size==add['size']
        partition=add['partitionValues'];sid=partition['source_id'];period=partition['source_period']
        if period!=latest['month']:continue
        totals[sid]=totals.get(sid,0)+rows;footers+=1
        sample=next(parquet.iter_batches(batch_size=5,columns=['record_key','object_id','member_path','record_ordinal','payload_json','payload_sha256']))
        for row in sample.to_pylist():
            assert row['object_id'] in objects
            identity=json.dumps([row['object_id'],row['member_path'],row['record_ordinal']],ensure_ascii=False,separators=(',',':'))
            assert hashlib.sha256(identity.encode()).hexdigest()==row['record_key']
            assert hashlib.sha256(row['payload_json'].encode()).hexdigest()==row['payload_sha256']
            payload=json.loads(row['payload_json'])
            assert isinstance(payload,dict) and payload
            sample_checks+=1
    expected={x['source_id']:x['prepared']['rows'] for x in objects.values() if x['prepared']['rows']}
    assert totals==expected,(totals,expected)
    assert sum(totals.values())==9082600
    final=latest['ods_result']
    assert final['passed'] and all(x['processed_members']==x['expected_members'] for x in final['sources'])
    idempotent=final['inserted_rows']==0 and final['duplicate_skipped_rows']==9082600 and all(x['ods_version_before']==x['ods_version_after'] for x in final['sources'])
    report={'verified_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'passed':True,'month':latest['month'],'full_raw_files_sha256_verified':len(objects),
            'ods_rows_by_source':totals,'total_ods_rows':sum(totals.values()),'committed_ods_version':version,'active_parquet_footers_checked':footers,
            'committed_file_sample_hashes_checked':sample_checks,'repeat_run_inserted_zero':idempotent,'latest_run_id':latest['run_id'],
            'scope':'Raw source integrity, ODS counts and sampled content identities; not DWD business quality or full payload equality audit'}
    (ROOT/'outputs/ods-verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
