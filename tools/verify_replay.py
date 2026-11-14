"""Replay the actual Airflow chain and compare business snapshots before/after."""
import datetime as dt, json, shutil, subprocess, sys
from pathlib import Path
from publish_marts import ClickHouse
ROOT=Path(__file__).resolve().parents[1]

def read(name):return json.loads((ROOT/'outputs'/name).read_text(encoding='utf-8'))
def snapshot():
    dwd=read('dwd-latest.json');marts=read('marts-latest.json')
    return {'dwd_run_id':dwd['run_id'],'release_id':marts['release_id'],
      'dwd_versions':{k:max(int(p.stem) for p in (ROOT/'data'/k.replace('.','/')/'_delta_log').glob('[0-9]'*20+'.json')) for k in dwd['output_versions'] if k.startswith('dwd.')},
      'marts_versions':{k:max(int(p.stem) for p in (ROOT/'data'/k.replace('.','/')/'_delta_log').glob('[0-9]'*20+'.json')) for k in marts['output_versions']},
      'pointers':ClickHouse().query('SELECT dataset_id,release_id FROM ads.release_pointer ORDER BY dataset_id')}
def main():
    before=snapshot();started=dt.datetime.now(dt.timezone.utc).isoformat()
    shutil.copyfile(ROOT/'outputs/airflow-verification.json',ROOT/'outputs/pipeline-first-run.json')
    run=subprocess.run([sys.executable,str(ROOT/'tools/verify_orchestration.py')],cwd=ROOT,timeout=3700)
    after=snapshot();receipts={name:read(name+'-rerun-verification.json') for name in ['ods','dwd','marts']}
    fresh=all(r.get('passed') and r.get('checked_at_utc','')>=started for r in receipts.values())
    report={'passed':run.returncode==0 and before==after and fresh,'started_at_utc':started,
      'finished_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'before':before,'after':after,
      'fresh_reuse_receipts':fresh,'receipts':receipts,'airflow':read('airflow-verification.json')}
    (ROOT/'outputs/pipeline-replay-verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print('PIPELINE_REPLAY '+str(report['passed']),flush=True)
    assert report['passed'],'Inspect pipeline-replay-verification.json'
if __name__=='__main__':main()
