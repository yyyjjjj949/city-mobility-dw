"""Enable project DAGs and verify real scheduler-driven runs through Airflow v2 API."""
import argparse
import datetime as dt
import json
from pathlib import Path
import time
from urllib.parse import quote
import uuid
import requests

ROOT=Path(__file__).resolve().parents[1]
DAGS=['city_mobility_historical_daily','city_mobility_health_monitor']


def client():
    session=requests.Session();session.trust_env=False
    password=json.loads((ROOT/'config/airflow/passwords.json').read_text(encoding='utf-8'))['admin']
    response=session.post('http://127.0.0.1:18080/auth/token',json={'username':'admin','password':password},timeout=30)
    response.raise_for_status();session.headers['Authorization']='Bearer '+response.json()['access_token']
    return session


def api(session,method,path,**kwargs):
    response=session.request(method,'http://127.0.0.1:18080/api/v2'+path,timeout=30,**kwargs)
    response.raise_for_status();return response.json()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--trigger-only',action='store_true');parser.add_argument('--inspect',action='store_true');args=parser.parse_args()
    session=client();errors=api(session,'GET','/importErrors',params={'filename':'/opt/airflow/dags/city_mobility_operations.py'})
    assert errors['total_entries']==0,'DAG import failed'
    if args.inspect:
        records=json.loads((ROOT/'outputs/airflow-verification.json').read_text(encoding='utf-8'))['scheduler_runs']
    else:
        records=[]
        for dag in DAGS:
            meta=api(session,'PATCH','/dags/'+dag,json={'is_paused':False})
            assert not meta['is_paused']
            run='acceptance_'+uuid.uuid4().hex
            result=api(session,'POST','/dags/'+dag+'/dagRuns',json={'dag_run_id':run,'logical_date':None,'note':'Local project orchestration verification'})
            records.append({'dag_id':dag,'dag_run_id':result['dag_run_id'],'schedule':meta.get('timetable_description'),'state':result['state']})
    if args.trigger_only:
        print(json.dumps(records,indent=2));return
    deadline=time.monotonic()+3600;last=None
    while time.monotonic()<deadline:
        for item in records:
            base='/dags/'+item['dag_id']+'/dagRuns/'+quote(item['dag_run_id'],safe='')
            result=api(session,'GET',base)
            item.update(state=result['state'],start_date=result.get('start_date'),end_date=result.get('end_date'))
            tasks=api(session,'GET',base+'/taskInstances')['task_instances']
            item['tasks']=[{'task_id':t['task_id'],'state':t['state'],'try_number':t['try_number']} for t in tasks]
        snapshot=json.dumps(records,ensure_ascii=False)
        if snapshot!=last:print(snapshot,flush=True);last=snapshot
        if all(item['state'] in ['success','failed'] for item in records):break
        time.sleep(3)
    report={'passed':all(r['state']=='success' and r['tasks'] and all(t['state']=='success' for t in r['tasks']) for r in records),
        'checked_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'import_errors':errors['total_entries'],'scheduler_runs':records}
    scheduled=api(session,'GET','/dags/city_mobility_health_monitor/dagRuns',params={'order_by':'-run_after','limit':10})['dag_runs']
    report['scheduled_monitor_runs']=[]
    for run in scheduled:
        if run['dag_run_id'].startswith('scheduled__'):
            tasks=api(session,'GET','/dags/city_mobility_health_monitor/dagRuns/'+quote(run['dag_run_id'],safe='')+'/taskInstances')['task_instances']
            report['scheduled_monitor_runs'].append({'dag_run_id':run['dag_run_id'],'state':run['state'],
                'tasks':[{'task_id':t['task_id'],'state':t['state'],'try_number':t['try_number']} for t in tasks]})
    (ROOT/'outputs/airflow-verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    assert report['passed'],'Inspect outputs/airflow-verification.json and Airflow task logs'
    print('AIRFLOW_SCHEDULER_VERIFIED',flush=True)


if __name__=='__main__':main()
