"""Restart the local offline stack once and verify serving, scheduler and data identity."""
import datetime as dt, json, os, subprocess
from pathlib import Path
import requests
from publish_marts import ClickHouse

ROOT=Path(__file__).resolve().parents[1]
DOCKER=r'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'

def env_values():
    return {k:v.strip().strip('"').strip("'") for k,v in (line.split('=',1) for line in (ROOT/'.env').read_text(encoding='utf-8').splitlines() if '=' in line and not line.startswith('#'))}

def snapshot():
    ch=ClickHouse(); p=ch.query('SELECT dataset_id,release_id FROM ads.release_pointer ORDER BY dataset_id')
    counts={name:int(ch.query('SELECT count() AS n FROM ads.bi_'+name)[0]['n']) for name in ['trip_day','trip_hour','zone_hour','od_day','weather_group','quality_batch','quality_rule']}
    versions={name:max(int(x.stem) for x in (ROOT/'data/dwd'/name/'_delta_log').glob('[0-9]'*20+'.json')) for name in ['fact_taxi_trip','fact_bike_trip']}
    return {'pointers':p,'trip_count':int(ch.query('SELECT sum(trip_count) AS n FROM ads.bi_trip_day')[0]['n']),'active_view_counts':counts,'fact_delta_versions':versions}

def services():
    os.environ['DOCKER_CONFIG']=r'D:\CodexData\app-data\docker-profile\cli'
    out=subprocess.check_output([DOCKER,'compose','ps','--format','json'],cwd=ROOT,text=True,encoding='utf-8')
    rows=[json.loads(x) for x in out.splitlines() if x]
    return sorted((r['Service'],r['State'],r.get('Health','')) for r in rows)

def main():
    before=snapshot(); before_services=services()
    with (ROOT/'work/acceptance-2026-11-14/restart.log').open('w',encoding='utf-8') as log:
        for action in ['Stop','Start']:
            proc=subprocess.run(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File',str(ROOT/'Manage-Project.ps1'),'-Action',action],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,timeout=600)
            if proc.returncode: raise RuntimeError('Service '+action+' failed; inspect work/acceptance-2026-11-14/restart.log')
    after=snapshot(); after_services=services(); values=env_values()
    s=requests.Session(); s.trust_env=False
    sup=s.post('http://127.0.0.1:8088/api/v1/security/login',json={'username':'admin','password':values['SUPERSET_ADMIN_PASSWORD'],'provider':'db','refresh':True},timeout=30)
    superset_login=sup.ok
    map_response=s.get('http://127.0.0.1:8088/city-mobility/map/',timeout=30)
    map_route=map_response.ok and 'map.js' in map_response.text
    af=s.post('http://127.0.0.1:18080/auth/token',json={'username':'admin','password':json.loads((ROOT/'config/airflow/passwords.json').read_text(encoding='utf-8'))['admin']},timeout=30)
    airflow_login=af.ok
    health=s.get('http://127.0.0.1:18080/api/v2/monitor/health',timeout=30).json()
    airflow_health=all(health[k]['status']=='healthy' for k in ['metadatabase','scheduler','dag_processor'])
    worker=json.loads((ROOT/'work/orchestration/worker-state.json').read_text(encoding='utf-8'))
    report={'passed':bool(superset_login and airflow_login and airflow_health and map_route and worker['running'] and before==after and len(after_services)==5 and all(x[1]=='running' and x[2] in ('','healthy') for x in after_services)),
      'checked_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'before_services':before_services,'after_services':after_services,
      'superset_admin_login':superset_login,'airflow_admin_login':airflow_login,'before':before,'after':after,
      'map_route':map_route,'airflow_scheduler_health':airflow_health,'worker_state':worker}
    (ROOT/'outputs/restart-verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'passed':report['passed'],'superset_admin_login':superset_login,'airflow_admin_login':airflow_login,'trip_count':after['trip_count']},ensure_ascii=False))
    if not report['passed']: raise SystemExit(1)
if __name__=='__main__': main()
