"""Run a fixed set of project actions requested by containerized Airflow."""
import datetime as dt
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

ROOT=Path(__file__).resolve().parents[1];WORK=ROOT/'work/orchestration';UTC=dt.timezone.utc
ACTIONS={
    'ingest_historical':['ingest_sources.py','--reuse-verified'],
    'verify_historical_ods':['verify_ods.py'],
    'build_historical_dwd':['build_dwd.py'],
    'verify_historical_dwd':['verify_dwd.py'],
    'build_historical_dashboards':['build_dashboards.py'],
    'build_historical_map':['build_map.py'],
    'historical_preflight':['historical_preflight.py'],
    'build_historical_marts':['build_marts.py'],
    'verify_historical_charts':['verify_dashboards.py'],
    'export_historical_preview':['export_marts_preview.py'],
    'monitor_project':['monitor_project.py'],
}


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix('.tmp');temp.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    for attempt in range(40):
        try:
            temp.replace(path);return
        except PermissionError:
            if attempt==39:raise
            time.sleep(.05)


def now():return dt.datetime.now(UTC).isoformat()


def execute(path):
    job=path.stem
    result={'job_id':job,'action':None,'started_at_utc':now(),'passed':False}
    try:
        request=json.loads(path.read_text(encoding='utf-8'));result['action']=request.get('action')
        assert len(job)==64 and all(c in '0123456789abcdef' for c in job),'Invalid job ID'
        assert request['job_id']==job and request['action'] in ACTIONS,'Unsupported project action'
        parameters=request.get('parameters',{'month':'2025-06'})
        assert isinstance(parameters,dict) and set(parameters)=={'month'} and parameters['month']=='2025-06','Only the validated month 2025-06 is supported'
        assert dt.datetime.fromisoformat(request['deadline_utc'])>dt.datetime.now(UTC),'Request expired'
        command=[sys.executable,str(ROOT/'tools'/ACTIONS[request['action']][0]),*ACTIONS[request['action']][1:]]
        if request['action'] in ['ingest_historical','build_historical_dwd']:
            command+=['--month',parameters['month']]
        result['parameters']=parameters
        if request['action'] in ['build_historical_dwd','build_historical_marts']:
            from compute_lock import compute_lock
            while True:
                try:
                    with compute_lock():pass
                    break
                except RuntimeError:
                    if dt.datetime.now(UTC)>=dt.datetime.fromisoformat(request['deadline_utc']):
                        raise TimeoutError('Timed out waiting for the current Spark job')
                    time.sleep(2)
        with (WORK/'logs'/(job+'.log')).open('a',encoding='utf-8') as log:
            # No request text is interpreted as a command or argument.
            proc=subprocess.Popen(command,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW)
            # Keep an in-progress receipt. Worker restarts fail interrupted attempts;
            # Airflow retries use a new attempt ID and idempotent project scripts.
            save(WORK/'running'/path.name,{**result,'pid':proc.pid})
            while proc.poll() is None:
                if dt.datetime.now(UTC)>=dt.datetime.fromisoformat(request['deadline_utc']):
                    subprocess.run(['taskkill','/PID',str(proc.pid),'/T','/F'],capture_output=True)
                    proc.wait(timeout=30);raise TimeoutError('Project action exceeded its deadline; inspect the job log')
                time.sleep(.5)
            result.update(exit_code=proc.returncode,passed=proc.returncode==0)
            if proc.returncode:result['error']='Project action failed; see work/orchestration/logs/'+job+'.log'
    except Exception as exc:result['error']=type(exc).__name__+': '+str(exc)[:500]
    result['finished_at_utc']=now()
    save(WORK/'results'/path.name,result)
    (WORK/'running'/path.name).unlink(missing_ok=True)


def main():
    import msvcrt
    for name in ['requests','results','logs','running']:(WORK/name).mkdir(parents=True,exist_ok=True)
    os.environ['DOCKER_CONFIG']=r'D:\CodexData\app-data\docker-profile\cli'
    os.environ['PYTHONUTF8']='1'
    with (WORK/'worker.lock').open('a+b') as lock:
        lock.seek(0);lock.write(b'0');lock.flush();lock.seek(0)
        try:msvcrt.locking(lock.fileno(),msvcrt.LK_NBLCK,1)
        except OSError:return
        # Do not blindly repeat an action whose owner crashed.
        for old in (WORK/'running').glob('*.json'):
            result=json.loads(old.read_text(encoding='utf-8'))
            child=result.get('pid')
            if child:
                listing=subprocess.check_output(['tasklist','/FI','PID eq '+str(int(child)),'/FO','CSV','/NH'],text=True,encoding='utf-8',errors='replace')
                if any(len(row)>1 and row[1]==str(child) for row in csv.reader(listing.splitlines())):
                    raise RuntimeError('Interrupted job still has a live child PID '+str(child)+'. Check it before restarting the worker.')
            if not (WORK/'results'/old.name).exists():
                save(WORK/'results'/old.name,{**result,'passed':False,'error':'Worker was interrupted; check any surviving child before retrying','finished_at_utc':now()})
            old.unlink()
        stop=WORK/'stop.request';stop.unlink(missing_ok=True)
        done=threading.Event();active={'job_id':None,'action':None}
        def heartbeat():
            while not done.is_set():
                try:save(WORK/'worker-state.json',{'running':True,'pid':os.getpid(),'checked_at_utc':now(),**active})
                except PermissionError:pass  # A Windows reader may briefly hold the target; retry next heartbeat.
                done.wait(2)
        thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
        try:
            while not stop.exists():
                pending=sorted((p for p in (WORK/'requests').glob('*.json') if not (WORK/'results'/p.name).exists()),key=lambda p:p.stat().st_mtime)
                if pending:
                    request=pending[0]
                    try:action=json.loads(request.read_text(encoding='utf-8')).get('action')
                    except Exception:action=None
                    active.update(job_id=request.stem,action=action)
                    execute(request);active.update(job_id=None,action=None)
                else:time.sleep(.5)
        finally:
            done.set();thread.join(timeout=5)
            save(WORK/'worker-state.json',{'running':False,'pid':os.getpid(),'checked_at_utc':now(),'job_id':None,'action':None})


if __name__=='__main__':main()
