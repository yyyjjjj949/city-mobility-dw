"""Local project orchestration. Requests contain named actions, never shell commands."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import time

import pendulum
from airflow.sdk import dag,task,get_current_context

WORK=Path('/opt/airflow/project-queue')


def run_action(action):
    context=get_current_context();ti=context['ti']
    conf=(context.get('dag_run').conf or {}) if context.get('dag_run') else {}
    month=conf.get('month',context.get('params',{}).get('month','2025-06'))
    if month!='2025-06':raise ValueError('Only verified month 2025-06 is currently supported; no downstream publication is allowed')
    identity='|'.join([ti.dag_id,ti.run_id,ti.task_id,str(ti.try_number)])
    job=hashlib.sha256(identity.encode()).hexdigest()
    heartbeat=json.loads((WORK/'worker-state.json').read_text())
    stamp=dt.datetime.fromisoformat(heartbeat['checked_at_utc'])
    if not heartbeat['running'] or (dt.datetime.now(dt.timezone.utc)-stamp).total_seconds()>30:
        raise RuntimeError('Local project worker is unavailable. Use 启动项目.cmd or Manage-Orchestration.ps1 -Action Start')
    target=WORK/'requests'/(job+'.json')
    deadline=dt.datetime.now(dt.timezone.utc)+dt.timedelta(minutes=45)
    if not target.exists():
        temp=target.with_suffix('.tmp')
        temp.write_text(json.dumps({'job_id':job,'action':action,'dag_id':ti.dag_id,'run_id':ti.run_id,
            'task_id':ti.task_id,'attempt':ti.try_number,'deadline_utc':deadline.isoformat(),'parameters':{'month':month}}))
        temp.replace(target)
    else:deadline=dt.datetime.fromisoformat(json.loads(target.read_text())['deadline_utc'])
    result=WORK/'results'/(job+'.json')
    while dt.datetime.now(dt.timezone.utc)<deadline:
        if result.exists():
            receipt=json.loads(result.read_text())
            if not receipt['passed']:raise RuntimeError(receipt.get('error','Project action failed'))
            return receipt
        time.sleep(1)
    raise TimeoutError('Timed out waiting for project action '+action)


DEFAULT={'retries':1,'retry_delay':dt.timedelta(minutes=1),'execution_timeout':dt.timedelta(minutes=47)}


@dag(dag_id='city_mobility_historical_daily',schedule='0 2 * * *',start_date=pendulum.datetime(2026,11,1,tz='Asia/Shanghai'),
    catchup=False,max_active_runs=1,max_active_tasks=1,default_args=DEFAULT,tags=['城市出行','历史指标'],
    params={'month':'2025-06'},
    doc_md='完整离线链路：采集与ODS → ODS验收 → DWD → DWD验收 → DWS/ADS校验发布 → 看板注册 → 图表验收 → 地图 → CSV → 健康检查。支持手动传入month=2025-06重跑；其他月份显式拒绝。相同来源SHA和已验证月分区复用，代码或规则改变则重建。')
def historical_daily():
    @task
    def ingest_ods():return run_action('ingest_historical')
    @task
    def verify_ods():return run_action('verify_historical_ods')
    @task
    def build_dwd():return run_action('build_historical_dwd')
    @task
    def verify_dwd():return run_action('verify_historical_dwd')
    @task
    def build_marts():return run_action('build_historical_marts')
    @task
    def register_dashboards():return run_action('build_historical_dashboards')
    @task
    def verify_charts():return run_action('verify_historical_charts')
    @task
    def export_map():return run_action('build_historical_map')
    @task
    def export_preview():return run_action('export_historical_preview')
    @task
    def inspect_project():return run_action('monitor_project')
    ingest_ods() >> verify_ods() >> build_dwd() >> verify_dwd() >> build_marts() >> register_dashboards() >> verify_charts() >> export_map() >> export_preview() >> inspect_project()


@dag(dag_id='city_mobility_health_monitor',schedule='*/5 * * * *',start_date=pendulum.datetime(2026,11,1,tz='Asia/Shanghai'),
    catchup=False,max_active_runs=1,max_active_tasks=1,default_args=DEFAULT,tags=['城市出行','运行监控'],
    doc_md='每五分钟检查服务、历史发布指针和离线数据完整性，生成本地运行状态页面；主动暂停采集不作为故障。异常会使任务失败并重试一次。')
def health_monitor():
    @task
    def inspect_project():return run_action('monitor_project')
    inspect_project()


historical_daily()
health_monitor()
