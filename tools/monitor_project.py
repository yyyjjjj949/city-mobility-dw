"""Check offline warehouse services, historical publications and scheduler health."""
import datetime as dt
import html
import json
import os
from pathlib import Path
import subprocess

import requests
from publish_marts import ClickHouse

ROOT=Path(__file__).resolve().parents[1];UTC=dt.timezone.utc
DOCKER=r'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'


def read(path):return json.loads(path.read_text(encoding='utf-8'))


def age(stamp,now):
    parsed=dt.datetime.fromisoformat(stamp)
    if parsed.tzinfo is None:parsed=parsed.replace(tzinfo=UTC)
    return int((now-parsed).total_seconds())


def main():
    now=dt.datetime.now(UTC);checks=[];issues=[];pointers=[]
    def check(name,fn):
        try:
            detail=fn();checks.append({'name':name,'passed':True,'detail':detail});return detail
        except Exception as exc:
            # Do not embed raw HTTP responses or credentials in the report.
            issues.append(name+' 检查失败（'+type(exc).__name__+'），请查看对应服务日志。')
            checks.append({'name':name,'passed':False,'error_type':type(exc).__name__});return None
    os.environ['DOCKER_CONFIG']=r'D:\CodexData\app-data\docker-profile\cli'
    def containers():
        payload=subprocess.check_output([DOCKER,'compose','-f',str(ROOT/'compose.yaml'),'ps','--format','json'],cwd=ROOT,text=True,encoding='utf-8',timeout=30)
        values=[json.loads(line) for line in payload.splitlines() if line.strip()]
        states={v['Service']:{'state':v['State'],'health':v.get('Health','')} for v in values}
        for service in ['postgres','clickhouse','spark','airflow','superset']:
            assert service in states and states[service]['state']=='running',service
            assert states[service]['health'] in ['','healthy'],service
        return states
    check('容器服务',containers)
    session=requests.Session();session.trust_env=False
    def http(url):
        response=session.get(url,timeout=15);response.raise_for_status();return response
    check('Superset',lambda:{'http_status':http('http://127.0.0.1:8088/health').status_code})
    def airflow():
        value=http('http://127.0.0.1:18080/api/v2/monitor/health').json()
        assert all(value[n]['status']=='healthy' for n in ['metadatabase','scheduler','dag_processor'])
        return value
    check('Airflow调度器',airflow)
    def serving():
        nonlocal pointers
        ch=ClickHouse();pointers=ch.query('SELECT dataset_id,release_id,activated_at_utc FROM ads.release_pointer')
        assert len(pointers)==len({p['dataset_id'] for p in pointers})
        lookup={p['dataset_id']:p['release_id'] for p in pointers}
        history=read(ROOT/'outputs/marts-latest.json')
        assert lookup['historical']==lookup['quality']==history['release_id']
        totals=ch.query('SELECT sum(trip_count) AS trips FROM ads.bi_trip_day')[0]
        assert int(totals['trips'])==read(ROOT/'outputs/dwd-latest.json')['total_valid_trips']
        return {'pointers':[p for p in pointers if p['dataset_id'] in ['historical','quality']],'historical_trips':int(totals['trips'])}
    check('ClickHouse发布与口径',serving)
    def worker():
        state=read(ROOT/'work/orchestration/worker-state.json')
        assert state['running'] and age(state['checked_at_utc'],now)<30
        return state
    check('本机任务执行器',worker)
    # Only retain real failures since the latest success of the same action.
    latest={}
    for path in (ROOT/'work/orchestration/results').glob('*.json'):
        result=read(path);action=result.get('action')
        if action and (action not in latest or result['finished_at_utc']>latest[action]['finished_at_utc']):latest[action]=result
    failures=[{'action':action,'job_id':r['job_id'],'finished_at_utc':r['finished_at_utc']} for action,r in latest.items() if not r['passed'] and action!='monitor_project']
    if failures:issues.append('存在尚未恢复的本机业务任务失败，请在Airflow查看重试与日志。')
    report={'passed':not issues,'status':'healthy' if not issues else 'degraded','checked_at_utc':now.isoformat(),
        'checked_at_beijing':now.astimezone(dt.timezone(dt.timedelta(hours=8))).isoformat(),'checks':checks,'issues':issues,
        'mode':'offline','latest_actions':latest,'unresolved_failures':failures}
    target=ROOT/'outputs/运行状态.json';previous=read(target) if target.exists() else {}
    if (previous.get('status'),previous.get('issues'))!=(report['status'],report['issues']):
        with (ROOT/'work/orchestration/status-transitions.jsonl').open('a',encoding='utf-8') as out:
            out.write(json.dumps({'checked_at_utc':now.isoformat(),'status':report['status'],'issues':issues},ensure_ascii=False)+'\n')
    temp=target.with_name(target.name+'.'+str(os.getpid())+'.tmp');temp.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8');temp.replace(target)
    render(report)
    print('PROJECT_MONITOR '+report['status']+' mode=offline',flush=True)
    if issues:raise RuntimeError('Project health checks failed; inspect outputs/运行状态.json')


def render(report):
    esc=lambda value:html.escape(str(value))
    rows=''.join('<tr><td>'+esc(c['name'])+'</td><td>'+('正常' if c['passed'] else '异常')+'</td></tr>' for c in report['checks'])
    alerts=''.join('<li>'+esc(issue)+'</li>' for issue in report['issues']) or '<li>本次检查未发现故障。</li>'
    text='''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta http-equiv="refresh" content="60"><title>城市出行 · 运行状态</title>
<style>body{font:16px/1.8 "Microsoft YaHei",sans-serif;background:#f3f6fa;color:#19334e;max-width:1000px;margin:40px auto;padding:24px}section{background:white;padding:24px;border-radius:12px;margin:20px 0}td{padding:8px 35px 8px 0;border-bottom:1px solid #e3eaf1}a{color:#126aac}.good{color:#157455}.bad{color:#b33428}</style>
<h1>城市出行 · 运行状态</h1><p>检查时间（北京时间）：STAMP<br><span id="age"></span> · 每五分钟自动检查，页面每分钟刷新。</p>
<section><h2 class="STATUS">TITLE</h2><ul>ALERTS</ul><table>ROWS</table></section>
<section><h2>离线数仓</h2><p>业务数据为2025年6月历史行程及日天气，按批次清洗与汇总。看板查询最近一次通过校验的发布版本，数据范围与本次运行检查时间分别展示。</p><a href="marts/六月历史指标与看板.html">历史分析与CSV导出</a> · <a href="http://localhost:18080">Airflow调度与任务日志</a> · <a href="项目总入口.html">项目总入口</a><p>手动检查：双击项目根目录“检查运行状态.cmd”。电脑休眠、执行器或Docker停止时不会生成新的检查结果。</p></section>
<script>const stamp=Date.parse('ISOTIME');function update(){const n=Math.max(0,Math.floor((Date.now()-stamp)/1000));const el=document.getElementById('age');el.textContent='检查结果距今 '+n+' 秒'+(n>600?'，报告已过期，请启动项目或手动检查':'');el.className=n>600?'bad':'';}update();setInterval(update,1000);</script></html>'''
    replacements={'STAMP':esc(report['checked_at_beijing']),'STATUS':'good' if report['passed'] else 'bad','TITLE':'服务正常' if report['passed'] else '需要处理',
        'ALERTS':alerts,'ROWS':rows,'ISOTIME':report['checked_at_utc']}
    # Delimit replacements to avoid replacing AGE within unrelated markup.
    import re
    text=re.sub(r'\b('+'|'.join(replacements)+r')\b',lambda match:replacements[match.group()],text)
    path=ROOT/'outputs/运行状态.html';temp=path.with_name(path.name+'.'+str(os.getpid())+'.tmp');temp.write_text(text,encoding='utf-8');temp.replace(path)


if __name__=='__main__':main()
