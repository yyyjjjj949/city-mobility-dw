"""Exercise every saved chart through Superset's authenticated query API."""
import datetime as dt
import json
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]

def main():
    env = dict(line.split('=', 1) for line in (ROOT / '.env').read_text(encoding='utf-8').splitlines() if '=' in line and not line.startswith('#'))
    session = requests.Session(); session.trust_env = False
    base = 'http://127.0.0.1:8088'
    response = session.post(base + '/api/v1/security/login', json={'username': 'admin', 'password': env['SUPERSET_ADMIN_PASSWORD'].strip().strip('"').strip("'"), 'provider': 'db', 'refresh': True}, timeout=30)
    response.raise_for_status()
    session.headers['Authorization'] = 'Bearer ' + response.json()['access_token']
    csrf = session.get(base + '/api/v1/security/csrf_token/', timeout=30); csrf.raise_for_status()
    session.headers['X-CSRFToken'] = csrf.json()['result']
    session.headers['Referer'] = base + '/'
    saved = json.loads((ROOT / 'outputs/superset-historical-dashboards.json').read_text(encoding='utf-8'))
    checks = []
    for chart in saved['charts']:
        meta = session.get(base + f"/api/v1/chart/{chart['id']}", timeout=30); meta.raise_for_status()
        context = json.loads(meta.json()['result']['query_context']); context['force'] = True
        response = session.post(base + '/api/v1/chart/data', json=context, timeout=180)
        payload = response.json()
        good = response.ok and bool(payload.get('result')) and all(x.get('status') == 'success' and x.get('data') for x in payload.get('result', []))
        item = dict(chart, passed=bool(good), http_status=response.status_code)
        if good:
            result = payload['result'][0]
            item.update(rows=len(result['data']), columns=result['colnames'], sample=result['data'][:2])
            if chart['name'] == '已接入两类有效行程': assert result['data'][0]['行程数'] == 9012795
            if chart['name'] == '黄色出租车行程': assert result['data'][0]['行程数'] == 4254337
            if chart['name'] == '共享单车骑行': assert result['data'][0]['行程数'] == 4758458
            if '当地小时' in chart['name']: assert len(result['data']) == 24
            if chart['name'].startswith('出发量最多'): assert len(result['data']) == 15
            if chart['name'].startswith('热门 OD'): assert len(result['data']) == 20
        else: item['error'] = payload
        checks.append(item)
        print(json.dumps({'id':chart['id'],'passed':good,'rows':item.get('rows'),'error':None if good else payload}, ensure_ascii=False), flush=True)
    report = {'passed': all(x['passed'] for x in checks), 'checked_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(), 'chart_count':len(checks), 'charts':checks}
    (ROOT / 'outputs/superset-chart-verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    assert report['passed'], 'Inspect chart verification report'

if __name__ == '__main__': main()
