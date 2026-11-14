"""Create/update local Superset dashboards with stable chart and dashboard IDs."""
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
DOCKER = r'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'

def main():
    os.environ['DOCKER_CONFIG'] = r'D:\CodexData\app-data\docker-profile\cli'
    command = [DOCKER, 'compose', '-f', str(ROOT / 'compose.yaml')]
    report = ROOT / 'outputs/superset-historical-dashboards.json'
    previous = json.loads(report.read_text(encoding='utf-8')) if report.exists() else None
    log = ROOT / 'work/marts/superset-build.log'; log.parent.mkdir(parents=True, exist_ok=True)
    with log.open('w', encoding='utf-8') as stream:
        subprocess.run(command + ['exec', '-T', 'superset', 'python', '/app/project/build_historical_dashboards.py'], cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=True)
    subprocess.run(command + ['cp', 'superset:/app/superset_home/historical-dashboards-result.json', str(report)], cwd=ROOT, check=True, capture_output=True)
    current = json.loads(report.read_text(encoding='utf-8'))
    if previous:
        for category in ['datasets', 'charts', 'dashboards']:
            assert [x['id'] for x in previous[category]] == [x['id'] for x in current[category]], category + ' IDs changed'
        (ROOT / 'outputs/superset-rerun-verification.json').write_text(json.dumps({'passed':True,'stable_ids':True,'datasets':len(current['datasets']),'charts':len(current['charts']),'dashboards':len(current['dashboards'])},indent=2),encoding='utf-8')
    print('SUPERSET_DASHBOARDS_OK charts='+str(len(current['charts']))+' dashboards='+str(len(current['dashboards'])))

if __name__ == '__main__': main()
