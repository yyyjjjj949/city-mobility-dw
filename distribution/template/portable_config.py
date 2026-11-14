"""Machine independent settings for the Windows team distribution."""
import os
from pathlib import Path
import shutil

ROOT=Path(__file__).resolve().parents[1]
for line in (ROOT/'.env').read_text(encoding='utf-8').splitlines():
    if '=' in line and not line.lstrip().startswith('#'):
        key,value=line.split('=',1)
        os.environ.setdefault(key,value.strip().strip('"').strip("'"))
os.environ['PYTHONUTF8']='1'
os.environ['PYTHONIOENCODING']='utf-8'
os.environ['TEMP']=os.environ['TMP']=str(ROOT/'work/temp')
(ROOT/'work/temp').mkdir(parents=True,exist_ok=True)
DOCKER=os.environ.get('MOBILITY_DOCKER') or shutil.which('docker')
if not DOCKER:
    candidate=Path(os.environ.get('ProgramFiles',r'C:\Program Files'))/'Docker/Docker/resources/bin/docker.exe'
    DOCKER=str(candidate) if candidate.is_file() else 'docker'
SUPERSET='http://127.0.0.1:'+os.environ.get('MOBILITY_SUPERSET_PORT','8088')
AIRFLOW='http://127.0.0.1:'+os.environ.get('MOBILITY_AIRFLOW_PORT','18080')
CLICKHOUSE='http://127.0.0.1:'+os.environ.get('MOBILITY_CLICKHOUSE_PORT','18123')
PROJECT=os.environ.get('MOBILITY_PROJECT_NAME','city-mobility-team')

def expand_urls(text):
    for old,new in [('http://127.0.0.1:18123',CLICKHOUSE),('http://localhost:18123',CLICKHOUSE),
                    ('http://127.0.0.1:18080',AIRFLOW),('http://localhost:18080',AIRFLOW),
                    ('http://127.0.0.1:8088',SUPERSET),('http://localhost:8088',SUPERSET)]:
        text=text.replace(old,new)
    return text
