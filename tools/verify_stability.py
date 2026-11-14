"""Record bounded live service probes; report measured duration, never infer days of uptime."""
import argparse
import datetime as dt
import json
from pathlib import Path
import time
import requests
from publish_marts import ClickHouse

ROOT=Path(__file__).resolve().parents[1]


def main():
    p=argparse.ArgumentParser();p.add_argument('--soak-seconds',type=int,default=900);p.add_argument('--interval',type=int,default=20);args=p.parse_args()
    assert 30<=args.soak_seconds<=86400 and 1<=args.interval<=60
    s=requests.Session();s.trust_env=False;ch=ClickHouse()
    report={'started_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'requested_seconds':args.soak_seconds,'samples':[],'passed':False}
    start=time.monotonic()
    while True:
        sample={'at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'passed':False}
        try:
            assert s.get('http://127.0.0.1:8088/health',timeout=10).status_code==200
            a=s.get('http://127.0.0.1:18080/api/v2/monitor/health',timeout=10);a.raise_for_status()
            assert all(a.json()[k]['status']=='healthy' for k in ['metadatabase','scheduler','dag_processor'])
            assert int(ch.query('SELECT sum(trip_count) AS n FROM ads.bi_trip_day')[0]['n'])==9012795
            pointers=ch.query('SELECT dataset_id,release_id FROM ads.release_pointer')
            assert {r['dataset_id'] for r in pointers}=={'historical','quality'} and len({r['release_id'] for r in pointers})==1
            sample.update(passed=True,release_id=pointers[0]['release_id'])
        except Exception as exc:sample['error_type']=type(exc).__name__
        report['samples'].append(sample);report['measured_seconds']=round(time.monotonic()-start,2)
        report['passed']=all(x['passed'] for x in report['samples']) and report['measured_seconds']>=args.soak_seconds
        (ROOT/'outputs/stability-soak-verification.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
        print('STABILITY_PROBE '+str(len(report['samples']))+' passed='+str(sample['passed']),flush=True)
        if time.monotonic()-start>=args.soak_seconds:break
        time.sleep(min(args.interval,args.soak_seconds-(time.monotonic()-start)))
    assert report['passed'],'At least one live service probe failed; inspect stability-soak-verification.json'


if __name__=='__main__':main()
