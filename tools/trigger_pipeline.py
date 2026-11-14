"""Trigger the complete supported monthly offline pipeline through Airflow."""
import argparse
import uuid
from verify_orchestration import client,api


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--month',choices=['2025-06'],default='2025-06');args=parser.parse_args()
    s=client();dag='city_mobility_historical_daily'
    api(s,'PATCH','/dags/'+dag,json={'is_paused':False})
    run=api(s,'POST','/dags/'+dag+'/dagRuns',json={'dag_run_id':'monthly_'+uuid.uuid4().hex,'logical_date':None,'conf':{'month':args.month},'note':'Complete offline monthly pipeline'})
    print('PIPELINE_QUEUED month='+args.month+' run='+run['dag_run_id'])
    print('http://localhost:18080/dags/'+dag)


if __name__=='__main__':main()
