"""Check the active offline scope against services, serving data and BI metadata."""
import portable_config as _portable
import datetime as dt 
import hashlib 
import json 
import os 
from pathlib import Path 
import subprocess 
import requests 
from publish_marts import ClickHouse 
from ods_snapshot import verified_month_version 

ROOT =Path (__file__ ).resolve ().parents [1 ]
DOCKER =_portable .DOCKER 


def main ():
    pass # Use the recipient Docker CLI configuration.
    def read (name ):return json .loads ((ROOT /name ).read_text (encoding ='utf-8'))
    def docker (*args ):return subprocess .check_output ([DOCKER ,*args ],cwd =ROOT ,text =True ,encoding ='utf-8')
    services =docker ('compose','config','--services').split ()
    assert set (services )=={'postgres','clickhouse','spark','airflow','superset'}
    containers =[json .loads (s )for s in docker ('compose','ps','--format','json').splitlines ()if s ]
    assert {c ['Service']for c in containers }==set (services )
    assert all (c ['State']=='running'and c .get ('Health','')in ['','healthy']for c in containers )
    assert 'kafka' not in services
    contract =read ('config/model/schema-contract.json');names ={t ['name']for t in contract ['tables']}
    assert contract ['version']=='2.0-offline'and len (names )==28 
    retired ={'dwd.station_link','dwd.fact_station_status_snapshot','dws.station_hour','ads.station_latest','ads.station_hour'}
    assert not names .intersection (retired )
    assert len ([n for n in names if n .startswith ('dwd.dim_')])==7 
    assert len ([n for n in names if n .startswith ('dwd.fact_')])==2 
    ch =ClickHouse ();pointers =ch .query ('SELECT dataset_id,release_id FROM ads.release_pointer')
    assert {r ['dataset_id']for r in pointers }=={'historical','quality'}
    release =read ('outputs/marts-latest.json')['release_id']
    assert all (r ['release_id']==release for r in pointers )
    views =ch .query ("SELECT name FROM system.tables WHERE database='ads' AND engine='View'")
    assert {r ['name']for r in views }=={'bi_'+n for n in ['trip_day','trip_hour','zone_hour','od_day','weather_group','quality_batch','quality_rule']}
    trips =int (ch .query ('SELECT sum(trip_count) AS n FROM ads.bi_trip_day')[0 ]['n'])
    assert trips ==9012795 
    values ={}
    for line in (ROOT /'.env').read_text (encoding ='utf-8').splitlines ():
        if '='in line and not line .startswith ('#'):
            k ,v =line .split ('=',1 );values [k ]=v .strip ().strip ('"').strip ("'")
    session =requests .Session ();session .trust_env =False 
    base =_portable.expand_urls('http://127.0.0.1:8088')
    login =session .post (base +'/api/v1/security/login',json ={'username':'admin','password':values ['SUPERSET_ADMIN_PASSWORD'],'provider':'db','refresh':True },timeout =30 )
    login .raise_for_status ();session .headers ['Authorization']='Bearer '+login .json ()['access_token']
    def listing (endpoint ):
        response =session .get (base +'/api/v1/'+endpoint +'/',params ={'q':'(page:0,page_size:100)'},timeout =30 )
        response .raise_for_status ();data =response .json ()
        assert data ['count']==len (data ['result']),'Uninspected metadata pages'
        return data ['result']
    dashboards =listing ('dashboard');charts =listing ('chart');datasets =listing ('dataset')
    expected =read ('outputs/superset-historical-dashboards.json')
    assert {d ['slug']for d in dashboards }=={d ['slug']for d in expected ['dashboards']}
    assert len (dashboards )==4 and len (charts )==19 
    assert not any (d ['table_name']in ['city_station_current','bi_station_hour']for d in datasets )
    model =read ('outputs/model-validation.json')
    assert model ['passed']and model ['contract_sha256']==hashlib .sha256 ((ROOT /'config/model/schema-contract.json').read_bytes ()).hexdigest ()
    for file in ['superset-chart-verification','superset-browser-verification','airflow-verification','offline-browser-verification','offline-serving-retirement','map-build-verification','map-browser-verification','stability-soak-verification','restart-verification','pipeline-replay-verification','pipeline-failure-verification','orchestration-browser-verification']:
        assert read ('outputs/'+file +'.json')['passed'],file 
    native =read ('outputs/native-filter-browser-verification.json')
    assert native ['passed']and {'native_transport_filter_updates_total_KPI','native_date_filter_updates_total_KPI'}<={c ['name']for c in native .get ('cases',[])if c .get ('passed')},'native filter acceptance'
    airflow =read ('outputs/airflow-verification.json')
    historical =next (r for r in airflow ['scheduler_runs']if r ['dag_id']=='city_mobility_historical_daily')
    assert len (historical ['tasks'])==10 and all (t ['state']=='success'for t in historical ['tasks'])
    assert read ('outputs/map-build-verification.json')['release_id']==release ,'Map is from another release'
    assert read ('outputs/pipeline-replay-verification.json')['after']['release_id']==release 
    assert read ('outputs/restart-verification.json')['after']['pointers']==sorted (pointers ,key =lambda r :r ['dataset_id'])
    soak =read ('outputs/stability-soak-verification.json')
    assert soak ['measured_seconds']>=900 and len (soak ['samples'])>=30 and all (s ['passed']for s in soak ['samples'])
    monitor =read ('outputs/运行状态.json')
    assert monitor ['mode']=='offline'and monitor ['status']=='healthy'
    pinned =verified_month_version (ROOT ,read ('outputs/ods-ingestion-latest.json'),read ('outputs/ods-verification.json'))
    startup =(ROOT /'tools/portable.py').read_text (encoding ='utf-8')
    assert 'Manage-Realtime'not in startup and not (ROOT /'Manage-Realtime.ps1').exists ()
    assert all((ROOT/name).exists() for name in ['01-first-install.cmd','02-start.cmd','03-stop.cmd','04-check.cmd','05-run-full-pipeline.cmd','06-open-dashboard.cmd'])
    report ={'passed':True ,'checked_at_utc':dt .datetime .now (dt .timezone .utc ).isoformat (),'mode':'offline',
    'services':services ,'kafka_not_configured':True ,
    'logical_tables':len (names ),'dimensions':7 ,'historical_facts':2 ,
    'historical_dashboards':4 ,'historical_charts':19 ,'valid_historical_trips':trips ,
    'release_id':release ,'active_datasets':sorted (r ['dataset_id']for r in pointers ),
    'historical_ods_snapshot_version':pinned ,'model_engine_validation':True ,
    'browser_validation':True ,'map_validation':True ,'native_filter_validation':True ,
    'airflow_scheduler_validation':True ,'full_pipeline_tasks':10 ,'replay_validation':True ,'restart_validation':True ,
    'soak_measured_seconds':soak ['measured_seconds'],'soak_samples':len (soak ['samples']),
    'distribution':'Windows x64 portable team bundle','business_rows_deleted':False ,
    'remaining_work':['Report, slides and demonstration video'],
    'scope_limits':['Only verified month 2025-06 is supported','Complete arrivals/net inflow require May boundary sources','Short measured soak and planned restart do not establish multi-day or power-loss stability']}
    (ROOT /'outputs/offline-verification.json').write_text (json .dumps (report ,ensure_ascii =False ,indent =2 ),encoding ='utf-8')
    print (json .dumps (report ,ensure_ascii =False ))


if __name__ =='__main__':main ()
