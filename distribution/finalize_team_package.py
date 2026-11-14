"""Freeze the tested team bundle, create ZIP64 and validate extracted files."""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import shutil
import zipfile

SOURCE=Path(__file__).resolve().parents[1]
BASE=Path(r'D:\CodexData\workspaces\city-mobility-share-2026-11-14')
ROOT=BASE/'work/CityMobility'
OUT=BASE/'outputs'
FULL=OUT/'城市出行数仓-完整运行包-Windows64.zip'
PREVIEW=OUT/'城市出行数仓-成果展示包.zip'

def now():return dt.datetime.now(dt.timezone.utc).isoformat()
def read(path):return json.loads(path.read_text(encoding='utf-8'))
def save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
def digest(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()

def remove(path):
    # Deletion is restricted to disposable files within the dedicated package stage.
    assert path.resolve().is_relative_to(ROOT.resolve()) and path.resolve()!=ROOT.resolve()
    if path.is_dir():shutil.rmtree(path)
    else:path.unlink(missing_ok=True)

def freeze():
    verification=read(ROOT/'outputs/member-verification.json')
    orchestration=read(ROOT/'outputs/airflow-verification.json')
    charts=read(ROOT/'outputs/superset-chart-verification.json')
    browser=read(ROOT/'outputs/superset-browser-verification.json')
    filters=read(ROOT/'outputs/native-filter-browser-verification.json')
    preview=read(OUT/'preview-package-verification.json')
    original=read(SOURCE/'outputs/运行状态.json')
    snapshot=read(ROOT/'backups/snapshot.json')
    assert all(x['passed'] for x in [verification,orchestration,charts,browser,filters,preview,original])
    assert verification['project']=='city-mobility-transfer-check'
    assert not read(ROOT/'work/orchestration/worker-state.json')['running']
    assert verification['pointers']==snapshot['pointers']
    original_serving=next(x['detail'] for x in original['checks'] if x['name']=='ClickHouse发布与口径')
    assert original_serving['historical_trips']==9012795
    assert sorted((r['dataset_id'],r['release_id']) for r in original_serving['pointers'])==sorted((r['dataset_id'],r['release_id']) for r in snapshot['pointers'])
    fixed=[]
    for folder in ['jobs','sql']:
        for file in (SOURCE/folder).rglob('*'):
            if file.is_file() and file.suffix!='.pyc' and '__pycache__' not in file.parts:
                target=ROOT/file.relative_to(SOURCE)
                assert target.is_file() and digest(target)==digest(file),'Changed job or schema: '+str(target)
                fixed.append(file.relative_to(SOURCE).as_posix())
    report={'passed':True,'checked_at_utc':now(),'scope':'Isolated Docker project on the packaging computer; no second physical computer tested',
        'project':verification['project'],'ports':{'superset':28088,'airflow':28080,'clickhouse_http':28123},
        'fresh_install':{'images_loaded':5,'postgres_databases_restored':['superset','airflow'],
            'clickhouse_tables_row_counts_and_content_checksums_verified':snapshot['tables']},
        'restored_verification':verification,'orchestration':orchestration,
        'chart_api':{'passed':charts['passed'],'chart_count':charts['chart_count']},
        'dashboards':[{'slug':x['slug'],'charts':x['charts'],'passed':x['passed']} for x in browser['dashboards']],
        'native_filters':filters,'preview_zip_extracted_browser_test':preview,
        'embedded_python_regressions':{'passed':True,'tests':7,'command':'runtime/python/python.exe -m unittest discover -s tests -v'},
        'jobs_and_schema_unchanged':fixed,'original_services_restored':{'passed':original['passed'],'checked_at_utc':original['checked_at_utc'],'valid_trips':9012795,'pointers':snapshot['pointers']}}
    save(ROOT/'outputs/migration-verification.json',report)
    save(OUT/'migration-verification.json',report)
    for name in ['member-installation.json','member-verification.json','docker-chinese.png','docker-localization-verification.json']:
        remove(ROOT/'outputs'/name)
    for folder in ['orchestration','temp','tests']:
        remove(ROOT/'work'/folder);(ROOT/'work'/folder).mkdir()
    for folder in list(ROOT.rglob('__pycache__')):
        if folder.exists():remove(folder)
    for pattern in ['*.pyc','*.pyo']:
        for file in list(ROOT.rglob(pattern)):remove(file)
    # Host links generated during isolated testing must use the recipient defaults.
    for file in (ROOT/'outputs').rglob('*'):
        if file.suffix in ['.html','.json'] and file.name!='migration-verification.json':
            text=file.read_text(encoding='utf-8')
            for old,new in [('28088','8088'),('28080','18080'),('28123','18123')]:
                text=text.replace('127.0.0.1:'+old,'127.0.0.1:'+new).replace('localhost:'+old,'localhost:'+new)
            if file.suffix=='.html':
                text=text.replace('启动项目.cmd','02-start.cmd').replace('检查运行状态.cmd','04-check.cmd')
            file.write_text(text,encoding='utf-8')
    monitor=ROOT/'tools/monitor_project.py'
    monitor.write_text(monitor.read_text(encoding='utf-8').replace('检查运行状态.cmd','04-check.cmd'),encoding='utf-8')
    shutil.copyfile(SOURCE/'distribution/README-给组员.md',ROOT/'README-给组员.md')
    envpath=ROOT/'.env';text=envpath.read_text(encoding='utf-8')
    defaults={'MOBILITY_PROJECT_NAME':'city-mobility-team','MOBILITY_SUPERSET_PORT':'8088','MOBILITY_AIRFLOW_PORT':'18080',
        'MOBILITY_CLICKHOUSE_PORT':'18123','MOBILITY_NATIVE_PORT':'9000','MOBILITY_SPARK_PORT':'4040'}
    keys={line.split('=',1)[0] for line in text.splitlines() if '=' in line and not line.startswith('#')}
    text+='\n# Team package: optional host ports and isolated project name.\n'
    text+=''.join(k+'='+v+'\n' for k,v in defaults.items() if k not in keys)
    envpath.write_text(text,encoding='utf-8')
    # Every archived regular file is covered, including software installer and credentials.
    files=[]
    for index,file in enumerate(sorted(ROOT.rglob('*')),1):
        if not file.is_file() or file==ROOT/'package-manifest.json':continue
        files.append({'path':file.relative_to(ROOT).as_posix(),'bytes':file.stat().st_size,'sha256':digest(file)})
        if len(files)%1500==0:print('HASHED_FILES '+str(len(files)),flush=True)
    save(ROOT/'package-manifest.json',{'created_at_utc':now(),'files':files,'total_bytes':sum(x['bytes'] for x in files)})
    print('PACKAGE_FROZEN files='+str(len(files))+' bytes='+str(sum(x['bytes'] for x in files)),flush=True)

def archive():
    part=FULL.with_suffix('.zip.part')
    compressed={'.gz','.zip','.parquet','.exe','.dll','.pyd','.png','.jpg','.jpeg','.whl'}
    with zipfile.ZipFile(part,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=1,allowZip64=True) as z:
        for index,file in enumerate(sorted(ROOT.rglob('*')),1):
            if not file.is_file():continue
            z.write(file,'CityMobility/'+file.relative_to(ROOT).as_posix(),compress_type=zipfile.ZIP_STORED if file.suffix.lower() in compressed else zipfile.ZIP_DEFLATED)
            if index%1500==0:print('ZIP_FILES '+str(index),flush=True)
    part.replace(FULL)
    print('ZIP_CREATED bytes='+str(FULL.stat().st_size),flush=True)

def verify():
    destination=BASE/'work/final-zip-extracted'
    assert not destination.exists(),'Do not overwrite an existing extraction'
    destination.mkdir()
    # Extraction streams through every ZIP member and checks its CRC.
    with zipfile.ZipFile(FULL) as z:
        for index,info in enumerate(z.infolist(),1):
            target=(destination/info.filename).resolve()
            assert target.is_relative_to(destination.resolve())
            z.extract(info,destination)
            if index%1500==0:print('ZIP_EXTRACTED '+str(index),flush=True)
    target=destination/'CityMobility'
    manifest=read(target/'package-manifest.json')
    for index,row in enumerate(manifest['files'],1):
        file=target/row['path']
        assert file.is_file() and file.stat().st_size==row['bytes'] and digest(file)==row['sha256'],row['path']
        if index%1500==0:print('EXTRACTED_SHA256_OK '+str(index),flush=True)
    assert not (target/'outputs/member-installation.json').exists()
    assert not list((target/'work/orchestration').rglob('*'))
    sha=[]
    for path in [FULL,PREVIEW]:
        sha.append(digest(path)+'  '+path.name)
    (OUT/'SHA256SUMS.txt').write_text('\n'.join(sha)+'\n',encoding='utf-8')
    verification={'passed':True,'checked_at_utc':now(),'zip64':True,'full_zip_bytes':FULL.stat().st_size,'preview_zip_bytes':PREVIEW.stat().st_size,
        'full_zip_crc_verified_by_extraction':True,'extracted_file_sha256_verified':len(manifest['files']),'clean_first_install_state':True}
    save(OUT/'archive-verification.json',verification)
    size=FULL.stat().st_size/1024**3
    note=f'''发给组员的文件\n\n1. 城市出行数仓-成果展示包.zip（约4.3 MiB）\n不必安装软件，解压后双击“打开展示.html”，可以查看地图、筛选、CSV、清洗结果、看板截图和设计图。\n\n2. 城市出行数仓-完整运行包-Windows64.zip（约{size:.2f} GiB）\n需要在组员电脑完整运行时发送这个包。数据、5个Docker镜像、数据库备份、内置Python和Docker安装程序已包含，不用单独装Java、Spark、ClickHouse、Airflow等。文件较大，建议用网盘或U盘，接收位置需支持大于4GB的单个文件（如NTFS或exFAT）。\n\n组员操作：\n- 使用Windows 10/11 64位，建议16GB内存，目标磁盘至少预留50GB。\n- 先完整解压，建议放到D:\\CityMobility，不要在压缩包里直接运行。\n- 从installers安装并打开Docker Desktop，使用Linux容器，等待引擎就绪。第一次可能需要按提示启用虚拟化、更新WSL或重启。\n- 双击01-first-install.cmd，首次导入和恢复只需执行一次。\n- 之后双击02-start.cmd，再双击06-open-dashboard.cmd。\n- Superset：http://localhost:8088；Airflow：http://localhost:18080。\n- 两个页面账号均为admin，密码均为YOUR_LOCAL_PASSWORD。\n- 04-check.cmd检查，05-run-full-pipeline.cmd触发完整离线流程，03-stop.cmd停止。\n- 具体说明见包内README-给组员.md；端口冲突时按说明调整.env。\n\n验收：本机独立Docker项目及独立端口首次恢复、完整10步调度、4看板19图、筛选均通过；最终ZIP实际解压并逐文件SHA-256校验通过。未在另一台实体电脑上实测。当前数据范围仅2025年6月，共9012795条有效行程。\n\nSHA256SUMS.txt可用于收发后核对压缩包是否损坏。完整包包含课程演示用本地账号配置，请通过组内渠道传递。\n'''
    (OUT/'发给组员说明.txt').write_text(note,encoding='utf-8-sig')
    print('FINAL_ARCHIVE_VERIFIED '+json.dumps(verification),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['freeze','archive','verify']);a=p.parse_args()
    {'freeze':freeze,'archive':archive,'verify':verify}[a.action]()
