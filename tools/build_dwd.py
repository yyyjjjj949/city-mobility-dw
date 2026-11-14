"""Run the full June historical DWD job and export an offline inspection report."""
import argparse
import csv
import datetime as dt
import hashlib
import html
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid
from compute_lock import exclusive_compute

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / 'work/dwd'
DOCKER = r'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    tmp.replace(path)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def committed_version(name):
    versions = list((ROOT / 'data' / name.replace('.', '/') / '_delta_log').glob('[0-9]' * 20 + '.json'))
    if not versions:
        raise ValueError('Missing Delta table: ' + name)
    return max(int(p.stem) for p in versions)


def export(report, run):
    output = ROOT / 'outputs/dwd'
    output.mkdir(parents=True, exist_ok=True)
    daily = []
    warnings = []
    warning_names = {'Q007': '距离不适用于均值', 'Q008': '至少一端未匹配纽约五行政区', 'NEGATIVE_AMOUNT': '存在负费用，保留原值', 'PASSENGER_MISSING_OR_ZERO': '乘客数缺失或非正', 'EXTREME_TOTAL_AMOUNT': '总费用绝对值超过1000美元', 'AMOUNT_PARSE_OR_OVERFLOW': '费用解析失败或超出数值范围', 'SPATIAL_MULTIPLE_MATCHES': '边界多重匹配，取最小区域编码', 'STATION_MISSING': '至少一端未提供站点编号'}
    for source, stats in report['sources'].items():
        for row in stats['profile']['daily']:
            daily.append({'source_id': source, **row})
        for code, count in sorted(stats['profile']['quality_flag_counts'].items()):
            warnings.append({'source_id': source, 'rule': code, 'description': warning_names.get(code, code), 'rows': count})
        preview = json.loads((run / (source + '-preview.json')).read_text(encoding='utf-8'))
        if preview:
            with (output / (source + '-前20条.csv')).open('w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=list(preview[0])); writer.writeheader(); writer.writerows(preview)
    with (output / '六月每日有效行程.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['source_id', 'service_date', 'trips', 'average_duration_seconds']); writer.writeheader(); writer.writerows(daily)
    with (output / '六月质量警告.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['source_id', 'rule', 'description', 'rows']); writer.writeheader(); writer.writerows(warnings)
    write_json(output / '清洗结果.json', report)
    geometry = WORK / 'zones.geojson'
    if geometry.exists():
        shutil.copyfile(geometry, output / '纽约区域边界.geojson')
    rows = ''
    labels = {'tlc_yellow': '黄色出租车', 'citibike_trips': '共享单车'}
    for source, r in report['sources'].items():
        cells = [labels[source]] + [f'{r[c]:,}' for c in ['input_rows', 'accepted_rows', 'quarantined_rows', 'duplicate_skipped_rows', 'out_of_scope_rows']]
        rows += '<tr>' + ''.join('<td>' + html.escape(c) + '</td>' for c in cells) + '</tr>'
    dimension_rows = ''.join('<tr><td>' + html.escape(n) + '</td><td>' + f'{count:,}' + '</td></tr>' for n, count in report['dimensions'].items())
    daily_rows = ''.join('<tr>' + ''.join('<td>' + html.escape(str(v)) + '</td>' for v in [labels[d['source_id']], d['service_date'], f"{d['trips']:,}", f"{d['average_duration_seconds'] / 60:.2f}"]) + '</tr>' for d in daily)
    warning_rows = ''.join('<tr>' + ''.join('<td>' + html.escape(str(v)) + '</td>' for v in [labels[d['source_id']], d['description'], f"{d['rows']:,}"]) + '</tr>' for d in warnings)
    coverage_rows = ''
    for source, result in report['sources'].items():
        profile = result['profile']
        coverage_rows += '<tr>' + ''.join('<td>' + html.escape(v) + '</td>' for v in [labels[source], f"{profile['origin_mapped_rows'] / profile['rows']:.2%}", f"{profile['destination_mapped_rows'] / profile['rows']:.2%}", f"{profile['both_mapped_rows'] / profile['rows']:.2%}"]) + '</tr>'
    content = f'''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>六月 DWD 数据清洗结果</title>
<style>body{{font-family:"Microsoft YaHei",sans-serif;max-width:1120px;margin:40px auto;padding:0 24px;color:#203143;background:#f5f7fa}}h1{{font-size:30px}}section{{background:white;padding:24px;margin:20px 0;border-radius:12px}}table{{width:100%;border-collapse:collapse}}th,td{{padding:10px;text-align:left;border-bottom:1px solid #e6ebf0}}th{{background:#eaf3f8}}.big{{font-size:28px;color:#076578}}a{{color:#03688b}}p{{line-height:1.8}}small{{color:#5f6d78}}</style>
<h1>2025 年 6 月 · 城市出行数据清洗</h1><p>完整官方月度数据 · DWD 历史明细层 · 已通过质量检查</p>
<section><span class="big">{report['total_valid_trips']:,}</span> 条有效行程，<span class="big">{report['total_quarantined_rows']:,}</span> 条异常记录已隔离。
<p>时间和时长异常保留在隔离表；空间未匹配、距离或费用异常有单独质量标记。下表四类去向互斥，其和等于输入量。</p>
<table><tr><th>方式</th><th>输入量</th><th>有效行程</th><th>隔离</th><th>重复跳过</th><th>范围外</th></tr>{rows}</table></section>
<section><h2>查看数据</h2><p><a href="tlc_yellow-前20条.csv">出租车前 20 条明细</a>　<a href="citibike_trips-前20条.csv">单车前 20 条明细</a>　<a href="六月每日有效行程.csv">完整 30 天日统计</a>　<a href="清洗结果.json">完整清洗及质量报告</a>　<a href="纽约区域边界.geojson">官方区域边界</a></p>
<p><a href="六月质量警告.csv">导出字段质量警告</a></p><p>CSV 可用 Excel 或 WPS 打开。20 条明细仅供快速查看，实际加工使用全部原始记录。完整事实和维度保存在项目 data/dwd 的 Delta 表中。</p></section>
<section><h2>保留行程中的字段警告</h2><p>同一行程可能有多个警告，下列行数不能直接相加。负费用可能涉及退款或冲正，保留来源数值，不能仅凭此判定为错误。</p><table><tr><th>方式</th><th>警告</th><th>行程数</th></tr>{warning_rows}</table></section>
<section><h2>纽约区域匹配覆盖率</h2><p>分母为各方式全部有效行程。区域未匹配的行程仍参与时间趋势；OD 等空间指标使用满足对应空间条件的记录。</p><table><tr><th>方式</th><th>起点在纽约</th><th>终点在纽约</th><th>两端均在纽约</th></tr>{coverage_rows}</table><p>六月 30 天天气完整性检查：{'通过' if report['weather_complete'] else '未通过，天气主题不得发布'}。</p></section>
<section><h2>已生成维度</h2><table><tr><th>维度表</th><th>行数（含未知成员）</th></tr>{dimension_rows}</table></section>
<section><h2>每日有效行程</h2><p>平均时长单位为分钟，含空间尚未匹配但时间有效的记录。</p><table><tr><th>方式</th><th>纽约日期</th><th>行程数</th><th>平均时长</th></tr>{daily_rows}</table></section>
<section><h2>当前进度</h2><p>离线 DWD 历史维度、行程清洗及质量隔离完成；DWS、ADS 与历史分析看板已接通。尚未采集五月边界记录，完整到达量和净流入暂不发布。</p><small>运行 ID：{report['run_id']} · ODS 版本：{report['ods_version']} · 规则版本：{report['rule_version']} · 完成时间 UTC：{report['finished_at_utc']}</small></section></html>'''
    (output / '六月数据清洗结果.html').write_text(content, encoding='utf-8')


def execute(args):
    latest = json.loads((ROOT / 'outputs/ods-ingestion-latest.json').read_text(encoding='utf-8'))
    verified = json.loads((ROOT / 'outputs/ods-verification.json').read_text(encoding='utf-8'))
    if latest['month'] != args.month or not verified['passed']:
        raise ValueError('A verified full ODS month is required')
    from ods_snapshot import verified_month_version
    ods_version = verified_month_version(ROOT, latest, verified)
    # New ODS verification can include metadata/other-source commits. Reuse the
    # older input version only when the exact monthly file actions still match.
    prior_path = ROOT/'outputs/dwd-latest.json'
    if prior_path.exists():
        old=json.loads(prior_path.read_text(encoding='utf-8'))
        from ods_snapshot import monthly_files
        if old.get('month')==args.month and monthly_files(ROOT,old['ods_version'],args.month)==monthly_files(ROOT,ods_version,args.month):
            ods_version=old['ods_version']
    policy = json.loads((ROOT / 'config/model/policies.json').read_text(encoding='utf-8'))
    mapping = json.loads((ROOT / 'config/model/source-field-mapping.json').read_text(encoding='utf-8'))
    objects = latest['objects']
    expected = {o['source_id']: o['prepared']['rows'] for o in objects}
    assert sum(expected.values()) == verified['total_ods_rows']
    source_identity = {o['source_id']: o['sha256'] for o in objects}
    code = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in ['jobs/build_dwd.py', 'jobs/dwd_spatial.py', 'config/model/schema-contract.json']}
    digest = fingerprint({'ods_version': ods_version, 'objects': source_identity, 'policies': policy, 'mapping': mapping, 'code': code})
    previous = ROOT / 'outputs/dwd-latest.json'
    if previous.exists() and not args.force:
        prior = json.loads(previous.read_text(encoding='utf-8'))
        if prior.get('passed') and prior.get('input_fingerprint') == digest:
            if all(committed_version(n) == v for n, v in prior['output_versions'].items() if n.startswith('dwd.')):
                print('DWD_REUSED validated build=' + prior['run_id'] + '; output Delta versions unchanged', flush=True)
                export(prior, WORK / 'runs' / prior['run_id'])
                write_json(ROOT / 'outputs/dwd-rerun-verification.json', {'passed': True, 'run_id': prior['run_id'], 'input_fingerprint': digest, 'all_output_versions_unchanged': True, 'checked_at_utc': dt.datetime.now(dt.timezone.utc).isoformat()})
                return
    run_id = uuid.uuid4().hex
    run = WORK / 'runs' / run_id
    run.mkdir(parents=True)
    manifest = dict(run_id=run_id, month=args.month, ods_version=ods_version, expected_rows=expected, source_identity=source_identity,
                    policies=policy, field_mapping=mapping, code_sha256=code, input_fingerprint=digest,
                    geometry=next(o for o in objects if o['source_id'] == 'taxi_zone_geometry'))
    if args.resume_run:
        if len(args.resume_run) != 32 or any(c not in '0123456789abcdef' for c in args.resume_run):
            raise ValueError('Invalid resume run ID')
        previous_run = WORK / 'runs' / args.resume_run
        old_manifest = json.loads((previous_run / 'manifest.json').read_text(encoding='utf-8'))
        old_report = json.loads((previous_run / 'result.json').read_text(encoding='utf-8'))
        for field in ['month', 'ods_version', 'source_identity', 'policies', 'field_mapping']:
            if old_manifest[field] != manifest[field]:
                raise ValueError('Resume inputs differ: ' + field)
        if old_manifest['code_sha256']['config/model/schema-contract.json'] != code['config/model/schema-contract.json']:
            raise ValueError('Resume schema differs')
        sources = old_report['sources']
        if not sources:
            raise ValueError('No completed source is available to resume')
        for sid, source in sources.items():
            name = {'tlc_yellow': 'dwd.fact_taxi_trip', 'citibike_trips': 'dwd.fact_bike_trip'}[sid]
            assert source['input_rows'] == expected[sid]
            assert committed_version(name) == old_report['output_versions'][name]
            shutil.copyfile(previous_run / (sid + '-preview.json'), run / (sid + '-preview.json'))
        manifest.update(resumed_from_run=args.resume_run, resume_sources=sources, resume_output_versions=old_report['output_versions'],
                        resume_quality=[q for q in old_report['quality'] if q['batch_id'] in {s['batch_id'] for s in sources.values()}],
                        prior_code_sha256=old_manifest['code_sha256'])
    path = run / 'manifest.json'
    write_json(path, manifest)
    shutil.copyfile(ROOT / 'config/model/schema-contract.json', WORK / 'schema-contract.json')
    shutil.copyfile(ROOT / 'sql/spark/001_create_lakehouse.sql', WORK / 'lake.sql')
    os.environ['DOCKER_CONFIG'] = r'D:\CodexData\app-data\docker-profile\cli'
    command = [DOCKER, 'compose', '-f', str(ROOT / 'compose.yaml'), 'exec', '-T',
               '-e', 'PYTHONPATH=/workspace/work/dwd/python-deps-np126:/workspace/jobs:/opt/spark/python', 'spark',
               '/opt/spark/bin/spark-submit', '--driver-memory', '2g',
               '/workspace/jobs/build_dwd.py', '--manifest', '/workspace/' + path.relative_to(ROOT).as_posix()]
    print('DWD_RUN_START ' + run_id + ' log=' + str(run / 'spark.log'), flush=True)
    container = subprocess.check_output([DOCKER, 'compose', '-f', str(ROOT / 'compose.yaml'), 'ps', '-q', 'spark'], cwd=ROOT, text=True).strip()
    limits = json.loads(subprocess.check_output([DOCKER, 'inspect', container], text=True))[0]['HostConfig']
    # Only the active batch gets extra memory. The normal 2.5 GiB service cap is restored.
    subprocess.run([DOCKER, 'update', '--memory', '4g', '--memory-swap', '4g', container], check=True, capture_output=True)
    try:
        with (run / 'spark.log').open('w', encoding='utf-8') as f:
            result = subprocess.run(command, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT)
    finally:
        subprocess.run([DOCKER, 'update', '--memory', str(limits['Memory']), '--memory-swap', str(limits['MemorySwap']), container], check=True, capture_output=True)
    if result.returncode:
        recovery_command = [DOCKER, 'compose', '-f', str(ROOT / 'compose.yaml'), 'exec', '-T', 'spark',
                            '/opt/spark/bin/spark-submit', '/workspace/jobs/recover_interrupted_dwd_batch.py', '--run-id', run_id]
        with (run / 'recovery.log').open('w', encoding='utf-8') as f:
            recovery = subprocess.run(recovery_command, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT)
        if recovery.returncode:
            print('DWD batch recovery also needs inspection: ' + str(run / 'recovery.log'), flush=True)
        raise RuntimeError('DWD failed. Inspect ' + str(run / 'spark.log'))
    report = json.loads((run / 'result.json').read_text(encoding='utf-8'))
    assert report['passed'] and report['status'] == 'validated'
    write_json(previous, report)
    export(report, run)
    print('DWD_RUN_OK ' + json.dumps({'trips': report['total_valid_trips'], 'quarantined': report['total_quarantined_rows'], 'preview': str(ROOT / 'outputs/dwd/六月数据清洗结果.html')}, ensure_ascii=False), flush=True)


@exclusive_compute
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--month', choices=['2025-06'], default='2025-06')
    parser.add_argument('--force', action='store_true', help='Rebuild validated outputs for a same-input replay audit')
    parser.add_argument('--resume-run', help='Reuse completed sources after operational recovery; inputs/schema must match and business transformations must be unchanged')
    args = parser.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    lock = WORK / 'writer.lock'
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    try:
        os.write(fd, json.dumps({'pid': os.getpid(), 'started_at_utc': dt.datetime.now(dt.timezone.utc).isoformat()}).encode()); os.close(fd)
        execute(args)
    finally:
        lock.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
