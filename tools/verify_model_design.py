"""Verify the documented dimensional design against source contracts and jobs.

No database, network, or data rebuild is required. Run from any working directory:
    python tools/verify_model_design.py --output-dir outputs/2026-11-18-theme-definition-e721eb90/model

The output directory is resolved relative to the repository unless absolute.
"""
from __future__ import annotations
import argparse
import ast
import datetime as dt
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = 'outputs/2026-11-18-theme-definition-e721eb90/model'
FACT_KEYS = {'dwd.fact_taxi_trip': 'trip_key', 'dwd.fact_bike_trip': 'ride_id'}
EXPECTED_DIMS = {
    'dwd.dim_date', 'dwd.dim_time', 'dwd.dim_zone', 'dwd.dim_station',
    'dwd.dim_transport', 'dwd.dim_bike_type', 'dwd.dim_weather_daily',
}
DOCS = [
    '14-Kimball案例借鉴与事实表设计.md',
    '15-业务总线矩阵与概念逻辑物理模型.md',
    '16-分层职责与指标口径复核.md',
    '17-数仓开发生命周期与追溯示例.md',
]
DIAGRAMS = ['01-概念主题结构', '02-逻辑星座模型', '03-分层与维度模型对应']


def source(path: str) -> str:
    return (ROOT / path).read_text(encoding='utf-8-sig')


def call_strings(tree: ast.AST, name: str):
    """Return arguments from actual Python calls, ignoring comments/docstrings."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if ((isinstance(func, ast.Attribute) and func.attr == name)
                or (isinstance(func, ast.Name) and func.id == name)):
            yield node.args


def constant(node, value):
    return isinstance(node, ast.Constant) and node.value == value


def has_literal_column(tree, column, value):
    for args in call_strings(tree, 'withColumn'):
        if len(args) < 2 or not constant(args[0], column):
            continue
        expr = args[1]
        # F.lit(value) or F.lit(value).cast(type)
        if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute) and expr.func.attr == 'cast':
            expr = expr.func.value
        if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute) and expr.func.attr == 'lit':
            if expr.args and constant(expr.args[0], value):
                return True
    return False


def verify(output: Path) -> dict:
    contract = json.loads(source('config/model/schema-contract.json'))
    tables = {t['name']: t for t in contract['tables']}
    checks = []

    def check(name, passed, evidence):
        checks.append({'check': name, 'passed': bool(passed), 'evidence': evidence})

    check('seven_public_or_specialized_dimensions',
          {n for n in tables if n.startswith('dwd.dim_')} == EXPECTED_DIMS,
          sorted(n for n in tables if n.startswith('dwd.dim_')))
    counts = {}
    for fact, pk in FACT_KEYS.items():
        table = tables[fact]
        counts[fact] = len(table['columns'])
        check(fact + '_key_and_partition', table['logical_key'] == [pk]
              and table['partition_by'] == ['service_date'],
              {'logical_key': table['logical_key'], 'partition': table['partition_by'],
               'columns': counts[fact], 'grain': table['grain']})

    references = []
    for table in tables.values():
        if not table['name'].startswith('dwd.'):
            continue
        for column in table['columns']:
            target = column.get('reference')
            if not target:
                continue
            name, key = target.rsplit('.', 1)
            destination = tables.get(name)
            target_columns = {c['name']: c for c in destination['columns']} if destination else {}
            matches = key in target_columns and target_columns[key]['type'] == column['type']
            references.append({'table': table['name'], 'column': column['name'],
                               'target': target, 'matching_type': matches})
    check('dwd_references_resolve_with_matching_types', bool(references)
          and all(r['matching_type'] for r in references), {'references': len(references)})

    roles = [('start_date_key', 'end_date_key'), ('start_time_key', 'end_time_key'),
             ('origin_zone_key', 'destination_zone_key')]
    for fact in FACT_KEYS:
        cols = {c['name']: c for c in tables[fact]['columns']}
        same_target = all(cols.get(a, {}).get('reference')
                          and cols[a]['reference'] == cols.get(b, {}).get('reference') for a, b in roles)
        check(fact + '_role_playing', same_target, roles)
    bike = {c['name']: c for c in tables['dwd.fact_bike_trip']['columns']}
    check('bike_station_role_playing', bike['origin_station_key']['reference']
          == bike['destination_station_key']['reference'] == 'dwd.dim_station.station_key',
          'origin_station_key / destination_station_key -> dim_station.station_key')
    check('five_public_aggregates',
          {n for n in tables if n.startswith('dws.')} == {
              'dws.trip_day', 'dws.trip_hour', 'dws.zone_hour', 'dws.od_day', 'dws.weather_group'},
          sorted(n for n in tables if n.startswith('dws.')))
    parts = {'duration_sum_seconds', 'duration_count', 'distance_sum_km', 'distance_count'}
    check('day_and_hour_retain_weighted_mean_parts', all(parts.issubset(
          {c['name'] for c in tables[name]['columns']}) for name in ['dws.trip_day', 'dws.trip_hour']),
          sorted(parts))
    weather = tables['dwd.dim_weather_daily']
    check('weather_has_unique_context_key_and_group', weather['logical_key'] == ['weather_key']
          and {'observation_date', 'weather_station_id', 'precipitation_group', 'weather_usable'}.issubset(
              {c['name'] for c in weather['columns']}), weather['grain'])

    marts = ast.parse(source('jobs/build_marts.py'))
    union_calls = list(call_strings(marts, 'unionByName'))
    check('shared_aggregation_uses_union', bool(union_calls), 'AST of jobs/build_marts.py: unionByName')
    check('incomplete_arrivals_remain_unavailable',
          has_literal_column(marts, 'arrival_count', None)
          and has_literal_column(marts, 'net_inflow', None)
          and has_literal_column(marts, 'arrival_complete', False),
          'AST of jobs/build_marts.py: unavailable arrivals/net inflow and false completeness')
    dwd = ast.parse(source('jobs/build_dwd.py'))
    unique_function = [n for n in ast.walk(dwd)
                       if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == 'check_unique']
    check('dwd_logical_key_validation_exists', bool(unique_function)
          and bool(list(call_strings(unique_function[0], 'distinct'))),
          'AST of jobs/build_dwd.py: check_unique contains distinct check')

    missing_links = []
    for name in DOCS:
        file = ROOT / 'docs' / name
        if not file.is_file():
            missing_links.append('docs/' + name)
            continue
        for link in re.findall(r'\]\(([^)]+)\)', file.read_text(encoding='utf-8')):
            if '://' in link or link.startswith('#'):
                continue
            if not (file.parent / link.split('#', 1)[0]).is_file():
                missing_links.append(name + ': ' + link)
    check('design_documents_and_local_links_exist', not missing_links, missing_links)
    missing_diagrams = [name + ext for name in DIAGRAMS for ext in ['.svg', '.png']
                        if not (output / (name + ext)).is_file()]
    check('model_diagrams_present', not missing_diagrams, missing_diagrams)
    manifest = output / 'reference-manifest.json'
    try:
        sources = json.loads(manifest.read_text(encoding='utf-8'))['sources']
        source_ok = len(sources) == 6 and all(r['status'] == 200 and r['url'].startswith('https://')
                  and re.fullmatch('[a-f0-9]{64}', r['sha256']) for r in sources)
    except (OSError, ValueError, KeyError, TypeError):
        sources, source_ok = [], False
    check('primary_method_reference_receipts', source_ok,
          'reference-manifest.json: six successful source receipts, not a new network validation')
    return {
        'passed': all(c['passed'] for c in checks),
        'checked_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'scope': 'model contract, job AST, role keys, types and document artifact consistency; not live-data validation',
        'logical_layer_decision': 'DIM responsibilities separated; physical dwd.dim_* retained by ADR-001',
        'checks': checks, 'references_checked': references, 'fact_columns': counts,
        'fingerprints': {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in [
            'config/model/schema-contract.json', 'config/model/policies.json',
            'jobs/build_dwd.py', 'jobs/build_marts.py']},
        'source_evidence': 'reference-manifest.json',
        'reproduce_command': 'python tools/verify_model_design.py --output-dir ' + DEFAULT_OUTPUT,
        'known_limits': ['only verified month 2025-06',
                         'no complete arrivals/net inflow without May boundary',
                         'no legal holiday calendar', 'single-station daily weather context only',
                         'full textbook and course slides were not supplied'],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT,
                        help='Directory containing the published model diagrams and reference manifest.')
    args = parser.parse_args()
    folder = Path(args.output_dir)
    if not folder.is_absolute():
        folder = ROOT / folder
    folder = folder.resolve()
    folder.mkdir(parents=True, exist_ok=True)
    report = verify(folder)
    (folder / 'model-design-review.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'passed': report['passed'], 'checks': len(report['checks']),
                      'references': len(report['references_checked'])}, ensure_ascii=False))
    if not report['passed']:
        for check in report['checks']:
            if not check['passed']:
                print('FAILED: ' + check['check'] + ': ' + str(check['evidence']))
        raise SystemExit(1)


if __name__ == '__main__':
    main()
