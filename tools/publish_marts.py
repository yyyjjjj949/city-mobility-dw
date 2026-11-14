"""Verify full candidate content before one atomic ClickHouse pointer exchange."""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]
TABLES = ['trip_day', 'trip_hour', 'zone_hour', 'od_day', 'weather_group', 'quality_batch', 'quality_rule']


class ClickHouse:
    def __init__(self):
        values = {}
        for line in (ROOT / '.env').read_text(encoding='utf-8').splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                k, v = line.split('=', 1); values[k] = v.strip().strip('"').strip("'")
        self.session = requests.Session(); self.session.trust_env = False
        self.session.auth = ('dw_admin', values['CLICKHOUSE_PASSWORD'])
        self.url = 'http://127.0.0.1:18123/'

    def request(self, sql, data=None, stream=False):
        r = self.session.post(self.url, params={'query': sql, 'output_format_json_quote_64bit_integers': '0', 'date_time_input_format': 'best_effort',
            'input_format_parallel_parsing': '0', 'max_insert_threads': '1', 'max_threads': '2', 'max_insert_block_size': '8192'}, data=data, timeout=(10, 180), stream=stream)
        if not r.ok: raise RuntimeError('ClickHouse: ' + r.text[:1500])
        return r

    def query(self, sql):
        return self.request(sql + ' FORMAT JSON').json()['data']


def columns(name):
    return next(t for t in json.loads((ROOT / 'config/model/schema-contract.json').read_text(encoding='utf-8'))['tables'] if t['name'] == 'ads.' + name)


def normal(row, table):
    values = []
    for c in table['columns']:
        value = row[c['name']]
        if value is not None:
            if c['type'] == 'DOUBLE': value = format(float(value), '.12g')
            elif c['type'] in ['INT', 'BIGINT']: value = int(value)
            elif c['type'] == 'BOOLEAN': value = bool(value)
            elif c['type'] == 'TIMESTAMP':
                value = dt.datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S.%f')
        values.append(value)
    return json.dumps(values, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()


def checksum(rows, table):
    # Full-row order-independent multiset hash: count + sum + xor of SHA-256.
    count = total = xor = 0; modulus = 1 << 256
    for row in rows:
        h = int.from_bytes(hashlib.sha256(normal(row, table)).digest(), 'big')
        count += 1; total = (total + h) % modulus; xor ^= h
    return {'rows': count, 'sha256_sum': f'{total:064x}', 'sha256_xor': f'{xor:064x}'}


def export_rows(folder):
    for path in sorted(folder.glob('part-*.json')):
        with path.open(encoding='utf-8') as f:
            for line in f:
                if line.strip(): yield json.loads(line)


def file_chunks(path):
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''): yield block


def verify_active(result):
    ch = ClickHouse(); release = result['release_id']
    pointers = {x['dataset_id']: x['release_id'] for x in ch.query('SELECT dataset_id,release_id FROM ads.release_pointer')}
    assert pointers.get('historical') == release and pointers.get('quality') == release
    for name in TABLES:
        count = int(ch.query(f'SELECT count() AS n FROM ads.bi_{name}')[0]['n'])
        assert count == result['counts']['ads.' + name], (name, count)


def publish(run, result):
    lock = ROOT / 'work/marts/publication.lock'; fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    try:
        os.write(fd, str(os.getpid()).encode()); os.close(fd)
        return publish_locked(run, result)
    finally: lock.unlink(missing_ok=True)


def publish_locked(run, result):
    ch = ClickHouse(); release = result['release_id']
    assert len(release) == 32 and all(c in '0123456789abcdef' for c in release)
    import re
    for sql in re.sub(r'^--.*$', '', (ROOT / 'sql/clickhouse/001_create_serving.sql').read_text(encoding='utf-8'), flags=re.M).split(';'):
        if sql.strip(): ch.request(sql)
    old = ch.query('SELECT dataset_id,release_id,activated_at_utc FROM ads.release_pointer')
    assert len({r['dataset_id'] for r in old}) == len(old)
    if any(r['release_id'] == release for r in old):
        verify_active(result)
        saved = run / 'publication.json'
        assert saved.exists(), 'Active publication requires its verification receipt'
        return json.loads(saved.read_text(encoding='utf-8'))
    report = {'release_id': release, 'previous_pointers': old, 'status': 'loading', 'tables': {}}
    def save():
        p = run / 'publication.tmp'; p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8'); p.replace(run / 'publication.json')
    save()
    for name in TABLES:
        table = columns(name)
        actual = ch.query('DESCRIBE TABLE ads.' + name)
        assert [x['name'] for x in actual] == [x['name'] for x in table['columns']], 'Serving schema drift: ' + name
        # An interrupted candidate may be retried; remove only that inactive candidate.
        ch.request(f"ALTER TABLE ads.{name} DELETE WHERE release_id='{release}' SETTINGS mutations_sync=2")
        expected = checksum(export_rows(run / 'export' / name), table)
        assert expected['rows'] == result['counts']['ads.' + name]
        for path in sorted((run / 'export' / name).glob('part-*.json')):
            ch.request(f'INSERT INTO ads.{name} FORMAT JSONEachRow', data=file_chunks(path))
        key = ','.join('`' + c + '`' for c in table['logical_key'])
        stats = ch.query(f"SELECT count() AS n,uniqExact(tuple({key})) AS unique_n FROM ads.{name} WHERE release_id='{release}'")[0]
        assert int(stats['n']) == int(stats['unique_n']) == expected['rows'], name
        resp = ch.request(f"SELECT * FROM ads.{name} WHERE release_id='{release}' FORMAT JSONEachRow", stream=True)
        try: actual_hash = checksum((json.loads(line) for line in resp.iter_lines() if line), table)
        finally: resp.close()
        assert actual_hash == expected, (name, actual_hash, expected)
        report['tables'][name] = {'passed': True, 'logical_keys_unique': True, 'full_content_checksum': actual_hash}
        save(); print('SERVING_CANDIDATE_OK ' + name + ' rows=' + str(expected['rows']), flush=True)
    zone_check = ch.query(f"SELECT countIf(arrival_count IS NOT NULL OR net_inflow IS NOT NULL OR arrival_complete) AS invalid FROM ads.zone_hour WHERE release_id='{release}'")[0]
    assert int(zone_check['invalid']) == 0
    totals = ch.query(f"SELECT transport_code,sum(trip_count) AS n FROM ads.trip_day WHERE release_id='{release}' GROUP BY transport_code")
    manifest = json.loads((run / 'manifest.json').read_text(encoding='utf-8'))
    assert {r['transport_code']: int(r['n']) for r in totals} == {'taxi_yellow': manifest['dwd']['sources']['tlc_yellow']['accepted_rows'], 'citibike': manifest['dwd']['sources']['citibike_trips']['accepted_rows']}
    # The offline release activates only historical and quality datasets.
    activated = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    pointer = [{'dataset_id': d, 'release_id': release, 'activated_at_utc': activated} for d in ['historical', 'quality']]
    ch.request('CREATE TABLE IF NOT EXISTS ads.release_pointer_next AS ads.release_pointer')
    ch.request('TRUNCATE TABLE ads.release_pointer_next')
    ch.request('INSERT INTO ads.release_pointer_next FORMAT JSONEachRow', data='\n'.join(json.dumps(r) for r in pointer).encode())
    assert ch.query('SELECT count() AS n FROM ads.release_pointer_next')[0]['n'] == len(pointer)
    report.update(status='validated', candidate_validated_at_utc=dt.datetime.now(dt.timezone.utc).isoformat()); save()
    ch.request('EXCHANGE TABLES ads.release_pointer AND ads.release_pointer_next')
    try:
        verify_active(result)
    except Exception:
        ch.request('EXCHANGE TABLES ads.release_pointer AND ads.release_pointer_next')
        report.update(status='rolled_back'); save(); raise
    report.update(status='active', passed=True, activated_at_utc=activated, Q017=True, atomic_pointer_exchange=True, all_candidate_rows_hashed=True)
    save()
    (ROOT / 'outputs/marts-publication-verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return report
