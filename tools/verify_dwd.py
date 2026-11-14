"""Independent audit of committed Delta files and traced ODS samples, without Spark."""
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
from urllib.parse import unquote
from zoneinfo import ZoneInfo
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]


def committed(name, expected_version):
    path = ROOT / 'data' / name.replace('.', '/')
    active = {}
    versions = []
    for log in sorted((path / '_delta_log').glob('[0-9]' * 20 + '.json')):
        version = int(log.stem)
        if version > expected_version:
            break  # Audit the exact validated snapshot, including shared tables.
        versions.append(version)
        for line in log.read_text(encoding='utf-8').splitlines():
            action = json.loads(line)
            if 'add' in action:
                active[action['add']['path']] = action['add']
            if 'remove' in action:
                active.pop(action['remove']['path'], None)
    assert max(versions) == expected_version, name
    for later in sorted((path/'_delta_log').glob('[0-9]'*20+'.json')):
        if int(later.stem)<=expected_version:continue
        if name.startswith('dwd.'):
            assert not any('add' in json.loads(line) or 'remove' in json.loads(line) for line in later.read_text(encoding='utf-8').splitlines()), 'Business table changed since validated DWD: '+name
    return path, active


def main():
    report = json.loads((ROOT / 'outputs/dwd-latest.json').read_text(encoding='utf-8'))
    assert report['passed'] and report['status'] == 'validated'
    counts, samples = {}, {}
    files = 0
    ny = ZoneInfo('America/New_York')
    for name, version in report['output_versions'].items():
        base, active = committed(name, version)
        count = 0
        for relative, add in active.items():
            file = base / unquote(relative)
            parquet = pq.ParquetFile(file)
            n = parquet.metadata.num_rows
            assert json.loads(add['stats'])['numRecords'] == n
            assert file.stat().st_size == add['size']
            files += 1
            count += n
            if name not in ['dwd.fact_taxi_trip', 'dwd.fact_bike_trip']:
                continue
            source = 'tlc_yellow' if name.endswith('taxi_trip') else 'citibike_trips'
            maximum = 86400 if source == 'tlc_yellow' else 259200
            for batch in parquet.iter_batches(batch_size=65536, columns=['duration_seconds', 'duration_metric_eligible', 'started_at_utc', 'ended_at_utc']):
                duration = batch.column('duration_seconds')
                assert pc.all(pc.and_(pc.greater(duration, 0), pc.less_equal(duration, maximum))).as_py()
                assert pc.all(batch.column('duration_metric_eligible')).as_py()
                elapsed = pc.divide(pc.cast(pc.cast(pc.subtract(pc.cast(batch.column('ended_at_utc'), pa.timestamp('us')), pc.cast(batch.column('started_at_utc'), pa.timestamp('us'))), pa.int64()), pa.float64()), 1000000)
                assert pc.all(pc.less_equal(pc.abs(pc.subtract(elapsed, duration)), 1e-6)).as_py()
            first = next(parquet.iter_batches(batch_size=2))
            for row in first.to_pylist():
                assert row['duration_metric_eligible']
                for prefix in ['started', 'ended']:
                    raw = dt.datetime.fromisoformat(row[prefix + '_at_raw'])
                    assert raw == row[prefix + '_at_local']
                    stamp = row[prefix + '_at_utc']
                    if stamp.tzinfo is None:
                        stamp = stamp.replace(tzinfo=dt.timezone.utc)
                    assert stamp == raw.replace(tzinfo=ny).astimezone(dt.timezone.utc)
                assert add['partitionValues']['service_date'] == row['started_at_local'].date().isoformat()
                assert row['start_date_key'] == int(row['started_at_local'].strftime('%Y%m%d'))
                assert row['end_date_key'] == int(row['ended_at_local'].strftime('%Y%m%d'))
                if source == 'tlc_yellow':
                    assert row['trip_key'] == row['source_record_key']
                    assert math.isclose(row['distance_km'], row['distance_miles'] * 1.609344) if row['distance_miles'] is not None else row['distance_km'] is None
                samples[row['source_record_key']] = (source, row)
        counts[name] = count
    for name, count in report['dimensions'].items():
        assert counts[name] == count
    assert counts['dwd.fact_taxi_trip'] == report['sources']['tlc_yellow']['accepted_rows']
    assert counts['dwd.fact_bike_trip'] == report['sources']['citibike_trips']['accepted_rows']
    for source, stats in report['sources'].items():
        assert stats['input_rows'] == sum(stats[c] for c in ['accepted_rows', 'quarantined_rows', 'duplicate_skipped_rows', 'out_of_scope_rows'])
        assert stats['quarantined_rows'] / stats['input_rows'] <= .05
    base, active = committed('ods.source_record', report['ods_version'])
    remaining = set(samples)
    source_checks = 0
    for relative, add in active.items():
        if add['partitionValues']['source_id'] not in ['tlc_yellow', 'citibike_trips']:
            continue
        for batch in pq.ParquetFile(base / unquote(relative)).iter_batches(batch_size=32768, columns=['record_key', 'payload_json', 'payload_sha256']):
            mask = pc.is_in(batch.column('record_key'), value_set=pa.array(list(remaining), type=pa.string()))
            for original in pa.Table.from_batches([batch]).filter(mask).to_pylist():
                source, row = samples[original['record_key']]
                assert hashlib.sha256(original['payload_json'].encode()).hexdigest() == row['content_fingerprint'] == original['payload_sha256']
                p = json.loads(original['payload_json'])
                start_field, end_field = ('started_at', 'ended_at') if source == 'citibike_trips' else ('tpep_pickup_datetime', 'tpep_dropoff_datetime')
                assert row['started_at_raw'] == p[start_field] and row['ended_at_raw'] == p[end_field]
                if source == 'citibike_trips':
                    assert row['ride_id'] == p['ride_id']
                    for src, target in [('start_lat', 'origin_latitude'), ('start_lng', 'origin_longitude'), ('end_lat', 'destination_latitude'), ('end_lng', 'destination_longitude')]:
                        expected = float(p[src]) if p[src] else None
                        assert row[target] == expected
                remaining.remove(original['record_key']); source_checks += 1
    assert not remaining, 'DWD source samples missing in ODS'
    result = dict(passed=True, run_id=report['run_id'], verified_at_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                  committed_parquet_files_checked=files, table_rows=counts, all_fact_duration_bounds_and_utc_differences_checked=True,
                  traced_ods_sample_rows=source_checks, all_sample_raw_times_coordinates_and_fingerprints_match=True,
                  scope='All committed file counts and durations, sampled source traceability; full key/FK checks performed by Spark build.')
    (ROOT / 'outputs/dwd-verification.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
