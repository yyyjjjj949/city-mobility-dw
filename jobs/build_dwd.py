"""Build full-month historical DWD from a pinned ODS Delta version.

All multi-table outputs become usable only through the final validated manifest.
The source/month is rebuilt atomically per table, never appended on retries.
"""
import argparse
import calendar
import datetime as dt
import hashlib
import json
import re
from pathlib import Path

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F, types as T

from dwd_spatial import prepare_zones, match_endpoints, ZoneMatcher

ROOT = Path('/workspace')
WORK = ROOT / 'work/dwd'
UTC = dt.timezone.utc


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str, allow_nan=False)


def key(*values):
    return hashlib.sha256(canonical(list(values)).encode()).hexdigest()


def now():
    return dt.datetime.now(UTC)


def save_json(path, data):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str))
    tmp.replace(path)


def time_columns(df, timezone):
    for prefix in ['started', 'ended']:
        raw, local, utc = prefix + '_at_raw', prefix + '_at_local', prefix + '_at_utc'
        df = df.withColumn(local, F.expr(f'try_cast(`{raw}` AS TIMESTAMP_NTZ)'))
        df = df.withColumn(utc, F.to_utc_timestamp(F.col(local), timezone))
        roundtrip = F.from_utc_timestamp(F.col(utc), timezone).cast('timestamp_ntz')
        previous = F.from_utc_timestamp(F.col(utc) - F.expr('INTERVAL 1 HOUR'), timezone).cast('timestamp_ntz')
        following = F.from_utc_timestamp(F.col(utc) + F.expr('INTERVAL 1 HOUR'), timezone).cast('timestamp_ntz')
        valid = F.col(local).isNotNull() & (roundtrip == F.col(local)) & (previous != F.col(local)) & (following != F.col(local))
        df = df.withColumn('_' + prefix + '_valid', F.coalesce(valid, F.lit(False)))
    return df.withColumn('duration_seconds', (F.unix_micros('ended_at_utc') - F.unix_micros('started_at_utc')) / F.lit(1000000.0))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', required=True)
    args = parser.parse_args()
    path = Path(args.manifest).resolve()
    assert path.is_relative_to(WORK / 'runs')
    m = json.loads(path.read_text())
    run = path.parent
    policy = m['policies']
    start = dt.date.fromisoformat(m['month'] + '-01')
    end = (start.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    timezone = policy['historical_period']['timezone']
    contract = json.loads((WORK / 'schema-contract.json').read_text())
    tables = {t['name']: t for t in contract['tables']}
    spark = (SparkSession.builder.appName('CityMobility-Historical-DWD')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.warehouse.dir', str(WORK / 'warehouse'))
             .config('spark.sql.shuffle.partitions', '32')
             .config('spark.databricks.delta.snapshotPartitions', '2')
             .config('spark.sql.ui.retainedExecutions', '20')
             .config('spark.ui.retainedJobs', '20')
             .config('spark.ui.retainedStages', '40')
             .config('spark.sql.codegen.cache.maxEntries', '50')
             .config('spark.sql.adaptive.enabled', 'true').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    spark.sparkContext.addPyFile('/workspace/jobs/dwd_spatial.py')
    report = {'run_id': m['run_id'], 'month': m['month'], 'input_fingerprint': m['input_fingerprint'],
              'ods_version': m['ods_version'], 'rule_version': policy['version'], 'status': 'building',
              'started_at_utc': now().isoformat(), 'dimensions': {}, 'sources': {}, 'quality': [], 'output_versions': {}}
    if m.get('resume_sources'):
        report['sources'] = m['resume_sources']
        report['quality'] = m['resume_quality']
        report['resumed_from_run'] = m['resumed_from_run']
        report['source_build_runs'] = {sid: m['resumed_from_run'] for sid in m['resume_sources']}
    stamp = now()

    def log(message):
        print(message, flush=True)
        save_json(run / 'result.json', report)

    def schema(name):
        return T.StructType([T.StructField(c['name'], T._parse_datatype_string(c['type']), c['nullable']) for c in tables[name]['columns']])

    def load(name, version=None):
        reader = spark.read.format('delta')
        if version is not None:
            reader = reader.option('versionAsOf', version)
        return reader.load(tables[name]['location'])

    def version(name):
        return int(DeltaTable.forPath(spark, tables[name]['location']).history(1).first()['version'])

    def conform(df, name):
        return df.select(*[F.col(c['name']).cast(c['type']).alias(c['name']) for c in tables[name]['columns']])

    def write(name, df, predicate=None):
        writer = conform(df, name).write.format('delta').mode('overwrite')
        if predicate:
            writer = writer.option('replaceWhere', predicate)
        writer.save(tables[name]['location'])
        report['output_versions'][name] = version(name)

    def small(name, rows, predicate=None):
        write(name, spark.createDataFrame(rows, schema(name)).coalesce(1), predicate)
        report['dimensions'][name] = load(name).count()

    def upsert(name, rows, keys):
        df = spark.createDataFrame(rows, schema(name))
        cond = ' AND '.join(f't.`{c}` = s.`{c}`' for c in keys)
        DeltaTable.forPath(spark, tables[name]['location']).alias('t').merge(df.alias('s'), cond).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    def quality(batch, name, rule, checked, failed, severity='block', passed=None, **details):
        result = dict(result_id=key(batch, name, rule), batch_id=batch, table_name=name,
                      partition_spec=canonical({'source_period': m['month']}), rule_id=rule, severity=severity,
                      checked_rows=int(checked), failed_rows=int(failed), passed=(failed == 0 if passed is None else passed),
                      details_json=canonical(details), checked_at_utc=stamp)
        report['quality'].append({k: v for k, v in result.items() if k not in ['result_id', 'checked_at_utc']})
        upsert('ops.quality_result', [result], ['result_id'])

    def parsed(sid):
        df = ods.where(F.col('source_id') == sid)
        df = df.withColumn('_p', F.from_json('payload_json', T.MapType(T.StringType(), T.StringType())))
        required = ['Borough', 'LocationID', 'Zone', 'service_zone'] if sid == 'taxi_zones' else list(m['field_mapping'][sid])
        # Missing JSON keys are structural errors; present null values are field-level quality.
        invalid = F.col('_p').isNull()
        for field in required:
            invalid = invalid | ~F.array_contains(F.map_keys('_p'), field)
        failures = df.where(invalid).count()
        if failures:
            raise ValueError(f'{sid}: {failures} rows missing required source fields')
        return df.select('*', *[F.col('_p')[c].alias(c) for c in required]).drop('_p')

    def json_flags(*pairs):
        return F.to_json(F.filter(F.array(*[F.when(cond, F.lit(code)) for cond, code in pairs]), lambda c: c.isNotNull()))

    def check_unique(name):
        df = load(name)
        n = df.count()
        if df.select(*tables[name]['logical_key']).distinct().count() != n:
            raise ValueError('Duplicate logical keys: ' + name)
        required = [c['name'] for c in tables[name]['columns'] if not c['nullable']]
        nulls = df.agg(*[F.sum(F.col(c).isNull().cast('long')).alias(c) for c in required]).first().asDict()
        if any(v for v in nulls.values()):
            raise ValueError('Null required fields: ' + name + ' ' + canonical(nulls))
        return n

    try:
        ddl = re.sub(r'^--.*$', '', (WORK / 'lake.sql').read_text(), flags=re.M)
        selected = ['ops.ingest_batch', 'ops.quality_result', 'ops.quarantine_record'] + [n for n in tables if n.startswith('dwd.') and 'snapshot' not in n and n != 'dwd.station_link']
        for statement in ddl.split(';'):
            text = statement.strip()
            if text and (text.startswith('SET ') or re.match(r'CREATE DATABASE IF NOT EXISTS (ops|dwd)\b', text) or any(text.startswith('CREATE TABLE IF NOT EXISTS ' + name + ' ') for name in selected)):
                spark.sql(text)
        ods = load('ods.source_record', m['ods_version']).where(F.col('source_period') == m['month'])

        # Engine-level timezone regression includes both DST edge cases and subsecond duration.
        test = spark.createDataFrame([
            ('2025-11-02 01:30:00', '2025-11-02 02:30:00'),
            ('2025-03-09 02:30:00', '2025-03-09 03:30:00'),
            ('2025-06-01 00:00:00.100', '2025-06-01 00:00:01.350'),
            ('bad', '2025-06-01 00:00:00')], ['started_at_raw', 'ended_at_raw'])
        answers = time_columns(test, timezone).collect()
        assert [r['_started_valid'] for r in answers] == [False, False, True, False]
        assert answers[2]['duration_seconds'] == 1.25
        assert answers[2]['started_at_utc'].hour == 4
        report['timezone_regressions_passed'] = True

        asset = m['geometry']
        geometry = prepare_zones(ROOT / asset['relative_path'], asset['sha256'], WORK / 'zones.geojson')
        save_json(run / 'geometry.json', geometry['provenance'])
        # Shared-boundary coverage: deterministic lowest ID; null and outside remain unknown.
        fixture = ZoneMatcher([{'properties': {'LocationID': i}, 'geometry': {'type': 'Polygon', 'coordinates': [coords]}}
                               for i, coords in [(2, [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]), (1, [[1, 0], [2, 0], [2, 1], [1, 1], [1, 0]])]])
        assert fixture.match(.5, 1) == (1, 2) and fixture.match(None, 1) == (0, 0) and fixture.match(3, 3) == (0, 0)
        report['spatial_regressions_passed'] = True
        geos = {f['properties']['LocationID']: canonical(f['geometry']) for f in geometry['features']}
        zones = [dict(zone_key=0, zone_name='未知区域', borough='UNKNOWN', service_zone='UNKNOWN', is_nyc=False,
                      geometry_geojson=None, source_crs='EPSG:2263', geometry_source_sha256=None)]
        for row in parsed('taxi_zones').collect():
            loc = int(row['LocationID'])
            zones.append(dict(zone_key=loc, zone_name=row['Zone'], borough=row['Borough'], service_zone=row['service_zone'] or 'UNKNOWN',
                              is_nyc=row['Borough'] in policy['spatial']['nyc_boroughs'], geometry_geojson=geos.get(loc), source_crs='EPSG:2263', geometry_source_sha256=asset['sha256'] if loc in geos else None))
        assert len(zones) == 266 and len({x['zone_key'] for x in zones}) == 266
        assert set(geos) <= {x['zone_key'] for x in zones}
        small('dwd.dim_zone', zones)
        nyc = [r['zone_key'] for r in zones if r['is_nyc']]
        known = [r['zone_key'] for r in zones]
        report['geometry'] = {**geometry['provenance'], 'geometry_zones': len(geos)}

        dates = [dict(date_key=0, calendar_date=None, year=None, quarter=None, month=None, day_of_month=None,
                      iso_week=None, iso_week_year=None, iso_day_of_week=None, is_weekday=None, is_weekend=None, is_holiday=None)]
        day = dt.date(start.year, 1, 1)
        while day <= dt.date(end.year, 12, 31):
            iso = day.isocalendar()
            dates.append(dict(date_key=int(day.strftime('%Y%m%d')), calendar_date=day, year=day.year, quarter=(day.month - 1) // 3 + 1,
                              month=day.month, day_of_month=day.day, iso_week=iso.week, iso_week_year=iso.year, iso_day_of_week=iso.weekday,
                              is_weekday=day.weekday() < 5, is_weekend=day.weekday() >= 5, is_holiday=None))
            day += dt.timedelta(days=1)
        small('dwd.dim_date', dates)
        times = [dict(time_key=-1, hour=None, minute=None, period_label='未知', is_peak=None)]
        for minute in range(1440):
            hour = minute // 60
            peak = hour in policy['peak_hours_local']['morning'] + policy['peak_hours_local']['evening']
            label = '早高峰' if hour in policy['peak_hours_local']['morning'] else '晚高峰' if hour in policy['peak_hours_local']['evening'] else '夜间' if hour < 7 else '日间' if hour < 19 else '晚间'
            times.append(dict(time_key=minute, hour=hour, minute=minute % 60, period_label=label, is_peak=peak))
        small('dwd.dim_time', times)
        small('dwd.dim_transport', [dict(transport_code=c, transport_name=n, source_id=s) for c, n, s in [('UNKNOWN', '未知', 'UNKNOWN'), ('taxi_yellow', '黄色出租车', 'tlc_yellow'), ('citibike', '共享单车', 'citibike_trips')]])

        weather = [dict(weather_key='UNKNOWN', weather_station_id='UNKNOWN', observation_date=None, date_key=0,
                        temperature_max_c=None, temperature_min_c=None, precipitation_mm=None, wind_speed_mps=None,
                        precipitation_group='unknown', attributes_json='{}', weather_usable=False)]
        for row in ods.where(F.col('source_id') == 'noaa_daily').select('payload_json').collect():
            p = json.loads(row['payload_json'])
            date = dt.date.fromisoformat(p['DATE'])
            values = {}
            flags = {k: v for k, v in p.items() if k.endswith('_ATTRIBUTES')}
            usable = True
            for name in ['TMAX', 'TMIN', 'PRCP', 'AWND']:
                try:
                    value = float(p[name])
                    if not (-1e6 < value < 1e6) or value == -9999:
                        raise ValueError('Invalid value')
                except (KeyError, ValueError, TypeError):
                    value = None
                attributes = p.get(name + '_ATTRIBUTES', '').split(',')
                if value is None or len(attributes) < 3 or attributes[1].strip():
                    usable = False
                values[name] = value
            usable = usable and values['TMIN'] <= values['TMAX'] and values['PRCP'] >= 0 and values['AWND'] >= 0
            trace = p.get('PRCP_ATTRIBUTES', '').split(',')[0] == 'T'
            group = 'unknown' if not usable else 'trace' if trace else 'measurable' if values['PRCP'] > 0 else 'no_measurable'
            weather.append(dict(weather_key=key(p['STATION'], p['DATE']), weather_station_id=p['STATION'], observation_date=date, date_key=int(date.strftime('%Y%m%d')),
                                temperature_max_c=values['TMAX'], temperature_min_c=values['TMIN'], precipitation_mm=values['PRCP'], wind_speed_mps=values['AWND'],
                                precipitation_group=group, attributes_json=canonical(flags), weather_usable=bool(usable)))
        assert len({r['weather_key'] for r in weather}) == len(weather)
        small('dwd.dim_weather_daily', weather)
        expected_days = calendar.monthrange(start.year, start.month)[1]
        usable_dates = {r['observation_date'] for r in weather if r['weather_usable'] and r['observation_date'] and start <= r['observation_date'] < end}
        report['weather_complete'] = len(usable_dates) == expected_days
        quality(key(m['run_id'], 'dimensions'), 'dwd.dim_weather_daily', 'Q009', expected_days, expected_days - len(usable_dates), severity='block_weather_publication')
        weather_df = load('dwd.dim_weather_daily').where(F.col('weather_key') != 'UNKNOWN').select(F.col('observation_date').alias('service_date'), 'weather_key')
        log('DWD_BASE_DIMENSIONS_OK')

        source_specs = [('tlc_yellow', 'dwd.fact_taxi_trip', 'taxi_yellow'), ('citibike_trips', 'dwd.fact_bike_trip', 'citibike')]
        for sid, name, transport in source_specs:
            if sid in report['sources']:
                assert version(name) == m['resume_output_versions'][name]
                assert load(name).count() == report['sources'][sid]['accepted_rows']
                report['output_versions'][name] = version(name)
                log('DWD_SOURCE_REUSED ' + sid)
                continue
            batch_id = key(m['run_id'], sid, 'dwd')
            attempts = load('ops.ingest_batch').where((F.col('logical_batch_key') == key('dwd', sid, m['month']))).count()
            batch = dict(batch_id=batch_id, logical_batch_key=key('dwd', sid, m['month']), source_id=sid, source_period=m['month'],
                         attempt_number=attempts + 1, status='running', started_at_utc=now(), finished_at_utc=None,
                         input_rows=m['expected_rows'][sid], accepted_rows=0, quarantined_rows=0, duplicate_skipped_rows=0,
                         out_of_scope_rows=0, parse_error_rows=0, rule_version=policy['version'], error_summary=None)
            upsert('ops.ingest_batch', [batch], ['batch_id'])
            log('DWD_SOURCE_START ' + sid)
            try:
                raw = parsed(sid).withColumn('content_fingerprint', F.col('payload_sha256'))
                raw = raw.drop('payload_json', 'payload_sha256', 'object_id', 'member_path', 'record_ordinal', 'poll_id', 'batch_id')
                bike = sid == 'citibike_trips'
                if bike:
                    conflicts = raw.where(F.length(F.trim('ride_id')) > 0).groupBy('ride_id').agg(F.countDistinct('content_fingerprint').alias('n')).where('n > 1')
                    conflict_count = conflicts.count()
                    quality(batch_id, name, 'Q006', batch['input_rows'], conflict_count, conflict_ride_id_samples=[r['ride_id'] for r in conflicts.limit(10).collect()] if conflict_count else [])
                    if conflict_count:
                        raise ValueError(f'{conflict_count} conflicting ride IDs; bike output blocked')
                    raw = raw.withColumn('_copy', F.row_number().over(Window.partitionBy('ride_id').orderBy('record_key')))
                    raw = raw.withColumn('started_at_raw', F.col('started_at')).withColumn('ended_at_raw', F.col('ended_at'))
                else:
                    raw = raw.withColumn('_copy', F.lit(1)).withColumn('started_at_raw', F.col('tpep_pickup_datetime')).withColumn('ended_at_raw', F.col('tpep_dropoff_datetime'))
                df = time_columns(raw, timezone).withColumn('service_date', F.to_date('started_at_local'))
                time_bad = ~(F.col('_started_valid') & F.col('_ended_valid'))
                limits = policy['duration_domain_seconds'][transport]
                duration_bad = (~time_bad) & ((F.col('duration_seconds') <= limits['minimum_exclusive']) | (F.col('duration_seconds') > limits['maximum_inclusive']))
                id_bad = F.col('ride_id').isNull() | (F.length(F.trim('ride_id')) == 0) if bike else F.lit(False)
                df = df.withColumn('_errors', F.filter(F.array(F.when(time_bad, F.lit('Q003')), F.when(duration_bad, F.lit('Q004')), F.when(id_bad, F.lit('Q005'))), lambda c: c.isNotNull()))
                df = df.withColumn('_status', F.when(F.size('_errors') > 0, 'quarantined').when((F.col('service_date') < F.lit(start)) | (F.col('service_date') >= F.lit(end)), 'out_of_scope').when(F.col('_copy') > 1, 'duplicate_skipped').otherwise('accepted'))
                df = df.persist(StorageLevel.DISK_ONLY)
                counts = {r['_status']: r['count'] for r in df.groupBy('_status').count().collect()}
                for category in ['accepted', 'quarantined', 'out_of_scope', 'duplicate_skipped']:
                    batch[category + '_rows'] = counts.get(category, 0)
                assert sum(counts.values()) == batch['input_rows'], counts
                quality(batch_id, name, 'Q013', batch['input_rows'], 0, categories=counts)
                fraction = batch['quarantined_rows'] / batch['input_rows']
                quality(batch_id, name, 'Q014', batch['input_rows'], batch['quarantined_rows'], passed=fraction <= policy['quality']['max_quarantine_fraction'], quarantine_fraction=fraction, limit=policy['quality']['max_quarantine_fraction'])
                quarantine = df.where("_status = 'quarantined'").select(F.col('record_key').alias('source_record_key'), F.explode('_errors').alias('rule_id'))
                quarantine = (quarantine.withColumn('batch_id', F.lit(batch_id)).withColumn('rule_version', F.lit(policy['version']))
                              .withColumn('source_id', F.lit(sid)).withColumn('source_period', F.lit(m['month'])).withColumn('field_name', F.when(F.col('rule_id') == 'Q005', 'ride_id').otherwise('started_at,ended_at'))
                              .withColumn('reason', F.when(F.col('rule_id') == 'Q003', '时间不可解析、夏令时歧义或当地时间不存在').when(F.col('rule_id') == 'Q004', '时长不在项目约定的核心有效域').otherwise('ride_id为空'))
                              .withColumn('created_at_utc', F.lit(stamp)))
                write('ops.quarantine_record', quarantine, f"source_id='{sid}' AND source_period='{m['month']}'")
                for rule in ['Q003', 'Q004'] + (['Q005'] if bike else []):
                    n = df.where(F.array_contains('_errors', rule)).count()
                    quality(batch_id, name, rule, batch['input_rows'], n, severity='warn', passed=True, action='quarantine_record')
                if fraction > policy['quality']['max_quarantine_fraction']:
                    raise ValueError('Quarantine fraction exceeds configured limit')
                accepted = df.where("_status = 'accepted'")
                if bike:
                    match_udf = F.udf(match_endpoints, 'origin INT, destination INT, multiple BOOLEAN')
                    for src, target in [('start_lat', 'origin_latitude'), ('start_lng', 'origin_longitude'), ('end_lat', 'destination_latitude'), ('end_lng', 'destination_longitude')]:
                        accepted = accepted.withColumn(target, F.expr(f'try_cast(`{src}` AS DOUBLE)'))
                    accepted = accepted.withColumn('_spatial', match_udf('origin_latitude', 'origin_longitude', 'destination_latitude', 'destination_longitude'))
                    accepted = (accepted.withColumn('origin_zone_key', F.col('_spatial.origin')).withColumn('destination_zone_key', F.col('_spatial.destination'))
                                .withColumn('origin_station_id_raw', F.when(F.length(F.trim('start_station_id')) > 0, F.col('start_station_id')))
                                .withColumn('destination_station_id_raw', F.when(F.length(F.trim('end_station_id')) > 0, F.col('end_station_id'))))
                    for role in ['origin', 'destination']:
                        accepted = accepted.withColumn(role + '_station_key', F.when(F.col(role + '_station_id_raw').isNull(), F.lit('UNKNOWN')).otherwise(F.sha2(F.to_json(F.array(F.lit('history_month'), F.lit(m['month']), F.col(role + '_station_id_raw'))), 256)))
                    accepted = (accepted.withColumn('bike_type_raw', F.col('rideable_type'))
                                .withColumn('bike_type_code', F.when(F.length(F.trim('rideable_type')) > 0, F.col('rideable_type')).otherwise('UNKNOWN'))
                                .withColumn('member_type', F.when(F.col('member_casual').isin('member', 'casual'), F.col('member_casual')).otherwise('unknown')))
                    flags = [(~F.col('origin_zone_key').isin(nyc) | ~F.col('destination_zone_key').isin(nyc), 'Q008'), (F.col('_spatial.multiple'), 'SPATIAL_MULTIPLE_MATCHES'), (F.col('origin_station_id_raw').isNull() | F.col('destination_station_id_raw').isNull(), 'STATION_MISSING')]
                else:
                    accepted = accepted.withColumn('trip_key', F.col('record_key'))
                    for src, target in [('VendorID', 'vendor_id'), ('passenger_count', 'passenger_count'), ('RatecodeID', 'rate_code_id'), ('payment_type', 'payment_type'), ('PULocationID', 'origin_zone_id_raw'), ('DOLocationID', 'destination_zone_id_raw')]:
                        accepted = accepted.withColumn(target, F.expr(f'try_cast(`{src}` AS BIGINT)'))
                    accepted = (accepted.withColumn('origin_zone_key', F.when(F.col('origin_zone_id_raw').isin(known), F.col('origin_zone_id_raw')).otherwise(0))
                                .withColumn('destination_zone_key', F.when(F.col('destination_zone_id_raw').isin(known), F.col('destination_zone_id_raw')).otherwise(0))
                                .withColumn('store_and_forward_flag', F.col('store_and_fwd_flag'))
                                .withColumn('distance_miles', F.expr('try_cast(trip_distance AS DOUBLE)'))
                                .withColumn('distance_km', F.col('distance_miles') * 1.609344))
                    domain = policy['taxi_distance_metric_miles']
                    valid_distance = (F.col('distance_miles') > domain['minimum_exclusive']) & (F.col('distance_miles') <= domain['maximum_inclusive']) & ~F.isnan('distance_miles')
                    accepted = accepted.withColumn('distance_metric_eligible', F.coalesce(valid_distance, F.lit(False)))
                    money = [('fare_amount', 'fare_amount_usd'), ('extra', 'extra_usd'), ('mta_tax', 'mta_tax_usd'), ('tip_amount', 'tip_amount_usd'), ('tolls_amount', 'tolls_amount_usd'), ('improvement_surcharge', 'improvement_surcharge_usd'), ('total_amount', 'total_amount_usd'), ('congestion_surcharge', 'congestion_surcharge_usd'), ('Airport_fee', 'airport_fee_usd'), ('cbd_congestion_fee', 'cbd_congestion_fee_usd')]
                    negative, parse_bad = F.lit(False), F.lit(False)
                    for src, target in money:
                        accepted = accepted.withColumn(target, F.expr(f'try_cast(`{src}` AS DECIMAL(18,4))'))
                        negative = negative | F.coalesce(F.col(target) < 0, F.lit(False))
                        parse_bad = parse_bad | (F.col(src).isNotNull() & F.col(target).isNull())
                    flags = [(~F.col('distance_metric_eligible'), 'Q007'), (~F.col('origin_zone_key').isin(nyc) | ~F.col('destination_zone_key').isin(nyc), 'Q008'), (F.col('passenger_count').isNull() | (F.col('passenger_count') <= 0), 'PASSENGER_MISSING_OR_ZERO'), (negative, 'NEGATIVE_AMOUNT'), (F.abs(F.col('total_amount_usd')) > 1000, 'EXTREME_TOTAL_AMOUNT'), (parse_bad, 'AMOUNT_PARSE_OR_OVERFLOW')]
                accepted = (accepted.withColumn('quality_flags_json', json_flags(*flags))
                            .withColumn('origin_in_nyc', F.col('origin_zone_key').isin(nyc)).withColumn('destination_in_nyc', F.col('destination_zone_key').isin(nyc))
                            .withColumn('transport_code', F.lit(transport)).withColumn('duration_metric_eligible', F.lit(True))
                            .join(F.broadcast(weather_df), 'service_date', 'left').withColumn('weather_key', F.coalesce('weather_key', F.lit('UNKNOWN')))
                            .withColumn('source_record_key', F.col('record_key')).withColumn('batch_id', F.lit(batch_id))
                            .withColumn('rule_version', F.lit(policy['version'])).withColumn('processed_at_utc', F.lit(stamp)))
                for role, prefix in [('start', 'started'), ('end', 'ended')]:
                    local, utc = F.col(prefix + '_at_local'), F.col(prefix + '_at_utc')
                    accepted = (accepted.withColumn(role + '_date_key', F.date_format(local, 'yyyyMMdd').cast('int'))
                                .withColumn(role + '_time_key', F.hour(local) * 60 + F.minute(local)).withColumn(role + '_hour_utc', F.date_trunc('hour', utc)))
                log('DWD_WRITING_FACT ' + sid + ' ' + canonical(counts))
                write(name, conform(accepted, name).repartition(8, 'service_date'), f"service_date >= '{start}' AND service_date < '{end}'")
                df.unpersist()
                fact = load(name).where((F.col('service_date') >= F.lit(start)) & (F.col('service_date') < F.lit(end)))
                assert fact.count() == batch['accepted_rows']
                if bike:
                    # Every source ride can contribute historical station metadata, even if its core time fails.
                    candidates = []
                    for role in ['start', 'end']:
                        candidates.append(raw.select(F.col(role + '_station_id').alias('station_business_key'), F.col(role + '_station_name').alias('station_name'), F.round(F.expr(f'try_cast({role}_lat AS DOUBLE)'), 6).alias('latitude'), F.round(F.expr(f'try_cast({role}_lng AS DOUBLE)'), 6).alias('longitude'), 'received_at_utc').where(F.length(F.trim('station_business_key')) > 0))
                    attrs = candidates[0].unionByName(candidates[1])
                    stats = attrs.groupBy('station_business_key', 'station_name', 'latitude', 'longitude').agg(F.count('*').alias('_frequency'), F.min('received_at_utc').alias('_first'), F.max('received_at_utc').alias('_last'))
                    stats = stats.withColumn('_tuple', F.to_json(F.array(F.coalesce('station_name', F.lit('')), F.coalesce(F.col('latitude').cast('string'), F.lit('')), F.coalesce(F.col('longitude').cast('string'), F.lit('')))))
                    win = Window.partitionBy('station_business_key')
                    stats = (stats.withColumn('attributes_conflicted', F.count('*').over(win) > 1).withColumn('first_observed_at_utc', F.min('_first').over(win))
                             .withColumn('last_observed_at_utc', F.max('_last').over(win)).withColumn('_rank', F.row_number().over(win.orderBy(F.desc('_frequency'), F.asc('_tuple')))).where('_rank=1'))
                    stats = (stats.withColumn('namespace', F.lit('history_month')).withColumn('reference_period', F.lit(m['month']))
                             .withColumn('station_key', F.sha2(F.to_json(F.array('namespace', 'reference_period', 'station_business_key')), 256))
                             .withColumn('station_name', F.coalesce(F.col('station_name'), F.lit('未提供名称')))
                             .withColumn('zone_key', F.udf(lambda lat, lon: match_endpoints(lat, lon, None, None)[0], 'int')('latitude', 'longitude'))
                             .withColumn('short_name', F.lit(None).cast('string')).withColumn('capacity', F.lit(None).cast('int')).withColumn('attribute_origin', F.lit('historical_trip')))
                    unknown = dict(station_key='UNKNOWN', namespace='unknown', reference_period='UNKNOWN', station_business_key='UNKNOWN', station_name='未知站点', latitude=None, longitude=None, zone_key=0, short_name=None, capacity=None, attribute_origin='unknown', first_observed_at_utc=None, last_observed_at_utc=None, attributes_conflicted=False)
                    stations = conform(stats, 'dwd.dim_station').unionByName(spark.createDataFrame([unknown], schema('dwd.dim_station')))
                    write('dwd.dim_station', stations, f"(namespace='history_month' AND reference_period='{m['month']}') OR namespace='unknown'")
                    report['dimensions']['dwd.dim_station'] = load('dwd.dim_station').count()
                    types = [r['rideable_type'] for r in raw.select('rideable_type').distinct().collect() if r['rideable_type'] and r['rideable_type'].strip()]
                    small('dwd.dim_bike_type', [dict(bike_type_code=c, bike_type_name={'classic_bike': '经典单车', 'electric_bike': '电助力单车', 'UNKNOWN': '未知'}.get(c, c)) for c in sorted(set(types + ['UNKNOWN']))])
                summary = fact.agg(F.count('*').alias('rows'), F.sum('duration_seconds').alias('duration_sum_seconds'), F.min('duration_seconds').alias('duration_min_seconds'), F.max('duration_seconds').alias('duration_max_seconds'), F.sum(F.col('origin_in_nyc').cast('long')).alias('origin_mapped_rows'), F.sum(F.col('destination_in_nyc').cast('long')).alias('destination_mapped_rows'), F.sum((F.col('origin_in_nyc') & F.col('destination_in_nyc')).cast('long')).alias('both_mapped_rows')).first().asDict()
                flag_counts = {r['flag']: r['count'] for r in fact.select(F.explode(F.from_json('quality_flags_json', 'ARRAY<STRING>')).alias('flag')).groupBy('flag').count().collect()}
                quality(batch_id, name, 'Q008', summary['rows'], summary['rows'] - summary['both_mapped_rows'], severity='warn', passed=True, action='retain_trip_exclude_unmapped_spatial_metrics', multiple_polygon_rows=flag_counts.get('SPATIAL_MULTIPLE_MATCHES', 0))
                if not bike:
                    quality(batch_id, name, 'Q007', summary['rows'], flag_counts.get('Q007', 0), severity='warn', passed=True, action='retain_trip_exclude_distance_metric')
                daily = [r.asDict() for r in fact.groupBy('service_date').agg(F.count('*').alias('trips'), F.avg('duration_seconds').alias('average_duration_seconds')).orderBy('service_date').collect()]
                preview = [r.asDict() for r in fact.orderBy('service_date', tables[name]['logical_key'][0]).limit(20).collect()]
                save_json(run / (sid + '-preview.json'), preview)
                summary.update(quality_flag_counts=flag_counts, daily=daily)
                report['sources'][sid] = {**{k: batch[k] for k in ['input_rows', 'accepted_rows', 'quarantined_rows', 'duplicate_skipped_rows', 'out_of_scope_rows', 'parse_error_rows']}, 'batch_id': batch_id, 'profile': summary}
                batch.update(status='validated', finished_at_utc=now())
                upsert('ops.ingest_batch', [batch], ['batch_id'])
                log('DWD_SOURCE_OK ' + sid + ' accepted=' + str(batch['accepted_rows']))
            except Exception as exc:
                batch.update(status='failed', finished_at_utc=now(), error_summary=str(exc)[:2000])
                upsert('ops.ingest_batch', [batch], ['batch_id'])
                raise

        # Full key/FK checks use actual committed output, including explicit unknown members.
        for name in report['dimensions']:
            assert check_unique(name) == report['dimensions'][name]
        for sid, name, transport in source_specs:
            fact = load(name).where((F.col('service_date') >= F.lit(start)) & (F.col('service_date') < F.lit(end)))
            assert fact.select(*tables[name]['logical_key']).distinct().count() == report['sources'][sid]['accepted_rows']
            refs = [('start_date_key', 'dwd.dim_date', 'date_key'), ('end_date_key', 'dwd.dim_date', 'date_key'), ('start_time_key', 'dwd.dim_time', 'time_key'), ('end_time_key', 'dwd.dim_time', 'time_key'), ('origin_zone_key', 'dwd.dim_zone', 'zone_key'), ('destination_zone_key', 'dwd.dim_zone', 'zone_key'), ('weather_key', 'dwd.dim_weather_daily', 'weather_key'), ('transport_code', 'dwd.dim_transport', 'transport_code')]
            if transport == 'citibike':
                refs += [('origin_station_key', 'dwd.dim_station', 'station_key'), ('destination_station_key', 'dwd.dim_station', 'station_key'), ('bike_type_code', 'dwd.dim_bike_type', 'bike_type_code')]
            unresolved = 0
            values = fact.agg(*[F.collect_set(F.col(col)).alias(col) for col, _, _ in refs]).first().asDict()
            for col, dim, dimcol in refs:
                allowed = {r[dimcol] for r in load(dim).select(dimcol).collect()}
                unresolved += len(set(values[col]) - allowed)
            quality(report['sources'][sid]['batch_id'], name, 'Q012', report['sources'][sid]['accepted_rows'], unresolved, references_checked=len(refs))
            assert unresolved == 0, 'Unresolved fact references: ' + name
        report.update(status='validated', passed=True, finished_at_utc=now().isoformat(),
                      total_valid_trips=sum(r['accepted_rows'] for r in report['sources'].values()),
                      total_quarantined_rows=sum(r['quarantined_rows'] for r in report['sources'].values()),
                      arrival_boundary_complete=False, downstream_note='May boundary sources not collected; complete arrivals/net inflow must remain unpublished. DWS/ADS are built by the separate historical marts job.')
        save_json(run / 'result.json', report)
        print('DWD_ALL_OK ' + canonical({'trips': report['total_valid_trips'], 'quarantined': report['total_quarantined_rows']}), flush=True)
    except Exception as exc:
        report.update(status='failed', passed=False, error=str(exc)[:3000], finished_at_utc=now().isoformat())
        save_json(run / 'result.json', report)
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
