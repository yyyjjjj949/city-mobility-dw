"""Pinned DWD -> historical DWS/ADS; candidates stay invisible until publication."""
import argparse
import datetime as dt
import json
from pathlib import Path
import re
from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

ROOT = Path('/workspace')
WORK = ROOT / 'work/marts'


def main():
    p = argparse.ArgumentParser(); p.add_argument('--manifest', required=True); args = p.parse_args()
    path = Path(args.manifest).resolve(); assert path.is_relative_to(WORK / 'runs')
    m = json.loads(path.read_text()); run = path.parent
    contract = {t['name']: t for t in json.loads((WORK / 'schema-contract.json').read_text())['tables']}
    spark = (SparkSession.builder.appName('CityMobility-Historical-Marts')
             .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.warehouse.dir', str(WORK / 'warehouse'))
             .config('spark.sql.shuffle.partitions', '24').config('spark.sql.adaptive.enabled', 'true')
             .config('spark.databricks.delta.snapshotPartitions', '2').config('spark.sql.ui.retainedExecutions', '20')
             .config('spark.ui.retainedJobs', '20').config('spark.ui.retainedStages', '40').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    stamp = dt.datetime.now(dt.timezone.utc)
    start = dt.date.fromisoformat(m['month'] + '-01'); end = (start.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    historical = ['trip_day', 'trip_hour', 'zone_hour', 'od_day', 'weather_group']
    ads_names = historical + ['quality_batch', 'quality_rule']
    result = dict(release_id=m['release_id'], input_fingerprint=m['input_fingerprint'], dwd_run_id=m['dwd']['run_id'],
                  input_versions=m['versions'], status='building', counts={}, output_versions={}, checks={}, month=m['month'], started_at_utc=stamp.isoformat())

    def save():
        temp = run / 'result.tmp'; temp.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str)); temp.replace(run / 'result.json')

    def read(name, pinned=False):
        reader = spark.read.format('delta')
        if pinned: reader = reader.option('versionAsOf', m['versions'][name])
        return reader.load(contract[name]['location'])

    def write(name, df):
        table = contract[name]
        df = df.select(*[F.col(c['name']).cast(c['type']).alias(c['name']) for c in table['columns']])
        writer = df.coalesce(4).write.format('delta').mode('overwrite')
        if name.startswith('ads.'):
            writer = writer.option('replaceWhere', f"release_id='{m['release_id']}'")
        else:
            col = table['partition_by'][0]
            writer = writer.option('replaceWhere', f"{col} >= '{start}' AND {col} < '{end}'")
        writer.save(table['location'])
        data = read(name)
        if name.startswith('ads.'): data = data.where(F.col('release_id') == m['release_id'])
        else: data = data.where(F.col('build_id') == m['release_id'])
        count = data.count()
        assert data.select(*table['logical_key']).distinct().count() == count, name
        required = [c['name'] for c in table['columns'] if not c['nullable']]
        nulls = data.agg(*[F.sum(F.col(c).isNull().cast('int')).alias(c) for c in required]).first()
        assert not any(v for v in nulls), (name, nulls)
        result['counts'][name] = count
        result['output_versions'][name] = int(DeltaTable.forPath(spark, table['location']).history(1).first()['version'])
        if name.startswith('ads.'):
            (data.coalesce(2).write.mode('overwrite').option('timestampFormat', 'yyyy-MM-dd HH:mm:ss.SSSSSS')
             .option('ignoreNullFields', 'false').json(str(run / 'export' / name.split('.')[1])))
        save(); print('MART_TABLE_OK ' + name + ' rows=' + str(count), flush=True)
        return data

    def buildcols(df):
        return (df.withColumn('build_id', F.lit(m['release_id'])).withColumn('input_versions_json', F.lit(json.dumps(m['versions'], sort_keys=True)))
                .withColumn('rule_version', F.lit(m['policies']['version'])).withColumn('built_at_utc', F.lit(stamp)))

    def adscols(df):
        return df.withColumn('release_id', F.lit(m['release_id'])).withColumn('published_at_utc', F.lit(stamp))

    try:
        names = ['dws.' + n for n in historical] + ['ads.' + n for n in ads_names] + ['ops.publication']
        for sql in re.sub(r'^--.*$', '', (WORK / 'lake.sql').read_text(), flags=re.M).split(';'):
            sql = sql.strip()
            if sql and (sql.startswith('SET ') or re.match(r'CREATE DATABASE IF NOT EXISTS (dws|ads|ops)\b', sql) or any(sql.startswith('CREATE TABLE IF NOT EXISTS ' + n + ' ') for n in names)):
                spark.sql(sql)
        parts = []
        common = ['service_date', 'transport_code', 'start_hour_utc', 'origin_zone_key', 'destination_zone_key', 'duration_seconds', 'duration_metric_eligible', 'origin_in_nyc', 'destination_in_nyc']
        for sid, name in [('tlc_yellow', 'dwd.fact_taxi_trip'), ('citibike_trips', 'dwd.fact_bike_trip')]:
            df = read(name, True).where((F.col('service_date') >= F.lit(start)) & (F.col('service_date') < F.lit(end)))
            assert df.count() == m['dwd']['sources'][sid]['accepted_rows']
            if sid == 'citibike_trips':
                df = df.withColumn('distance_km', F.lit(None).cast('double')).withColumn('distance_metric_eligible', F.lit(False))
            parts.append(df.select(*common, 'distance_km', 'distance_metric_eligible'))
        trips = parts[0].unionByName(parts[1]).persist(StorageLevel.DISK_ONLY)
        assert trips.count() == m['dwd']['total_valid_trips']
        zones = read('dwd.dim_zone', True).where('is_nyc').select('zone_key', 'zone_name', 'borough')
        dates = read('dwd.dim_date', True).where((F.col('calendar_date') >= F.lit(start)) & (F.col('calendar_date') < F.lit(end)))
        modes = spark.createDataFrame([('taxi_yellow', '黄色出租车'), ('citibike', '共享单车')], ['transport_code', 'transport_name'])
        aggregates = [F.count('*').alias('trip_count'), F.sum(F.when(F.col('duration_metric_eligible'), F.col('duration_seconds')).otherwise(0.)).alias('duration_sum_seconds'),
                      F.sum(F.col('duration_metric_eligible').cast('long')).alias('duration_count'),
                      F.sum(F.when(F.col('distance_metric_eligible'), F.col('distance_km')).otherwise(0.)).alias('distance_sum_km'),
                      F.sum(F.col('distance_metric_eligible').cast('long')).alias('distance_count'),
                      F.sum(F.col('origin_in_nyc').cast('long')).alias('origin_mapped_count'), F.sum(F.col('destination_in_nyc').cast('long')).alias('destination_mapped_count'),
                      F.sum((F.col('origin_in_nyc') & F.col('destination_in_nyc')).cast('long')).alias('od_mapped_count')]
        numeric = ['trip_count', 'duration_sum_seconds', 'duration_count', 'distance_sum_km', 'distance_count', 'origin_mapped_count', 'destination_mapped_count', 'od_mapped_count']
        def complete(df, scaffold, keys):
            df = scaffold.join(df, keys, 'left').fillna(0, subset=numeric).withColumn('source_complete', F.lit(True))
            return (df.withColumn('distance_sum_km', F.when(F.col('transport_code') == 'taxi_yellow', F.col('distance_sum_km')))
                    .withColumn('distance_count', F.when(F.col('transport_code') == 'taxi_yellow', F.col('distance_count'))))
        day_skeleton = dates.select(F.col('calendar_date').alias('service_date')).crossJoin(modes.select('transport_code'))
        day = write('dws.trip_day', buildcols(complete(trips.groupBy('service_date', 'transport_code').agg(*aggregates), day_skeleton, ['service_date', 'transport_code'])))
        hours = (spark.range(int((end - start).days * 24)).select((F.lit(dt.datetime.combine(start, dt.time(), dt.timezone.utc)) + F.expr('INTERVAL 1 HOUR') * F.col('id') + F.expr('INTERVAL 4 HOURS')).alias('hour_start_utc'))
                 .withColumn('service_date', F.to_date(F.from_utc_timestamp('hour_start_utc', 'America/New_York'))))
        # Initial month is June (no DST transition); reject unsupported months rather than assuming fixed offsets.
        assert m['month'] == '2025-06'
        hour_skeleton = hours.crossJoin(modes.select('transport_code'))
        hourly = trips.withColumnRenamed('start_hour_utc', 'hour_start_utc').groupBy('hour_start_utc', 'service_date', 'transport_code').agg(*aggregates)
        hour = write('dws.trip_hour', buildcols(complete(hourly, hour_skeleton, ['hour_start_utc', 'service_date', 'transport_code'])))
        departure = trips.where('origin_in_nyc').groupBy(F.col('start_hour_utc').alias('hour_start_utc'), F.col('origin_zone_key').alias('zone_key'), 'transport_code').agg(F.count('*').alias('departure_count'))
        zone = (hours.select('hour_start_utc', F.col('service_date').alias('event_date')).crossJoin(zones.select('zone_key')).crossJoin(modes.select('transport_code'))
                .join(departure, ['hour_start_utc', 'zone_key', 'transport_code'], 'left').fillna(0, subset=['departure_count'])
                .withColumn('arrival_count', F.lit(None).cast('long')).withColumn('departure_complete', F.lit(True)).withColumn('arrival_complete', F.lit(False)))
        zone = write('dws.zone_hour', buildcols(zone))
        od = write('dws.od_day', buildcols(trips.where('origin_in_nyc AND destination_in_nyc').groupBy('service_date', 'transport_code', 'origin_zone_key', 'destination_zone_key').agg(F.count('*').alias('trip_count'))))
        weather = read('dwd.dim_weather_daily', True).where('weather_usable').select(F.col('observation_date').alias('service_date'), 'precipitation_group')
        assert weather.count() == (end - start).days and m['dwd']['weather_complete']
        weather_daily = (day.join(weather, 'service_date').join(dates.select(F.col('calendar_date').alias('service_date'), 'is_weekday'), 'service_date')
                         .withColumn('month_start', F.lit(start)))
        group = write('dws.weather_group', buildcols(weather_daily.groupBy('month_start', 'transport_code', 'is_weekday', 'precipitation_group').agg(F.count('*').alias('complete_days'), F.sum('trip_count').alias('trip_count'))))
        # Reconcile all grains, spatial subsets and duration numerators against pinned DWD profiles.
        for sid, mode in [('tlc_yellow', 'taxi_yellow'), ('citibike_trips', 'citibike')]:
            profile = m['dwd']['sources'][sid]['profile']
            daily_profile = day.where(F.col('transport_code') == mode).agg(F.sum('trip_count'), F.sum('duration_sum_seconds'), F.sum('od_mapped_count')).first()
            assert daily_profile[0] == profile['rows']
            assert abs(daily_profile[1] - profile['duration_sum_seconds']) < .01
            assert daily_profile[2] == profile['both_mapped_rows']
            assert hour.where(F.col('transport_code') == mode).agg(F.sum('trip_count')).first()[0] == profile['rows']
            assert od.where(F.col('transport_code') == mode).agg(F.sum('trip_count')).first()[0] == profile['both_mapped_rows']
            assert zone.where(F.col('transport_code') == mode).agg(F.sum('departure_count')).first()[0] == profile['origin_mapped_rows']
            assert group.where(F.col('transport_code') == mode).agg(F.sum('trip_count'), F.sum('complete_days')).first()[:] == (profile['rows'], 30)
        result['checks'].update(Q015=True, Q016_arrival_blocked=True, no_incomplete_arrivals_published=True)
        for name, df in [('trip_day', day), ('trip_hour', hour)]:
            data = (df.join(modes, 'transport_code').withColumn('average_duration_minutes', F.when(F.col('duration_count') > 0, F.col('duration_sum_seconds') / F.col('duration_count') / 60))
                    .withColumn('average_taxi_distance_km', F.when(F.col('distance_count') > 0, F.col('distance_sum_km') / F.col('distance_count')))
                    .withColumn('origin_mapping_rate', F.when(F.col('trip_count') > 0, F.col('origin_mapped_count') / F.col('trip_count'))))
            write('ads.' + name, adscols(data))
        write('ads.zone_hour', adscols(zone.join(zones, 'zone_key').withColumn('net_inflow', F.lit(None).cast('long'))))
        oz = zones.select(F.col('zone_key').alias('origin_zone_key'), F.col('zone_name').alias('origin_zone_name'))
        dz = zones.select(F.col('zone_key').alias('destination_zone_key'), F.col('zone_name').alias('destination_zone_name'))
        write('ads.od_day', adscols(od.join(oz, 'origin_zone_key').join(dz, 'destination_zone_key')))
        group = group.withColumn('daily_average_trips', F.col('trip_count') / F.col('complete_days'))
        baseline = group.where("precipitation_group='no_measurable'").select('month_start', 'transport_code', 'is_weekday', F.col('daily_average_trips').alias('baseline_daily_average'))
        write('ads.weather_group', adscols(group.join(baseline, ['month_start', 'transport_code', 'is_weekday'], 'left').withColumn('demand_ratio', F.when(F.col('baseline_daily_average') > 0, F.col('daily_average_trips') / F.col('baseline_daily_average')))))
        batches = list({v['batch_id'] for v in m['dwd']['sources'].values()})
        b = read('ops.ingest_batch', True).where(F.col('batch_id').isin(batches))
        assert b.count() == 2 and b.where("status != 'validated'").count() == 0
        write('ads.quality_batch', adscols(b.withColumn('acceptance_rate', F.col('accepted_rows') / F.col('input_rows')).withColumn('last_success_at_utc', F.col('finished_at_utc'))))
        q = read('ops.quality_result', True)
        # Include only the quality results that belong to this validated DWD build, including its dimensions.
        qids = list({x['batch_id'] for x in m['dwd']['quality']})
        write('ads.quality_rule', adscols(q.where(F.col('batch_id').isin(qids))))
        trips.unpersist()
        result.update(status='validated', passed=True, arrival_complete=False, net_inflow_available=False, finished_at_utc=dt.datetime.now(dt.timezone.utc).isoformat())
        save(); print('MARTS_ALL_OK ' + json.dumps(result['counts']), flush=True)
    except Exception as exc:
        result.update(status='failed', passed=False, error=str(exc)[:3000]); save(); raise
    finally:
        spark.stop()


if __name__ == '__main__': main()
