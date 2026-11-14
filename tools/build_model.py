"""Single source for the reviewed model contract, dictionary and engine-specific DDL."""
from pathlib import Path
import csv
import hashlib
import json
import re

ROOT = Path(__file__).resolve().parents[1]
TABLES = []

def c(name, dtype, meaning, source='derived', nullable=False, ref=None):
    return dict(name=name, type=dtype, nullable=nullable, meaning=meaning, source=source, reference=ref)

def table(name, title, grain, keys, columns, partition=(), upstream=(), serving=False, dataset=None, storage='delta'):
    TABLES.append(dict(name=name, title=title, grain=grain, logical_key=list(keys), partition_by=list(partition), upstream=list(upstream),
                       columns=columns, serving=serving, dataset_id=dataset, storage=storage,
                       location=f'/workspace/data/{name.replace(".", "/")}' if storage=='delta' else None))

def lineage():
    return [c('source_record_key','STRING','原始记录定位键','ods.source_record.record_key',ref='ods.source_record.record_key'),
            c('batch_id','STRING','加工批次','ops.ingest_batch.batch_id',ref='ops.ingest_batch.batch_id'),
            c('rule_version','STRING','转换与质量规则版本'),c('processed_at_utc','TIMESTAMP','加工时刻，UTC瞬时值')]

def trip_common():
    return [c('started_at_raw','STRING','源开始时间文本'),c('ended_at_raw','STRING','源结束时间文本'),
            c('started_at_local','TIMESTAMP_NTZ','纽约当地开始时间'),c('ended_at_local','TIMESTAMP_NTZ','纽约当地结束时间'),
            c('started_at_utc','TIMESTAMP','开始时间UTC瞬时值'),c('ended_at_utc','TIMESTAMP','结束时间UTC瞬时值'),
            c('service_date','DATE','按开始时间归属的纽约日期'),c('start_date_key','INT','开始日期键',ref='dwd.dim_date.date_key'),
            c('end_date_key','INT','结束日期键',ref='dwd.dim_date.date_key'),c('start_time_key','INT','开始分钟键',ref='dwd.dim_time.time_key'),
            c('end_time_key','INT','结束分钟键',ref='dwd.dim_time.time_key'),c('start_hour_utc','TIMESTAMP','开始小时UTC，区分夏令时重复小时'),
            c('end_hour_utc','TIMESTAMP','结束小时UTC'),c('transport_code','STRING','交通方式',ref='dwd.dim_transport.transport_code'),
            c('origin_zone_key','INT','起点区域键，0为无法映射',ref='dwd.dim_zone.zone_key'),
            c('destination_zone_key','INT','终点区域键，0为无法映射',ref='dwd.dim_zone.zone_key'),
            c('weather_key','STRING','开始日期对应中央公园天气键，缺失为UNKNOWN',ref='dwd.dim_weather_daily.weather_key'),
            c('duration_seconds','DOUBLE','有效行程时长，秒'),c('duration_metric_eligible','BOOLEAN','是否纳入时长指标'),
            c('origin_in_nyc','BOOLEAN','起点是否可靠匹配纽约五行政区'),c('destination_in_nyc','BOOLEAN','终点是否可靠匹配纽约五行政区'),
            c('quality_flags_json','STRING','警告规则代码JSON数组')]

def aggregate_lineage():
    return [c('build_id','STRING','本次汇总构建ID'),c('input_versions_json','STRING','输入Delta表版本与批次JSON'),
            c('rule_version','STRING','指标规则版本'),c('built_at_utc','TIMESTAMP','汇总构建时刻')]

table('ops.ingest_batch','任务批次','一个来源期间的一次执行尝试',['batch_id'],[
 c('batch_id','STRING','执行尝试唯一ID'),c('logical_batch_key','STRING','来源+期间+任务的幂等调度标识'),
 c('source_id','STRING','数据源标识'),c('source_period','STRING','请求月份/日期/实时轮次'),c('attempt_number','INT','重试序号，从1开始'),
 c('status','STRING','planned/running/validated/published/failed'),c('started_at_utc','TIMESTAMP','开始时刻'),
 c('finished_at_utc','TIMESTAMP','结束时刻',nullable=True),c('input_rows','BIGINT','解析成功输入量'),
 c('accepted_rows','BIGINT','本批接收量'),c('quarantined_rows','BIGINT','隔离的不同记录数'),
 c('duplicate_skipped_rows','BIGINT','已接收重复副本跳过量'),c('out_of_scope_rows','BIGINT','范围外量'),
 c('parse_error_rows','BIGINT','可定位的解析失败记录量'),c('rule_version','STRING','规则版本'),
 c('error_summary','STRING','失败摘要，禁止写凭据',nullable=True)])

table('ops.source_object','来源对象清单','一个具有明确业务采集身份的来源对象',['object_id'],[
 c('object_id','STRING','历史SHA来源身份或实时poll身份的稳定键'),c('source_id','STRING','来源编码'),
 c('source_period','STRING','请求期间'),c('source_url','STRING','官方来源URL'),c('relative_path','STRING','D盘原始文件相对路径'),
 c('object_sha256','STRING','完整下载文件SHA256'),c('object_bytes','BIGINT','完整文件字节数'),
 c('poll_id','STRING','GBFS稳定采集轮次ID；历史为空',nullable=True),c('received_at_utc','TIMESTAMP','接收完成时刻'),
 c('first_batch_id','STRING','最初接收批次',ref='ops.ingest_batch.batch_id'),c('expected_members','INT','预期成员数量；普通文件为1'),
 c('processed_members','INT','成功处理的成员数量'),c('is_complete','BOOLEAN','完整下载与成员处理验证通过')])

table('ops.quality_result','规则执行结果','批次×表分区×规则',['result_id'],[
 c('result_id','STRING','规则执行唯一键'),c('batch_id','STRING','执行批次',ref='ops.ingest_batch.batch_id'),
 c('table_name','STRING','被检查表名'),c('partition_spec','STRING','分区条件JSON'),c('rule_id','STRING','规则代码'),
 c('severity','STRING','block/warn'),c('checked_rows','BIGINT','规则适用记录量'),c('failed_rows','BIGINT','失败记录量'),
 c('passed','BOOLEAN','是否满足该规则的发布阈值'),c('details_json','STRING','错误范围与样例定位JSON'),c('checked_at_utc','TIMESTAMP','检查时刻')])

table('ops.quarantine_record','隔离明细','原始记录×失败规则×规则版本',['source_record_key','rule_id','rule_version'],[
 c('source_record_key','STRING','被隔离的ODS记录',ref='ods.source_record.record_key'),c('batch_id','STRING','处理批次',ref='ops.ingest_batch.batch_id'),
 c('rule_id','STRING','失败规则'),c('rule_version','STRING','规则版本'),c('source_id','STRING','数据源'),
 c('source_period','STRING','源期间'),c('field_name','STRING','异常字段，整体记录可为空',nullable=True),
 c('reason','STRING','可解释的错误原因'),c('created_at_utc','TIMESTAMP','隔离时刻')],partition=['source_id','source_period'])

table('ops.publication','发布记录','数据集的一次不可变发布',['release_id'],[
 c('release_id','STRING','发布ID'),c('dataset_id','STRING','historical/quality'),c('scope_json','STRING','覆盖期间与完整性JSON'),
 c('status','STRING','building/validated/active/retired/failed'),c('input_versions_json','STRING','源批次与Delta版本快照'),
 c('output_counts_json','STRING','各输出表记录数'),c('rule_version','STRING','规则版本'),
 c('published_at_utc','TIMESTAMP','实际启用时刻',nullable=True)])

table('ods.source_record','原始记录封装','来源对象成员中的一个原始逻辑记录',['record_key'],[
 c('record_key','STRING','SHA256(object_id|member_path|record_ordinal)'),c('object_id','STRING','完整原始对象身份',ref='ops.source_object.object_id'),
 c('member_path','STRING','ZIP成员名；普通文件用空字符串'),c('record_ordinal','BIGINT','文件成员内从1开始的记录序号，CSV不含标题'),
 c('source_id','STRING','tlc_yellow/citibike_trips/taxi_zones/noaa_daily'),c('source_period','STRING','请求期间，不依赖事件时间可解析'),
 c('payload_json','STRING','保留源字段、原精度、空值和文本的JSON；原文件另存'),c('payload_sha256','STRING','规范化载荷摘要，仅用于内容比较'),
 c('poll_id','STRING','GBFS轮次ID',nullable=True),c('received_at_utc','TIMESTAMP','接收时刻'),
 c('batch_id','STRING','首次接收批次',ref='ops.ingest_batch.batch_id')],partition=['source_id','source_period'],upstream=['ops.source_object'])

table('dwd.dim_date','日期维度','一个公历日期；0是未知占位',['date_key'],[
 c('date_key','INT','yyyyMMdd；未知为0'),c('calendar_date','DATE','公历日期',nullable=True),c('year','INT','年份',nullable=True),
 c('quarter','INT','季度1至4',nullable=True),c('month','INT','月份1至12',nullable=True),c('day_of_month','INT','月内日',nullable=True),
 c('iso_week','INT','ISO周序号',nullable=True),c('iso_week_year','INT','ISO周年份',nullable=True),
 c('iso_day_of_week','INT','周一1至周日7',nullable=True),c('is_weekday','BOOLEAN','仅周一至周五',nullable=True),
 c('is_weekend','BOOLEAN','周六或周日',nullable=True),c('is_holiday','BOOLEAN','未接入可靠日历前为空',nullable=True)],upstream=['calendar'])
table('dwd.dim_time','日内分钟维度','一个当地分钟，共1440个；-1是未知',['time_key'],[
 c('time_key','INT','hour*60+minute；未知-1'),c('hour','INT','小时0至23',nullable=True),c('minute','INT','分钟0至59',nullable=True),
 c('period_label','STRING','夜间/早高峰/日间/晚高峰/晚间'),c('is_peak','BOOLEAN','配置时段的高峰标记；不代表统计结论',nullable=True)],upstream=['configuration'])
table('dwd.dim_zone','区域维度','一个官方区域编码或未知0',['zone_key'],[
 c('zone_key','INT','官方LocationID；未知0'),c('zone_name','STRING','区域名称','taxi_zone_lookup.Zone'),
 c('borough','STRING','行政区/特殊范围','taxi_zone_lookup.Borough'),c('service_zone','STRING','服务区','taxi_zone_lookup.service_zone'),
 c('is_nyc','BOOLEAN','是否属于纽约五行政区'),c('geometry_geojson','STRING','EPSG:4326区域几何JSON',nullable=True),
 c('source_crs','STRING','源EPSG:2263'),c('geometry_source_sha256','STRING','完整官方边界文件摘要',nullable=True)],upstream=['ods.source_record'])
table('dwd.dim_station','站点期间快照维度','命名空间×期间×来源站点业务键',['station_key'],[
 c('station_key','STRING','SHA256(namespace|reference_period|station_business_key)；UNKNOWN为未知'),
 c('namespace','STRING','history_month/unknown'),c('reference_period','STRING','历史来源月份yyyy-MM'),
 c('station_business_key','STRING','历史站点编号字符串或当前GBFS UUID'),c('station_name','STRING','该期间来源名称'),
 c('latitude','DOUBLE','该期间代表坐标纬度',nullable=True),c('longitude','DOUBLE','该期间代表坐标经度',nullable=True),
 c('zone_key','INT','代表坐标匹配区域',ref='dwd.dim_zone.zone_key'),c('short_name','STRING','GBFS短编号；历史可为空',nullable=True),
 c('capacity','INT','当前观察到的容量，不能反填历史',nullable=True),c('attribute_origin','STRING','historical_trip/unknown'),
 c('first_observed_at_utc','TIMESTAMP','本项目首次观察时刻',nullable=True),c('last_observed_at_utc','TIMESTAMP','最近观察时刻',nullable=True),
 c('attributes_conflicted','BOOLEAN','期间名称或坐标存在多个候选')],partition=['namespace'],upstream=['ods.source_record','dwd.dim_zone'])
table('dwd.dim_transport','交通方式维度','一个受控交通方式',['transport_code'],[
 c('transport_code','STRING','taxi_yellow/citibike/UNKNOWN'),c('transport_name','STRING','中文交通方式'),c('source_id','STRING','源编码')],upstream=['configuration'])
table('dwd.dim_bike_type','车型维度','一个来源车型代码',['bike_type_code'],[
 c('bike_type_code','STRING','classic_bike/electric_bike/UNKNOWN等真实源代码'),c('bike_type_name','STRING','展示名称')],upstream=['ods.source_record'])
table('dwd.dim_weather_daily','日天气观察维度','气象站×源观测日期',['weather_key'],[
 c('weather_key','STRING','SHA256(station_id|observation_date)；UNKNOWN为未知'),c('weather_station_id','STRING','USW00094728或UNKNOWN','NOAA.STATION'),
 c('observation_date','DATE','NOAA源观测日期','NOAA.DATE',nullable=True),c('date_key','INT','关联日期键',ref='dwd.dim_date.date_key'),
 c('temperature_max_c','DOUBLE','最高温°C','NOAA.TMAX',nullable=True),c('temperature_min_c','DOUBLE','最低温°C','NOAA.TMIN',nullable=True),
 c('precipitation_mm','DOUBLE','降水mm，metric已转换','NOAA.PRCP',nullable=True),c('wind_speed_mps','DOUBLE','平均风速m/s','NOAA.AWND',nullable=True),
 c('precipitation_group','STRING','measurable/trace/no_measurable/unknown'),c('attributes_json','STRING','所有源观测质量和测量标志JSON'),
 c('weather_usable','BOOLEAN','核心字段有效且源质量检查通过')],upstream=['ods.source_record','dwd.dim_date'])

table('dwd.fact_taxi_trip','出租车行程事实','一个通过核心规则的原始出租车记录',['trip_key'],[
 c('trip_key','STRING','等于ODS记录键，保留同内容的不同源行'),
 *trip_common(),c('vendor_id','INT','源供应商ID','TLC.VendorID',nullable=True),
 c('passenger_count','BIGINT','源乘客数，不作为整行拒收条件','TLC.passenger_count',nullable=True),
 c('rate_code_id','BIGINT','计价代码','TLC.RatecodeID',nullable=True),c('store_and_forward_flag','STRING','源转存标志','TLC.store_and_fwd_flag',nullable=True),
 c('payment_type','BIGINT','支付类型代码','TLC.payment_type',nullable=True),
 c('origin_zone_id_raw','INT','原始起点区域编码','TLC.PULocationID',nullable=True),c('destination_zone_id_raw','INT','原始终点编码','TLC.DOLocationID',nullable=True),
 c('distance_miles','DOUBLE','源距离英里','TLC.trip_distance',nullable=True),c('distance_km','DOUBLE','distance_miles*1.609344',nullable=True),
 c('distance_metric_eligible','BOOLEAN','距离是否适用于均值指标'),c('content_fingerprint','STRING','业务内容摘要，只识别候选重复，不作为自动删除键'),
 *[c(name,'DECIMAL(18,4)',meaning,source,nullable=True) for name,meaning,source in [
 ('fare_amount_usd','基本车费美元','TLC.fare_amount'),('extra_usd','附加费美元','TLC.extra'),('mta_tax_usd','MTA税美元','TLC.mta_tax'),
 ('tip_amount_usd','小费美元','TLC.tip_amount'),('tolls_amount_usd','通行费美元','TLC.tolls_amount'),
 ('improvement_surcharge_usd','改进附加费美元','TLC.improvement_surcharge'),('total_amount_usd','源总额美元','TLC.total_amount'),
 ('congestion_surcharge_usd','拥堵附加费美元','TLC.congestion_surcharge'),('airport_fee_usd','机场费美元','TLC.Airport_fee'),
 ('cbd_congestion_fee_usd','CBD拥堵费美元','TLC.cbd_congestion_fee')]],*lineage()],partition=['service_date'],upstream=['ods.source_record','dwd.dim_date','dwd.dim_time','dwd.dim_zone','dwd.dim_transport','dwd.dim_weather_daily'])

table('dwd.fact_bike_trip','共享单车骑行事实','一个规范化ride_id的一次有效骑行',['ride_id'],[
 c('ride_id','STRING','保留原始ride_id作为业务键','CitiBike.ride_id'),*trip_common(),
 c('origin_station_id_raw','STRING','原始起站编号，保留小数点','CitiBike.start_station_id',nullable=True),
 c('destination_station_id_raw','STRING','原始终站编号','CitiBike.end_station_id',nullable=True),
 c('origin_station_key','STRING','开始月份历史站点键；未知UNKNOWN',ref='dwd.dim_station.station_key'),
 c('destination_station_key','STRING','同一源开始月份历史终站键',ref='dwd.dim_station.station_key'),
 c('origin_latitude','DOUBLE','原始起点纬度','CitiBike.start_lat',nullable=True),c('origin_longitude','DOUBLE','原始起点经度','CitiBike.start_lng',nullable=True),
 c('destination_latitude','DOUBLE','原始终点纬度','CitiBike.end_lat',nullable=True),c('destination_longitude','DOUBLE','原始终点经度','CitiBike.end_lng',nullable=True),
 c('bike_type_raw','STRING','源车型','CitiBike.rideable_type',nullable=True),c('bike_type_code','STRING','规范化车型',ref='dwd.dim_bike_type.bike_type_code'),
 c('member_type','STRING','member/casual/unknown','CitiBike.member_casual'),c('content_fingerprint','STRING','同ride_id比较内容是否冲突'),
 *lineage()],partition=['service_date'],upstream=['ods.source_record','dwd.dim_station','dwd.dim_date','dwd.dim_time','dwd.dim_zone','dwd.dim_bike_type','dwd.dim_transport','dwd.dim_weather_daily'])

table('dws.trip_day','出发行程日汇总','开始日期×交通方式',['service_date','transport_code'],[
 c('service_date','DATE','纽约开始日期'),c('transport_code','STRING','交通方式',ref='dwd.dim_transport.transport_code'),
 c('trip_count','BIGINT','核心有效出发行程数'),c('duration_sum_seconds','DOUBLE','时长指标有效记录秒数总和'),c('duration_count','BIGINT','时长指标分母'),
 c('distance_sum_km','DOUBLE','出租车有效距离之和；骑行为NULL',nullable=True),c('distance_count','BIGINT','出租车距离分母；骑行为NULL',nullable=True),
 c('origin_mapped_count','BIGINT','起点匹配纽约区域的行程数'),c('destination_mapped_count','BIGINT','终点匹配纽约区域的行程数'),
 c('od_mapped_count','BIGINT','两端均匹配纽约区域的行程数'),c('source_complete','BOOLEAN','该开始日期来源覆盖完整'),
 *aggregate_lineage()],partition=['service_date'],upstream=['dwd.fact_taxi_trip','dwd.fact_bike_trip'])

table('dws.zone_hour','事件时间区域小时流量','UTC小时×区域×交通方式',['hour_start_utc','zone_key','transport_code'],[
 c('event_date','DATE','纽约事件日期'),c('hour_start_utc','TIMESTAMP','UTC小时唯一标识'),c('zone_key','INT','纽约区域键',ref='dwd.dim_zone.zone_key'),
 c('transport_code','STRING','交通方式',ref='dwd.dim_transport.transport_code'),c('departure_count','BIGINT','按开始时刻统计；不完整时NULL',nullable=True),
 c('arrival_count','BIGINT','按结束时刻统计；不完整时NULL',nullable=True),c('departure_complete','BOOLEAN','出发输入覆盖完整'),
 c('arrival_complete','BOOLEAN','到达所需相邻月份及有效时长范围覆盖完整'),*aggregate_lineage()],partition=['event_date'],upstream=['dwd.fact_taxi_trip','dwd.fact_bike_trip'])

table('dws.od_day','出发队列OD日汇总','开始日期×交通方式×起区×终区',['service_date','transport_code','origin_zone_key','destination_zone_key'],[
 c('service_date','DATE','纽约开始日期'),c('transport_code','STRING','交通方式',ref='dwd.dim_transport.transport_code'),
 c('origin_zone_key','INT','起区',ref='dwd.dim_zone.zone_key'),c('destination_zone_key','INT','终区',ref='dwd.dim_zone.zone_key'),
 c('trip_count','BIGINT','两端均匹配纽约区域的行程数'),*aggregate_lineage()],partition=['service_date'],upstream=['dwd.fact_taxi_trip','dwd.fact_bike_trip'])

table('dws.weather_group','天气分组汇总','月份×交通方式×工作日标记×降水组',['month_start','transport_code','is_weekday','precipitation_group'],[
 c('month_start','DATE','月份首日'),c('transport_code','STRING','交通方式',ref='dwd.dim_transport.transport_code'),c('is_weekday','BOOLEAN','周一至周五'),
 c('precipitation_group','STRING','降水组'),c('complete_days','BIGINT','来源与天气均完整的日期数，包括真实零行程日'),
 c('trip_count','BIGINT','对应完整日期行程量'),*aggregate_lineage()],partition=['month_start'],upstream=['dws.trip_day','dwd.dim_date','dwd.dim_weather_daily'])

def serve(name,title,grain,keys,columns,partition,upstream,dataset='historical'):
    table(name,title,grain+'×发布版本',['release_id',*keys],
          [c('release_id','STRING','不可变发布ID',ref='ops.publication.release_id'),*columns,c('published_at_utc','TIMESTAMP','结果构建完成时刻')],
          partition,upstream,serving=True,dataset=dataset)

serve('ads.trip_day','出行总览数据','日期×交通方式',['service_date','transport_code'],[
 *[x for x in next(t for t in TABLES if t['name']=='dws.trip_day')['columns'] if x['name'] not in ('build_id','input_versions_json','rule_version','built_at_utc')],
 c('transport_name','STRING','交通方式中文名'),c('average_duration_minutes','DOUBLE','duration_sum_seconds/duration_count/60',nullable=True),
 c('average_taxi_distance_km','DOUBLE','distance_sum_km/distance_count；骑行为NULL',nullable=True),
 c('origin_mapping_rate','DOUBLE','origin_mapped_count/trip_count，0至1',nullable=True)],['service_date'],['dws.trip_day','dwd.dim_transport'])

serve('ads.zone_hour','区域时空与净流入数据','UTC小时×区域×交通方式',['hour_start_utc','zone_key','transport_code'],[
 *[x for x in next(t for t in TABLES if t['name']=='dws.zone_hour')['columns'] if x['name'] not in ('build_id','input_versions_json','rule_version','built_at_utc')],
 c('zone_name','STRING','区域名称'),c('borough','STRING','行政区'),c('net_inflow','BIGINT','仅两端完整时arrival_count-departure_count',nullable=True)],
 ['event_date'],['dws.zone_hour','dwd.dim_zone'])
serve('ads.od_day','OD排行数据','开始日期×交通方式×起终区',['service_date','transport_code','origin_zone_key','destination_zone_key'],[
 *[x for x in next(t for t in TABLES if t['name']=='dws.od_day')['columns'] if x['name'] not in ('build_id','input_versions_json','rule_version','built_at_utc')],
 c('origin_zone_name','STRING','起区名称'),c('destination_zone_name','STRING','终区名称')],['service_date'],['dws.od_day','dwd.dim_zone'])
serve('ads.weather_group','天气对比数据','月份×交通方式×工作日标记×降水组',['month_start','transport_code','is_weekday','precipitation_group'],[
 *[x for x in next(t for t in TABLES if t['name']=='dws.weather_group')['columns'] if x['name'] not in ('build_id','input_versions_json','rule_version','built_at_utc')],
 c('daily_average_trips','DOUBLE','trip_count/complete_days',nullable=True),c('baseline_daily_average','DOUBLE','同月同方式同周类型的no_measurable日均量',nullable=True),
 c('demand_ratio','DOUBLE','daily_average_trips/baseline_daily_average',nullable=True)],['month_start'],['dws.weather_group'])

serve('ads.quality_batch','质量批次总览','批次ID',['batch_id'],[
 c('batch_id','STRING','执行尝试ID',ref='ops.ingest_batch.batch_id'),c('source_id','STRING','数据源'),c('source_period','STRING','期间'),c('status','STRING','运行状态'),
 *[c(x,'BIGINT',m) for x,m in [('input_rows','解析成功输入'),('accepted_rows','接收'),('quarantined_rows','不同记录隔离'),('duplicate_skipped_rows','重复跳过'),('out_of_scope_rows','范围外'),('parse_error_rows','解析失败')]],
 c('acceptance_rate','DOUBLE','accepted_rows/input_rows',nullable=True),c('last_success_at_utc','TIMESTAMP','最近成功时刻',nullable=True)],
 [],['ops.ingest_batch'],dataset='quality')
serve('ads.quality_rule','质量规则明细','规则执行结果ID',['result_id'],[
 *[x for x in next(t for t in TABLES if t['name']=='ops.quality_result')['columns']]],[],['ops.quality_result'],dataset='quality')

table('dws.trip_hour','出发行程小时汇总','开始UTC小时×交通方式',['hour_start_utc','transport_code'],[
 c('hour_start_utc','TIMESTAMP','开始UTC小时，夏令时重复小时分开'),
 *[x for x in next(t for t in TABLES if t['name']=='dws.trip_day')['columns']]],
 partition=['service_date'],upstream=['dwd.fact_taxi_trip','dwd.fact_bike_trip'])
serve('ads.trip_hour','小时趋势数据','开始UTC小时×交通方式',['hour_start_utc','transport_code'],[
 *[x for x in next(t for t in TABLES if t['name']=='ads.trip_day')['columns'] if x['name'] not in ('release_id','published_at_utc')],
 c('hour_start_utc','TIMESTAMP','开始UTC小时，展示时转换纽约时区')],['service_date'],['dws.trip_hour','dwd.dim_transport'])

table('ads.release_pointer','当前发布指针','一个数据集',['dataset_id'],[
 c('dataset_id','STRING','historical/quality'),c('release_id','STRING','当前启用版本'),c('activated_at_utc','TIMESTAMP','指针启用时刻')],storage='clickhouse')

def quote(value):return "'"+value.replace("'","''")+"'"
def ch_type(column):
    t=column['type']
    result={'STRING':'String','INT':'Int32','BIGINT':'Int64','BOOLEAN':'Bool','DOUBLE':'Float64','DATE':'Date32','TIMESTAMP':"DateTime64(6, 'UTC')"}.get(t,t.replace('DECIMAL','Decimal'))
    if t=='TIMESTAMP_NTZ':raise ValueError('Local wall time must not be implicitly converted to a serving instant')
    return f'Nullable({result})' if column['nullable'] else result

def build():
    by_name={t['name']:t for t in TABLES}
    assert len(by_name)==len(TABLES)
    for t in TABLES:
        cols={c['name']:c for c in t['columns']}
        assert len(cols)==len(t['columns']),t['name']
        assert set(t['logical_key']+t['partition_by'])<=cols.keys(),t['name']
        for column in t['columns']:
            if column['reference']:
                target,field=column['reference'].rsplit('.',1)
                assert field in {x['name'] for x in by_name[target]['columns']},column
    model={'version':'2.0-offline','scope':'NYC June 2025 offline warehouse','tables':TABLES}
    (ROOT/'config/model/schema-contract.json').write_text(json.dumps(model,ensure_ascii=False,indent=2),encoding='utf-8')
    spark=["-- Generated by tools/build_model.py. Register empty Delta tables; no business data is inserted.","SET spark.sql.session.timeZone=UTC;"]
    for layer in ['ops','ods','dwd','dws','ads']:
        spark.append(f"CREATE DATABASE IF NOT EXISTS {layer} LOCATION '/workspace/data/{layer}';")
    for t in TABLES:
        if t['storage']!='delta':continue
        fields=',\n'.join('  `'+x['name']+'` '+x['type']+('' if x['nullable'] else ' NOT NULL')+' COMMENT '+quote(x['meaning']) for x in t['columns'])
        partition='\nPARTITIONED BY ('+', '.join('`'+x+'`' for x in t['partition_by'])+')' if t['partition_by'] else ''
        spark.append(f"CREATE TABLE IF NOT EXISTS {t['name']} (\n{fields}\n) USING DELTA{partition}\nLOCATION {quote(t['location'])}\nCOMMENT {quote(t['title']+'；粒度：'+t['grain'])};")
    (ROOT/'sql/spark/001_create_lakehouse.sql').write_text('\n\n'.join(spark)+'\n',encoding='utf-8')
    clickhouse=["-- Generated. Empty serving storage + release-filtered BI views; no raw business rows.","CREATE DATABASE IF NOT EXISTS ads ENGINE = Atomic;"]
    views=[]
    for t in TABLES:
        if not t['serving'] and t['storage']!='clickhouse':continue
        fields=',\n'.join('  `'+x['name']+'` '+ch_type(x)+' COMMENT '+quote(x['meaning']) for x in t['columns'])
        partition=''
        if t['partition_by']:
            assert len(t['partition_by'])==1
            partition=f"\nPARTITION BY (release_id, toYYYYMM({t['partition_by'][0]}))"
        order=', '.join('`'+x+'`' for x in t['logical_key'])
        clickhouse.append(f"CREATE TABLE IF NOT EXISTS {t['name']} (\n{fields}\n) ENGINE = MergeTree{partition}\nORDER BY ({order});")
        if t['serving']:
            view_name='ads.bi_'+t['name'].split('.')[1]
            select=f"SELECT * FROM {t['name']} WHERE release_id IN (SELECT release_id FROM ads.release_pointer WHERE dataset_id={quote(t['dataset_id'])})"
            views.append(f"CREATE VIEW IF NOT EXISTS {view_name} AS {select};")
    (ROOT/'sql/clickhouse/001_create_serving.sql').write_text('\n\n'.join(clickhouse+views)+'\n',encoding='utf-8')
    with (ROOT/'docs/数据字典.csv').open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.writer(f);writer.writerow(['table','table_cn','grain','column','spark_type','clickhouse_type','nullable','logical_key','partition_key','meaning','source_or_derivation','logical_reference'])
        for t in TABLES:
            for x in t['columns']:
                writer.writerow([t['name'],t['title'],t['grain'],x['name'],x['type'] if t['storage']=='delta' else '',ch_type(x) if t['serving'] or t['storage']=='clickhouse' else '',x['nullable'],x['name'] in t['logical_key'],x['name'] in t['partition_by'],x['meaning'],x['source'],x['reference'] or ''])
    lines=['# 物理表清单（自动生成）','','数据字典及DDL与本清单均由 `tools/build_model.py` 生成。主键、外键是逻辑约束，必须由质量任务校验；MergeTree排序键不保证唯一性。','','| 表 | 中文名称 | 粒度 | 逻辑唯一键 | 分区 | 存储 |','| --- | --- | --- | --- | --- | --- |']
    for t in TABLES:
        lines.append('| '+ ' | '.join([t['name'],t['title'],t['grain'],', '.join(t['logical_key']),', '.join(t['partition_by']) or '不分区','Delta + ClickHouse' if t['serving'] else t['storage']])+' |')
    lines+=['','ClickHouse日期分区表使用 `(release_id, toYYYYMM(日期列))`，排序键使用完整逻辑键。发布指针及少量质量汇总不分区。','','BI只连接 `ads.bi_*` 视图，查询最近一次经过验证的离线发布版本。','']
    (ROOT/'docs/05-物理表清单.md').write_text('\n'.join(lines),encoding='utf-8')
    digest=hashlib.sha256((ROOT/'config/model/schema-contract.json').read_bytes()).hexdigest()
    print(json.dumps({'tables':len(TABLES),'lake_tables':sum(t['storage']=='delta' for t in TABLES),'dimensions':sum('.dim_' in t['name'] for t in TABLES),'core_facts':sum('.fact_' in t['name'] for t in TABLES),'columns':sum(len(t['columns']) for t in TABLES),'clickhouse_tables':len(clickhouse)-2,'clickhouse_views':len(views),'contract_sha256':digest}))

if __name__=='__main__':build()
