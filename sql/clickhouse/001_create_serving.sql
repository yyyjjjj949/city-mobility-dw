-- Generated. Empty serving storage + release-filtered BI views; no raw business rows.

CREATE DATABASE IF NOT EXISTS ads ENGINE = Atomic;

CREATE TABLE IF NOT EXISTS ads.trip_day (
  `release_id` String COMMENT '不可变发布ID',
  `service_date` Date32 COMMENT '纽约开始日期',
  `transport_code` String COMMENT '交通方式',
  `trip_count` Int64 COMMENT '核心有效出发行程数',
  `duration_sum_seconds` Float64 COMMENT '时长指标有效记录秒数总和',
  `duration_count` Int64 COMMENT '时长指标分母',
  `distance_sum_km` Nullable(Float64) COMMENT '出租车有效距离之和；骑行为NULL',
  `distance_count` Nullable(Int64) COMMENT '出租车距离分母；骑行为NULL',
  `origin_mapped_count` Int64 COMMENT '起点匹配纽约区域的行程数',
  `destination_mapped_count` Int64 COMMENT '终点匹配纽约区域的行程数',
  `od_mapped_count` Int64 COMMENT '两端均匹配纽约区域的行程数',
  `source_complete` Bool COMMENT '该开始日期来源覆盖完整',
  `transport_name` String COMMENT '交通方式中文名',
  `average_duration_minutes` Nullable(Float64) COMMENT 'duration_sum_seconds/duration_count/60',
  `average_taxi_distance_km` Nullable(Float64) COMMENT 'distance_sum_km/distance_count；骑行为NULL',
  `origin_mapping_rate` Nullable(Float64) COMMENT 'origin_mapped_count/trip_count，0至1',
  `published_at_utc` DateTime64(6, 'UTC') COMMENT '结果构建完成时刻'
) ENGINE = MergeTree
PARTITION BY (release_id, toYYYYMM(service_date))
ORDER BY (`release_id`, `service_date`, `transport_code`);

CREATE TABLE IF NOT EXISTS ads.zone_hour (
  `release_id` String COMMENT '不可变发布ID',
  `event_date` Date32 COMMENT '纽约事件日期',
  `hour_start_utc` DateTime64(6, 'UTC') COMMENT 'UTC小时唯一标识',
  `zone_key` Int32 COMMENT '纽约区域键',
  `transport_code` String COMMENT '交通方式',
  `departure_count` Nullable(Int64) COMMENT '按开始时刻统计；不完整时NULL',
  `arrival_count` Nullable(Int64) COMMENT '按结束时刻统计；不完整时NULL',
  `departure_complete` Bool COMMENT '出发输入覆盖完整',
  `arrival_complete` Bool COMMENT '到达所需相邻月份及有效时长范围覆盖完整',
  `zone_name` String COMMENT '区域名称',
  `borough` String COMMENT '行政区',
  `net_inflow` Nullable(Int64) COMMENT '仅两端完整时arrival_count-departure_count',
  `published_at_utc` DateTime64(6, 'UTC') COMMENT '结果构建完成时刻'
) ENGINE = MergeTree
PARTITION BY (release_id, toYYYYMM(event_date))
ORDER BY (`release_id`, `hour_start_utc`, `zone_key`, `transport_code`);

CREATE TABLE IF NOT EXISTS ads.od_day (
  `release_id` String COMMENT '不可变发布ID',
  `service_date` Date32 COMMENT '纽约开始日期',
  `transport_code` String COMMENT '交通方式',
  `origin_zone_key` Int32 COMMENT '起区',
  `destination_zone_key` Int32 COMMENT '终区',
  `trip_count` Int64 COMMENT '两端均匹配纽约区域的行程数',
  `origin_zone_name` String COMMENT '起区名称',
  `destination_zone_name` String COMMENT '终区名称',
  `published_at_utc` DateTime64(6, 'UTC') COMMENT '结果构建完成时刻'
) ENGINE = MergeTree
PARTITION BY (release_id, toYYYYMM(service_date))
ORDER BY (`release_id`, `service_date`, `transport_code`, `origin_zone_key`, `destination_zone_key`);

CREATE TABLE IF NOT EXISTS ads.weather_group (
  `release_id` String COMMENT '不可变发布ID',
  `month_start` Date32 COMMENT '月份首日',
  `transport_code` String COMMENT '交通方式',
  `is_weekday` Bool COMMENT '周一至周五',
  `precipitation_group` String COMMENT '降水组',
  `complete_days` Int64 COMMENT '来源与天气均完整的日期数，包括真实零行程日',
  `trip_count` Int64 COMMENT '对应完整日期行程量',
  `daily_average_trips` Nullable(Float64) COMMENT 'trip_count/complete_days',
  `baseline_daily_average` Nullable(Float64) COMMENT '同月同方式同周类型的no_measurable日均量',
  `demand_ratio` Nullable(Float64) COMMENT 'daily_average_trips/baseline_daily_average',
  `published_at_utc` DateTime64(6, 'UTC') COMMENT '结果构建完成时刻'
) ENGINE = MergeTree
PARTITION BY (release_id, toYYYYMM(month_start))
ORDER BY (`release_id`, `month_start`, `transport_code`, `is_weekday`, `precipitation_group`);

CREATE TABLE IF NOT EXISTS ads.quality_batch (
  `release_id` String COMMENT '不可变发布ID',
  `batch_id` String COMMENT '执行尝试ID',
  `source_id` String COMMENT '数据源',
  `source_period` String COMMENT '期间',
  `status` String COMMENT '运行状态',
  `input_rows` Int64 COMMENT '解析成功输入',
  `accepted_rows` Int64 COMMENT '接收',
  `quarantined_rows` Int64 COMMENT '不同记录隔离',
  `duplicate_skipped_rows` Int64 COMMENT '重复跳过',
  `out_of_scope_rows` Int64 COMMENT '范围外',
  `parse_error_rows` Int64 COMMENT '解析失败',
  `acceptance_rate` Nullable(Float64) COMMENT 'accepted_rows/input_rows',
  `last_success_at_utc` Nullable(DateTime64(6, 'UTC')) COMMENT '最近成功时刻',
  `published_at_utc` DateTime64(6, 'UTC') COMMENT '结果构建完成时刻'
) ENGINE = MergeTree
ORDER BY (`release_id`, `batch_id`);

CREATE TABLE IF NOT EXISTS ads.quality_rule (
  `release_id` String COMMENT '不可变发布ID',
  `result_id` String COMMENT '规则执行唯一键',
  `batch_id` String COMMENT '执行批次',
  `table_name` String COMMENT '被检查表名',
  `partition_spec` String COMMENT '分区条件JSON',
  `rule_id` String COMMENT '规则代码',
  `severity` String COMMENT 'block/warn',
  `checked_rows` Int64 COMMENT '规则适用记录量',
  `failed_rows` Int64 COMMENT '失败记录量',
  `passed` Bool COMMENT '是否满足该规则的发布阈值',
  `details_json` String COMMENT '错误范围与样例定位JSON',
  `checked_at_utc` DateTime64(6, 'UTC') COMMENT '检查时刻',
  `published_at_utc` DateTime64(6, 'UTC') COMMENT '结果构建完成时刻'
) ENGINE = MergeTree
ORDER BY (`release_id`, `result_id`);

CREATE TABLE IF NOT EXISTS ads.trip_hour (
  `release_id` String COMMENT '不可变发布ID',
  `service_date` Date32 COMMENT '纽约开始日期',
  `transport_code` String COMMENT '交通方式',
  `trip_count` Int64 COMMENT '核心有效出发行程数',
  `duration_sum_seconds` Float64 COMMENT '时长指标有效记录秒数总和',
  `duration_count` Int64 COMMENT '时长指标分母',
  `distance_sum_km` Nullable(Float64) COMMENT '出租车有效距离之和；骑行为NULL',
  `distance_count` Nullable(Int64) COMMENT '出租车距离分母；骑行为NULL',
  `origin_mapped_count` Int64 COMMENT '起点匹配纽约区域的行程数',
  `destination_mapped_count` Int64 COMMENT '终点匹配纽约区域的行程数',
  `od_mapped_count` Int64 COMMENT '两端均匹配纽约区域的行程数',
  `source_complete` Bool COMMENT '该开始日期来源覆盖完整',
  `transport_name` String COMMENT '交通方式中文名',
  `average_duration_minutes` Nullable(Float64) COMMENT 'duration_sum_seconds/duration_count/60',
  `average_taxi_distance_km` Nullable(Float64) COMMENT 'distance_sum_km/distance_count；骑行为NULL',
  `origin_mapping_rate` Nullable(Float64) COMMENT 'origin_mapped_count/trip_count，0至1',
  `hour_start_utc` DateTime64(6, 'UTC') COMMENT '开始UTC小时，展示时转换纽约时区',
  `published_at_utc` DateTime64(6, 'UTC') COMMENT '结果构建完成时刻'
) ENGINE = MergeTree
PARTITION BY (release_id, toYYYYMM(service_date))
ORDER BY (`release_id`, `hour_start_utc`, `transport_code`);

CREATE TABLE IF NOT EXISTS ads.release_pointer (
  `dataset_id` String COMMENT 'historical/quality',
  `release_id` String COMMENT '当前启用版本',
  `activated_at_utc` DateTime64(6, 'UTC') COMMENT '指针启用时刻'
) ENGINE = MergeTree
ORDER BY (`dataset_id`);

CREATE VIEW IF NOT EXISTS ads.bi_trip_day AS SELECT * FROM ads.trip_day WHERE release_id IN (SELECT release_id FROM ads.release_pointer WHERE dataset_id='historical');

CREATE VIEW IF NOT EXISTS ads.bi_zone_hour AS SELECT * FROM ads.zone_hour WHERE release_id IN (SELECT release_id FROM ads.release_pointer WHERE dataset_id='historical');

CREATE VIEW IF NOT EXISTS ads.bi_od_day AS SELECT * FROM ads.od_day WHERE release_id IN (SELECT release_id FROM ads.release_pointer WHERE dataset_id='historical');

CREATE VIEW IF NOT EXISTS ads.bi_weather_group AS SELECT * FROM ads.weather_group WHERE release_id IN (SELECT release_id FROM ads.release_pointer WHERE dataset_id='historical');

CREATE VIEW IF NOT EXISTS ads.bi_quality_batch AS SELECT * FROM ads.quality_batch WHERE release_id IN (SELECT release_id FROM ads.release_pointer WHERE dataset_id='quality');

CREATE VIEW IF NOT EXISTS ads.bi_quality_rule AS SELECT * FROM ads.quality_rule WHERE release_id IN (SELECT release_id FROM ads.release_pointer WHERE dataset_id='quality');

CREATE VIEW IF NOT EXISTS ads.bi_trip_hour AS SELECT * FROM ads.trip_hour WHERE release_id IN (SELECT release_id FROM ads.release_pointer WHERE dataset_id='historical');
