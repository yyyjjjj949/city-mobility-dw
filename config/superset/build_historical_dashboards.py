"""Idempotently register BI views and native Superset charts; no source data copied."""
import json
import uuid
from pathlib import Path
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import db
    from superset.extensions import appbuilder
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.connectors.sqla.models import SqlaTable, SqlMetric
    from flask import g
    admin = appbuilder.sm.find_user(username='admin')
    g.user = admin
    database = db.session.query(Database).filter_by(database_name='City Mobility ClickHouse').one()
    datasets = {}
    report = {'datasets': [], 'charts': [], 'dashboards': []}
    labels = {'taxi_yellow': '黄色出租车', 'citibike': '共享单车'}

    def dataset(name, sql=None, date=None):
        table = db.session.query(SqlaTable).filter_by(database_id=database.id, schema='ads', table_name=name).one_or_none()
        if table is None:
            table = SqlaTable(table_name=name, schema='ads', database=database, owners=[admin])
            db.session.add(table); db.session.flush()
        table.sql = sql; table.main_dttm_col = date; table.filter_select_enabled = True
        table.fetch_metadata(); table.main_dttm_col = date
        table.cache_timeout = 0
        db.session.flush()
        datasets[name] = table
        report['datasets'].append({'name': name, 'id': table.id, 'virtual': bool(sql)})
        return table

    day = dataset('bi_trip_day', date='service_date')
    hour = dataset('bi_trip_hour', date='service_date')
    zone = dataset('bi_zone_hour', date='event_date')
    od = dataset('bi_od_day', date='service_date')
    weather = dataset('bi_weather_group', date='month_start')
    quality = dataset('bi_quality_batch')
    rules = dataset('bi_quality_rule')
    profile = dataset('city_hour_profile', """SELECT service_date,toHour(toTimeZone(hour_start_utc,'America/New_York')) AS local_hour,
        transport_name,transport_code,trip_count FROM ads.bi_trip_hour""",date='service_date')
    weather_view = dataset('city_weather_compare', """SELECT *,if(transport_code='taxi_yellow','黄色出租车','共享单车') AS transport_name,
        if(is_weekday,'周一至周五','周末') AS week_type,
        multiIf(precipitation_group='measurable','可测降水',precipitation_group='trace','微量降水','未记录可测降水') AS weather_label
        FROM ads.bi_weather_group""", date='month_start')
    quality_view = dataset('city_quality_summary', """SELECT *,if(source_id='tlc_yellow','黄色出租车','共享单车') AS transport_name
        FROM ads.bi_quality_batch""")
    od_view = dataset('city_od_ranking', """SELECT *,concat(origin_zone_name,' → ',destination_zone_name) AS route,
        if(transport_code='taxi_yellow','黄色出租车','共享单车') AS transport_name FROM ads.bi_od_day""", date='service_date')
    coverage = dataset('city_mapping_coverage', """SELECT service_date,transport_code,transport_name,trip_count,
        origin_mapped_count,destination_mapped_count,od_mapped_count FROM ads.bi_trip_day""",date='service_date')

    def metric(sql, label):
        return {'expressionType': 'SQL', 'sqlExpression': sql, 'label': label, 'optionName': 'metric_' + uuid.uuid5(uuid.NAMESPACE_URL, sql + label).hex, 'hasCustomLabel': True}

    def filt(column, value):
        return {'expressionType': 'SIMPLE', 'subject': column, 'operator': '==', 'comparator': value, 'clause': 'WHERE', 'isExtra': False, 'sqlExpression': None}

    def chart(slug, title, table, viz, metrics, columns=None, group=None, filters=None, **extra):
        chart_uuid = uuid.uuid5(uuid.NAMESPACE_URL, 'city-mobility-dw/chart/' + slug)
        item = db.session.query(Slice).filter_by(uuid=chart_uuid).one_or_none()
        if item is None:
            item = Slice(uuid=chart_uuid, slice_name=title, datasource_id=table.id, datasource_type='table', owners=[admin]); db.session.add(item); db.session.flush()
        params = dict(datasource=f'{table.id}__table', viz_type=viz, slice_id=item.id, time_range='No filter',
                      adhoc_filters=filters or [], row_limit=10000, color_scheme='supersetColors', show_legend=True,
                      rich_tooltip=True, legendOrientation='top', legendType='scroll', truncateXAxis=True, x_axis_label='', y_axis_label='',
                      contributionMode=None)
        cols = columns or []
        group = group or []
        if viz == 'big_number_total': params.update(metric=metrics[0], subheader=extra.pop('subheader', ''), y_axis_format=',d', header_font_size=.34)
        elif viz == 'table': params.update(query_mode='aggregate', groupby=cols, metrics=metrics, order_desc=True, all_columns=[], percent_metrics=[], page_length=20, server_pagination=False, table_timestamp_format='smart_date')
        else:
            params.update(x_axis=cols[0], metrics=metrics, groupby=group, time_grain_sqla=None, x_axis_time_format='%m-%d', y_axis_format=',d',
                          order_desc=False, x_axis_sort_asc=True, show_value=False, show_marker=True, markerSize=5,
                          truncateYAxis=False, tooltipTimeFormat='%Y-%m-%d', zoomable=False, stack=False)
        params.update(extra)
        if table.main_dttm_col:
            params['granularity_sqla'] = table.main_dttm_col
        query = dict(columns=cols + group, metrics=metrics, filters=[{'col': f['subject'], 'op': f['operator'], 'val': f['comparator']} for f in (filters or [])],
                     extras={'having': '', 'where': ''}, applied_time_extras={}, row_limit=10000, order_desc=False,
                     time_range='No filter', annotation_layers=[])
        if table.main_dttm_col:
            query['granularity'] = table.main_dttm_col
        if viz == 'big_number_total': query['columns'] = []
        if viz == 'table' or (viz != 'big_number_total' and params.get('order_desc')):
            query.update(row_limit=params['row_limit'], orderby=[[metrics[0], False]])
        elif cols:
            query['orderby'] = [[cols[0], True]]
        if viz not in ('big_number_total', 'table'):
            query['post_processing'] = [{'operation': 'pivot', 'options': {
                'index': cols, 'columns': group, 'aggregates': {m['label']: {'operator': 'mean'} for m in metrics},
                'drop_missing_columns': True}}, {'operation': 'flatten'}]
        item.slice_name=title; item.viz_type=viz; item.datasource_id=table.id; item.datasource_type='table'; item.datasource_name=table.table_name
        item.cache_timeout=0; item.params=json.dumps(params, ensure_ascii=False)
        item.query_context=json.dumps({'datasource': {'id': table.id, 'type': 'table'}, 'force': True, 'queries': [query], 'form_data': params, 'result_format': 'json', 'result_type': 'full'}, ensure_ascii=False)
        db.session.flush()
        report['charts'].append({'id': item.id, 'name': title, 'dataset': table.table_name, 'url': f'http://localhost:8088/explore/?slice_id={item.id}'})
        return item

    count = metric('SUM(trip_count)', '行程数')
    duration = metric('SUM(duration_sum_seconds)/NULLIF(SUM(duration_count),0)/60', '平均时长（分钟）')
    overview = [
        chart('overview-total','已接入两类有效行程',day,'big_number_total',[count],subheader='2025 年 6 月 · 按纽约开始日期'),
        chart('overview-taxi','黄色出租车行程',day,'big_number_total',[count],filters=[filt('transport_code','taxi_yellow')]),
        chart('overview-bike','共享单车骑行',day,'big_number_total',[count],filters=[filt('transport_code','citibike')]),
        chart('overview-daily','每日出发行程趋势',day,'echarts_timeseries_line',[count],['service_date'],['transport_name']),
        chart('overview-hour','纽约当地小时 · 日均出发量',profile,'echarts_timeseries_bar',[metric('SUM(trip_count)/NULLIF(COUNT(DISTINCT service_date),0)','日均行程数')],['local_hour'],['transport_name'],x_axis_time_format='smart_date',x_axis_title='纽约当地小时',tooltipTimeFormat='smart_date'),
        chart('overview-duration','每日平均行程时长',day,'echarts_timeseries_line',[duration],['service_date'],['transport_name'],y_axis_format='.2f'),
        chart('overview-distance','每日出租车平均距离（有效距离记录）',day,'echarts_timeseries_line',[metric('SUM(distance_sum_km)/NULLIF(SUM(distance_count),0)','平均距离（千米）')],['service_date'],filters=[filt('transport_code','taxi_yellow')],y_axis_format='.2f'),
    ]
    spatial = [
        chart('spatial-departures','出发量最多的 15 个纽约区域',zone,'echarts_timeseries_bar',[metric('SUM(departure_count)','区域出发量')],['zone_name'],row_limit=15,order_desc=True,x_axis_sort_asc=False,x_axis_sort='区域出发量',orientation='horizontal',show_value=True),
        chart('spatial-borough','各行政区出发量',zone,'echarts_timeseries_bar',[metric('SUM(departure_count)','区域出发量')],['borough'],['transport_code']),
        chart('spatial-od','热门 OD · 前 20 条',od_view,'table',[count],['route','transport_name'],row_limit=20,order_by_cols=[],order_desc=True),
        chart('spatial-coverage','区域匹配覆盖率',coverage,'table',[metric('SUM(origin_mapped_count)/NULLIF(SUM(trip_count),0)','起点覆盖率'),metric('SUM(destination_mapped_count)/NULLIF(SUM(trip_count),0)','终点覆盖率'),metric('SUM(od_mapped_count)/NULLIF(SUM(trip_count),0)','双端覆盖率')],['transport_name'],column_config={k:{'d3NumberFormat':'.2%'} for k in ['起点覆盖率','终点覆盖率','双端覆盖率']}),
    ]
    weather_charts = [
        chart('weather-weekdays','周一至周五 · 不同降水组的日均出行量',weather_view,'echarts_timeseries_bar',[metric('SUM(daily_average_trips)','日均行程数')],['weather_label'],['transport_name'],filters=[filt('is_weekday',True)]),
        chart('weather-weekend','周末 · 不同降水组的日均出行量',weather_view,'echarts_timeseries_bar',[metric('SUM(daily_average_trips)','日均行程数')],['weather_label'],['transport_name'],filters=[filt('is_weekday',False)]),
        chart('weather-samples','天气对比样本与需求比率',weather_view,'table',[metric('SUM(complete_days)','完整样本天数'),metric('SUM(daily_average_trips)','日均行程数'),metric('MAX(demand_ratio)','相对无可测降水日比率')],['transport_name','week_type','weather_label'],row_limit=30,column_config={'日均行程数':{'d3NumberFormat':',.0f'},'相对无可测降水日比率':{'d3NumberFormat':'.3f'}}),
    ]
    quality_charts = [
        chart('quality-input','原始行程输入',quality,'big_number_total',[metric('SUM(input_rows)','原始行程数')],subheader='仅展示本次历史 DWD 的两个有效来源批次'),
        chart('quality-isolation','异常隔离记录',quality,'big_number_total',[metric('SUM(quarantined_rows)','隔离量')]),
        chart('quality-out-of-scope','六月范围外记录',quality,'big_number_total',[metric('SUM(out_of_scope_rows)','范围外量')]),
        chart('quality-destinations','两类来源的记录去向',quality_view,'echarts_timeseries_bar',[metric('SUM(accepted_rows)','有效行程'),metric('SUM(quarantined_rows)','异常隔离'),metric('SUM(out_of_scope_rows)','范围外')],['transport_name'],stack=True),
        chart('quality-rules','清洗质量规则与警告',rules,'table',[metric('SUM(checked_rows)','检查量'),metric('SUM(failed_rows)','规则失败记录量')],['table_name','rule_id','severity','passed'],row_limit=100),
    ]

    def dashboard(slug,title,charts,rows,note):
        dashboard = db.session.query(Dashboard).filter_by(slug=slug).one_or_none()
        if dashboard is None: dashboard=Dashboard(slug=slug, dashboard_title=title, owners=[admin]); db.session.add(dashboard); db.session.flush()
        pos={'DASHBOARD_VERSION_KEY':'v2','ROOT_ID':{'id':'ROOT_ID','type':'ROOT','children':['GRID_ID']},
             'GRID_ID':{'id':'GRID_ID','type':'GRID','parents':['ROOT_ID'],'children':['NOTE_ROW']},
             'HEADER_ID':{'id':'HEADER_ID','type':'HEADER','meta':{'text':title}},
             'NOTE_ROW':{'id':'NOTE_ROW','type':'ROW','parents':['ROOT_ID','GRID_ID'],'children':['NOTE'],'meta':{'background':'BACKGROUND_TRANSPARENT'}},
             'NOTE':{'id':'NOTE','type':'MARKDOWN','parents':['ROOT_ID','GRID_ID','NOTE_ROW'],'children':[], 'meta':{'code':note,'width':12,'height':16}}}
        for i,indices in enumerate(rows):
            rowid='ROW-'+str(i); pos['GRID_ID']['children'].append(rowid)
            pos[rowid]={'id':rowid,'type':'ROW','parents':['ROOT_ID','GRID_ID'],'children':[],'meta':{'background':'BACKGROUND_TRANSPARENT'}}
            for j,index in enumerate(indices):
                c=charts[index]; cid='CHART-'+str(c.id); pos[rowid]['children'].append(cid)
                pos[cid]={'id':cid,'type':'CHART','parents':['ROOT_ID','GRID_ID',rowid],'children':[], 'meta':{'chartId':c.id,'sliceName':c.slice_name,'uuid':str(c.uuid),'width':12//len(indices),'height':28 if c.viz_type=='big_number_total' else 52}}
        dashboard.dashboard_title=title; dashboard.published=True; dashboard.slices=charts; dashboard.position_json=json.dumps(pos,ensure_ascii=False)
        native=[]
        def native_select(key,name,column,tables,excluded=None):
            native.append({'id':'NATIVE_FILTER-'+slug+'-'+key,'name':name,'filterType':'filter_select',
                'targets':[{'datasetId':t.id,'column':{'name':column}} for t in tables],
                'defaultDataMask':{'extraFormData':{},'filterState':{},'ownState':{}},'cascadeParentIds':[],
                'scope':{'rootPath':['ROOT_ID'],'excluded':excluded or []},
                'controlValues':{'enableEmptyFilter':False,'defaultToFirstItem':False,'multiSelect':True,'searchAllOptions':False,'inverseSelection':False},
                'type':'NATIVE_FILTER','description':'仅影响作用范围内图表；不选择时显示全部。'})
        if slug in ['city-mobility-overview','city-mobility-spatial']:
            native.append({'id':'NATIVE_FILTER-'+slug+'-date','name':'出发日期范围','filterType':'filter_time',
                'targets':[{}],'defaultDataMask':{'extraFormData':{},'filterState':{},'ownState':{}},
                'cascadeParentIds':[],'scope':{'rootPath':['ROOT_ID'],'excluded':[]},'type':'NATIVE_FILTER',
                'controlValues':{},'description':'按纽约出发日期；自定义范围的结束时刻不包含在内。仅接入2025年6月。'})
            native_select('transport','交通方式','transport_code', [day,profile] if slug.endswith('overview') else [zone,od_view,coverage])
        if slug=='city-mobility-spatial':
            native_select('borough','出发行政区（区域图）','borough',[zone],excluded=[spatial[2].id,spatial[3].id])
        if slug=='city-mobility-weather':
            native_select('transport','交通方式','transport_code',[weather_view])
        dashboard.json_metadata=json.dumps({'color_scheme':'supersetColors','refresh_frequency':0,'timed_refresh_immune_slices':[], 'expanded_slices':{},'native_filter_configuration':native,'cross_filters_enabled':False},ensure_ascii=False)
        dashboard.css='.dashboard-content { background: #f3f6fa; } .dashboard-markdown { line-height: 1.75; }'
        db.session.flush(); report['dashboards'].append({'id':dashboard.id,'title':title,'slug':slug,'url':'http://localhost:8088/superset/dashboard/'+slug+'/'})

    shared='**2025 年 6 月｜纽约当地时间** · 真实完整月度数据 · Yellow Taxi + Citi Bike。合计表示已接入两类记录，不能解释为全市总出行量。'
    dashboard('city-mobility-overview','城市出行 · 历史总览',overview,[[0,1,2],[3],[4],[5,6]],shared+'\n\n支持日期、交通方式筛选。小时图为各小时选中日期总量 ÷ 选中有效日期数；平均时长、距离均用有效分子分母计算。固定方式KPI与交通方式筛选取交集，无记录显示空值。')
    dashboard('city-mobility-spatial','城市出行 · 区域与 OD',spatial,[[0],[1],[2],[3]],shared+'\n\n[打开地图与联动筛选](/city-mobility/map/) · 支持日期、方式、行政区及区域选择、CSV导出。\n\n看板日期与交通方式影响全部图表；行政区仅影响区域出发图，不改变OD或全局覆盖率。完整事件到达量和净流入暂不发布；区域与 OD 仅计符合空间条件的记录。')
    dashboard('city-mobility-weather','城市出行 · 天气对比',weather_charts,[[0],[1],[2]],shared+'\n\n以 NOAA 中央公园日天气为背景，按周一至周五/周末分组。微量降水独立展示；基准为同月、同方式、同周类型的无可测降水日。样本量有限，比较不表示因果。')
    dashboard('city-mobility-quality','城市出行 · 数据质量',quality_charts,[[0,1,2],[3],[4]],shared+'\n\n输入＝有效＋不同隔离原记录＋重复跳过＋范围外。字段警告可重叠，不能相加；规则失败记录量不等于最终隔离量。`passed` 表示满足该规则的门禁/处理策略，警告或已隔离异常可非零。')
    db.session.commit()
    Path('/app/superset_home/historical-dashboards-result.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print('SUPERSET_HISTORICAL_OK '+json.dumps({'datasets':len(report['datasets']),'charts':len(report['charts']),'dashboards':len(report['dashboards'])}),flush=True)
