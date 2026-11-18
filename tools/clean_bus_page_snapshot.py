"""Clean an independently collected local HTML snapshot. No HTTP, crawler or API code."""
from __future__ import annotations
import argparse, collections, csv, datetime as dt, hashlib, html, json, re
from pathlib import Path
from html.parser import HTMLParser
ROOT=Path(__file__).resolve().parents[1]
CHAT='2026-11-18-theme-definition-e721eb90'
TIME_RE=re.compile(r'^(\d{1,2}):(\d{2})([ps])?$')
DAY_MARKS={'⁺¹':1,'⁺²':2,'⁺³':3}
GAPS={'','-','–','—','↓','↑','⋮'}
def parse_time(raw):
    value=''.join(raw.split())
    if value in GAPS:return None
    day=0
    for mark,offset in DAY_MARKS.items():
        if mark in value:
            if value.count(mark)!=1 or day:raise ValueError('Ambiguous day marker')
            day=offset;value=value.replace(mark,'')
    m=TIME_RE.fullmatch(value)
    if not m:raise ValueError('Unrecognized source time')
    hour,minute=int(m[1]),int(m[2])
    if hour>23 or minute>59:raise ValueError('Out-of-range clock time')
    restriction=m[3] or ''
    return {'clock_time':f'{hour:02}:{minute:02}','day_offset':day,'service_minutes':day*1440+hour*60+minute,'pickup_allowed':restriction!='s','setdown_allowed':restriction!='p','restriction':restriction}
class IndexStops(HTMLParser):
    def __init__(self):super().__init__();self.ids=set()
    def handle_starttag(self,tag,attrs):
        if tag=='a':
            href=dict(attrs).get('href','')
            if re.fullmatch(r'/stops/[A-Za-z0-9]+',href):self.ids.add(href.split('/')[-1])
def write_csv(path,rows,fields):
    with path.open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rows)
def clean_snapshot(run_dir:Path,output:Path):
    manifest=json.loads((run_dir/'crawl-manifest.json').read_text(encoding='utf-8'))
    output.mkdir(parents=True,exist_ok=True)
    index_entry=next(x for x in manifest['requests'] if x['kind']=='index' and x['status']==200)
    index_parser=IndexStops();index_parser.feed((run_dir/index_entry['body_file']).read_text(encoding='utf-8'))
    calls=[];stop_rows=[];rejects=[];routes=[];keys=set();observed_dates=collections.Counter()
    for source in manifest['successful_pages']:
        page=json.loads((run_dir/source['parsed_file']).read_text(encoding='utf-8'))
        raw=(run_dir/page['source_response_file']).read_bytes()
        if hashlib.sha256(raw).hexdigest()!=page['source_response_sha256']:raise ValueError('HTML snapshot SHA mismatch')
        date=page.get('schedule_date')
        if date:
            dt.date.fromisoformat(date)
            schedule_key=date
        elif page.get('calendar_label'):
            schedule_key='calendar: '+page['calendar_label']
        else:raise ValueError('Neither explicit date nor calendar label available; refusing to infer today')
        observed_dates[schedule_key]+=1
        operator_names=' | '.join(x['name'] for x in page['operators'])
        base={'snapshot_run_id':manifest['run_id'],'schedule_date':date,'calendar_id':page.get('calendar_id'),'calendar_label':page.get('calendar_label'),'schedule_scope':page.get('schedule_scope'),'service_slug':page['service_slug'],'route_name':page['route_name'],'description':page['description'],'operators':operator_names,'source_url':page['source_url'],'fetched_at_utc':page['fetched_at_utc'],'source_response_sha256':page['source_response_sha256']}
        route_calls=[]
        for table in page['tables']:
            for row in table['rows']:
                rowbase={**base,'table_index':table['table_index'],'direction_label':table['direction_label'],'stop_sequence':row['stop_sequence'],'source_row_index':row['source_row_index'],'stop_id':row['stop_id'],'stop_name':row['stop_name'],'stop_listed_in_seed_locality':row['stop_id'] in index_parser.ids}
                stop_rows.append(rowbase)
                for cell in row['cells']:
                    if cell['raw_text'] in GAPS:continue
                    if cell['column_span']!=1 or cell['row_span']!=1:
                        rejects.append({**rowbase,'source_column':cell['column_start'],'raw_time':cell['raw_text'],'column_span':cell['column_span'],'row_span':cell['row_span'],'reason':'Non-unit span: retain raw, do not invent individual calls'});continue
                    try:parsed=parse_time(cell['raw_text'])
                    except ValueError as e:
                        rejects.append({**rowbase,'source_column':cell['column_start'],'raw_time':cell['raw_text'],'reason':str(e)});continue
                    if parsed is None:continue
                    item={**rowbase,'source_column':cell['column_start'],'raw_time':cell['raw_text'],**parsed}
                    key=(manifest['run_id'],page['service_slug'],table['table_index'],row['stop_sequence'],cell['column_start'])
                    if key in keys:raise ValueError('Duplicate fact grain')
                    keys.add(key);calls.append(item);route_calls.append(item)
        firsts={}
        for c in route_calls:
            if not c['pickup_allowed']:continue
            key=(c['table_index'],c['source_column'])
            if key not in firsts or c['stop_sequence']<firsts[key]['stop_sequence']:firsts[key]=c
        first_list=list(firsts.values());minutes=[x['service_minutes'] for x in first_list]
        routes.append({**base,'direction_tables':len(page['tables']),'route_stop_rows':sum(len(t['rows']) for t in page['tables']),'unique_stops':len({c['stop_id'] for c in route_calls}),'planned_time_cells':len(route_calls),'first_listed_pickup_columns':len(first_list),'night_first_pickup_columns':sum(x['service_minutes']%1440>=22*60 or x['service_minutes']%1440<6*60 for x in first_list),'first_listed_pickup_minute':min(minutes) if minutes else None,'last_listed_pickup_minute':max(minutes) if minutes else None})
    fields=list(calls[0]) if calls else []
    if not calls:raise ValueError('No validated planned calls')
    write_csv(output/'planned-stop-times.csv',calls,fields)
    write_csv(output/'route-stop-snapshot.csv',stop_rows,list(stop_rows[0]))
    write_csv(output/'route-summary.csv',routes,list(routes[0]))
    write_csv(output/'quarantined-cells.csv',rejects,list(rejects[0]) if rejects else ['service_slug','raw_time','reason'])
    write_csv(output/'planned-stop-times-sample.csv',calls[:30],fields)
    write_csv(output/'route-stop-snapshot-sample.csv',stop_rows[:30],list(stop_rows[0]))
    unique_stops={c['stop_id'] for c in calls}
    summary={'checked_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'passed':True,'scope':'Locality-page-discovered bus/coach timetable HTML snapshot, not observed passenger travel and not an entire-city completeness claim','run_id':manifest['run_id'],'source_seed_url':manifest['seed_url'],'discovered_service_links':len(manifest['discovered_service_urls']),'successful_service_pages':manifest['successful_service_pages'],'failed_service_pages':manifest['failed_service_pages'],'selection_truncated':manifest.get('selection_truncated'),'crawl_complete_for_selected_links':manifest['passed'],'schedule_dates_and_calendars':dict(observed_dates),'same_explicit_date_2026_11_18_pages':sum(r['schedule_date']=='2026-11-18' for r in routes),'same_explicit_date_2026_11_18_cells':sum(c['schedule_date']=='2026-11-18' for c in calls),'route_stop_rows':len(stop_rows),'planned_time_cells':len(calls),'source_nonempty_cells':manifest['nonempty_time_cells'],'raw_cell_reconciliation_passed':len(calls)+len(rejects)==manifest['nonempty_time_cells'],'quarantined_nonempty_cells':len(rejects),'unique_stop_ids_all_routes_including_outside_city':len(unique_stops),'unique_stop_ids_listed_on_seed_locality':len(index_parser.ids),'observed_seed_locality_stop_ids':len(unique_stops&index_parser.ids),'source_sha256_checks':len(routes),'fact_grain_duplicate_count':0,'pickup_only_cells':sum(c['restriction']=='p' for c in calls),'setdown_only_cells':sum(c['restriction']=='s' for c in calls),'after_midnight_cells':sum(c['day_offset']>0 for c in calls),'bulk_data_file_downloads':0,'api_requests':0,'python_network_requests':0,'time_unit_note':'A cell is a planned stop time. Multiple stop times in one timetable column are never counted as multiple passenger trips. Column identity is local to one page/direction/snapshot; no real vehicle or trip ID is asserted.','files':{}}
    for file in sorted(output.glob('*.csv')):summary['files'][file.name]={'bytes':file.stat().st_size,'sha256':hashlib.sha256(file.read_bytes()).hexdigest()}
    (output/'cleaning-verification.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    def cells(values):return ''.join('<td>'+html.escape(str(v))+'</td>' for v in values)
    route_table=''.join('<tr>'+cells([r['route_name'],r['description'],r['schedule_date'] or r['calendar_label'],r['operators'],r['direction_tables'],r['unique_stops'],r['first_listed_pickup_columns'],r['planned_time_cells']])+'</tr>' for r in routes)
    preview=''.join('<tr>'+cells([r['route_name'],r['direction_label'],r['stop_name'],r['raw_time'],r['schedule_date'] or r['calendar_label']])+f'<td><a href="{html.escape(r["source_url"])}">来源页面</a></td></tr>' for r in calls[:12])
    document='''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>业务页面真实爬取试验</title><style>body{font:16px/1.7 "Microsoft YaHei",sans-serif;margin:0;background:#f0f4f6;color:#213945}main{max-width:1180px;margin:24px auto;padding:30px;background:white}h1,h2{color:#123f50}.notice{background:#fff4dc;padding:16px;border-left:4px solid #d99a30}.stats{display:flex;flex-wrap:wrap;gap:20px;margin:24px 0}.stat{padding:18px;background:#edf5f5;flex:1;min-width:150px}.stat strong{display:block;font-size:30px;color:#156b6d}table{border-collapse:collapse;min-width:760px;width:100%;font-size:13px}td,th{border:1px solid #d3dfe3;padding:9px;text-align:left}th{background:#e7f1f3}.scroll{overflow-x:auto}a{color:#146d91;overflow-wrap:anywhere}@media(max-width:600px){main{padding:16px;margin:0}h1{font-size:25px}}</style><main><h1>城市公共交通业务页面采集试验</h1><p>来源：Bus Times 的 Cambridge 地点列表与其公开线路时刻详情。采集器只请求并解析 HTML，没有获取现成 CSV、ZIP、Parquet、GTFS 或 API。</p><div class="notice">这是新数据源试验，尚未替换纽约历史行程数仓。数据表达公布的计划供给，不能推出客流、满载率、准点率或资金投入效果。线路可能跨城；列表范围不等于行政区全量覆盖。</div>'''
    document+='<div class="stats">'+''.join(f'<div class="stat"><strong>{n:,}</strong>{label}</div>' for n,label in [(len(routes),'成功解析的线路详情页'),(len(stop_rows),'线路—方向—站点行'),(len(calls),'有效计划停靠时刻单元格'),(len(unique_stops),'涉及站点ID（含城外）')])+'</div>'
    document+='<h2>采集与核验</h2><p>页面日期／服务日历（须分别分析，不混成同一天）：'+html.escape(', '.join(observed_dates))+f'。发现 {len(manifest["discovered_service_urls"])} 条详情链接；成功 {len(routes)}，失败 {manifest["failed_service_pages"]}。隔离非空单元格 {len(rejects)} 个；跨午夜时刻 '+str(summary['after_midnight_cells'])+' 个已按页面标记保留日偏移。</p><p><a href="planned-stop-times-sample.csv">30条时刻样例CSV</a> · <a href="route-stop-snapshot-sample.csv">30条站点样例CSV</a> · <a href="route-summary.csv">线路汇总CSV</a> · <a href="cleaning-verification.json">本地清洗验证</a></p>'
    document+='<h2>适合管理者的三个分析主题</h2><ol><li>服务供给结构：不同运营方与线路的列示计划规模如何分布？</li><li>时段供给短板：已列示服务的早晚及夜间覆盖有哪些差异？需要补充多日快照后比较周类型。</li><li>站点服务依赖：哪些列示站点依赖较少线路？用同一站点ID比较，不能把站名相似当作同一站，也不能代替人口可达性。</li></ol><h2>实际字段样例</h2><div class="scroll"><table><thead><tr><th>线路</th><th>方向</th><th>站点</th><th>原时刻</th><th>页面适用日</th><th>追溯</th></tr></thead><tbody>'+preview+'</tbody></table></div>'
    document+='<h2>本次线路清单</h2><div class="scroll"><table><thead><tr><th>线路名</th><th>描述</th><th>页面适用日</th><th>运营方</th><th>方向表</th><th>站点ID数</th><th>首个可上车时刻列数</th><th>计划时刻单元格数</th></tr></thead><tbody>'+route_table+'</tbody></table></div><p>完整逐条CSV保留在本机同目录，公开仓库只提供30条样例和线路汇总；没有将第三方完整HTML重新发布。原始HTML及其SHA保存在本轮独立work目录。</p></main></html>'
    (output/'index.html').write_text(document,encoding='utf-8')
    return summary
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input-run',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args();print(json.dumps(clean_snapshot(a.input_run,a.output),ensure_ascii=False,indent=2))
