"""Read pinned historical Delta snapshots and reproduce twelve subject analyses.
No Docker is required. Input files, exact versions and SHA-256 are recorded.
Tested dependencies: pandas 3.0.3, pyarrow 23.0.1, matplotlib 3.11.0.
For a fresh Python: pip install pandas==3.0.3 pyarrow==23.0.1 matplotlib==3.11.0
"""
from __future__ import annotations
import argparse, datetime as dt, hashlib, html, json, os
from pathlib import Path
from urllib.parse import unquote
import pandas as pd
import pyarrow.parquet as pq
ROOT=Path(__file__).resolve().parents[1]
MODES={'taxi_yellow':'黄色出租车','citibike':'共享单车'}
WEATHER={'no_measurable':'未记录可测降水','trace':'微量降水','measurable':'可测降水'}
ORDER=list(WEATHER)
def checksum(path):
    with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def safe_ratio(n,d):return None if d is None or d<=0 else float(n)/float(d)
def weighted_mean(frame,n,d,scale=1):
    value=safe_ratio(frame[n].sum(),frame[d].sum())
    return None if value is None else value/scale
def active_files(base,version):
    active={}
    for number in range(version+1):
        log=Path(base)/'_delta_log'/f'{number:020}.json'
        if not log.is_file():raise ValueError('Missing complete Delta JSON logs')
        for line in log.read_text(encoding='utf-8').splitlines():
            action=json.loads(line)
            if 'remove' in action:active.pop(action['remove']['path'],None)
            if 'add' in action:active[action['add']['path']]=action['add']
    if not active:raise ValueError('Empty pinned snapshot')
    return active
def read_snapshot(root,table,version,release=None):
    base=Path(root)/'data'/table.replace('.','/');parts=[];files=[]
    for relative,action in sorted(active_files(base,version).items()):
        path=(base/unquote(relative)).resolve()
        if not path.is_relative_to(base.resolve()):raise ValueError('Delta file escapes table')
        if path.stat().st_size!=action['size']:raise ValueError('Pinned file size mismatch')
        frame=pq.ParquetFile(path).read().to_pandas()
        for key,value in action.get('partitionValues',{}).items():
            if key not in frame:frame[key]=value
        parts.append(frame);files.append({'path':path.relative_to(root).as_posix(),'bytes':path.stat().st_size,'sha256':checksum(path)})
    frame=pd.concat(parts,ignore_index=True)
    if release is not None:
        if 'release_id' not in frame:raise ValueError('Missing release_id')
        frame=frame.loc[frame.release_id==release].copy()
        if frame.empty:raise ValueError('Validated release not present')
    return frame,{'table':table,'delta_version':version,'rows':len(frame),'files':files}
def select_period(frame,column,dates=None,mode=None):
    result=frame.copy()
    if dates is not None:result=result.loc[result[column].astype(str).str[:10].isin({str(d)[:10] for d in dates})]
    if mode is not None:result=result.loc[result.transport_code==mode]
    return result.copy()
def validate_day_hour(day,hour,dates):
    expected=set(dates)
    for name,frame in [('day',day),('hour',hour)]:
        if not frame.source_complete.eq(True).all():raise ValueError('Incomplete '+name)
        if set(frame.transport_code)!=set(MODES):raise ValueError('Unexpected or missing transport modes')
    for mode in MODES:
        d=day.loc[day.transport_code==mode];h=hour.loc[hour.transport_code==mode]
        if len(d)!=len(expected) or set(d.service_date)!=expected or d.duplicated(['service_date','transport_code']).any():raise ValueError('Missing/duplicate daily dates')
        if len(h)!=24*len(expected) or h.duplicated(['hour_start_utc','transport_code']).any():raise ValueError('Missing/duplicate hourly scaffold')
        local=pd.to_datetime(h.hour_start_utc,utc=True).dt.tz_convert('America/New_York')
        if list(local.dt.strftime('%Y-%m-%d'))!=list(h.service_date):raise ValueError('UTC/local dates disagree')
        if not h.assign(local_hour=local.dt.hour).groupby('service_date').local_hour.nunique().eq(24).all():raise ValueError('Only complete 24-hour June dates supported')
        a=d.set_index('service_date').trip_count;b=h.groupby('service_date').trip_count.sum()
        if any(int(a.loc[v])!=int(b.loc[v]) for v in expected):raise ValueError('Day/hour totals disagree')
def hour_profile(hour,by_week=False):
    if not hour.source_complete.eq(True).all():raise ValueError('Incomplete hourly input')
    f=hour.copy();local=pd.to_datetime(f.hour_start_utc,utc=True).dt.tz_convert('America/New_York');f['local_hour']=local.dt.hour;f['week_type']=local.dt.dayofweek.map(lambda d:'周一至周五' if d<5 else '周末');keys=['transport_code']+(['week_type'] if by_week else [])
    result=f.groupby(keys+['local_hour'],as_index=False).trip_count.sum();days=f.groupby(keys,as_index=False).agg(complete_days=('service_date','nunique'));result=result.merge(days,on=keys,validate='many_to_one');result['daily_average_trips']=result.trip_count/result.complete_days;totals=result.groupby(keys).trip_count.transform('sum');result['within_mode_share']=result.trip_count/totals.where(totals>0)
    return result
def ranked_od(frame,mode,dates=None,limit=20):
    selected=select_period(frame,'service_date',dates,mode);keys=['origin_zone_key','destination_zone_key','origin_zone_name','destination_zone_name'];g=selected.groupby(keys,as_index=False).trip_count.sum().sort_values(['trip_count','origin_zone_key','destination_zone_key'],ascending=[False,True,True],kind='stable');top=g.head(limit).copy();den=int(g.trip_count.sum())
    return top,{'all_matched_od_trips':den,'top20_trips':int(top.trip_count.sum()),'top20_concentration':safe_ratio(top.trip_count.sum(),den),'observed_od_pairs':len(g)}
def weather_metrics(weather):
    f=weather.copy();keys=['month_start','transport_code','is_weekday']
    if f.duplicated(keys+['precipitation_group']).any():raise ValueError('Duplicate weather grain')
    base=f.loc[f.precipitation_group=='no_measurable',keys+['complete_days','daily_average_trips']].rename(columns={'complete_days':'baseline_days','daily_average_trips':'recomputed_baseline'});f=f.merge(base,on=keys,how='left',validate='many_to_one');f['recomputed_ratio']=f.daily_average_trips/f.recomputed_baseline.where(f.recomputed_baseline>0);f['change_percent']=(f.recomputed_ratio-1)*100;f['week_type']=f.is_weekday.map({True:'周一至周五',False:'周末'});f['weather_label']=f.precipitation_group.map(WEATHER)
    return f
def portable(value):
    if isinstance(value,dict):return {str(k):portable(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [portable(v) for v in value]
    if hasattr(value,'item'):return portable(value.item())
    if isinstance(value,(dt.datetime,dt.date,pd.Timestamp)):return value.isoformat()
    if isinstance(value,float) and pd.isna(value):return None
    return value
def records(frame):return portable(frame.to_dict('records'))
def draw_charts(data,out,work):
    os.environ.setdefault('MPLCONFIGDIR',str(work/'matplotlib-cache'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib import font_manager
    for candidate in ['C:/Windows/Fonts/msyh.ttc','C:/Windows/Fonts/simhei.ttf']:
        if Path(candidate).exists():font_manager.fontManager.addfont(candidate);plt.rcParams['font.family']=font_manager.FontProperties(fname=candidate).get_name();break
    plt.rcParams.update({'axes.unicode_minus':False,'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'figure.facecolor':'white','savefig.facecolor':'white','svg.fonttype':'none'})
    colors={'taxi_yellow':'#1d6b9b','citibike':'#14846f'};files=[]
    def save(fig,q,title):
        fig.suptitle(title,fontsize=14,fontweight='bold',x=.05,ha='left');fig.text(.05,.012,'纽约 · 2025年6月 · 仅接入Yellow Taxi / Citi Bike记录 · America/New_York',fontsize=8,color='#536773');fig.tight_layout(rect=[.015,.05,.99,.91])
        for ext in ['svg','png']:fig.savefig(out/'charts'/f'{q}.{ext}',dpi=160,bbox_inches='tight')
        plt.close(fig);files.append({'question_id':q,'title':title,'svg':f'charts/{q}.svg','png':f'charts/{q}.png'})
    day,hp,whp=data['day'],data['hour_profile'],data['week_hour_profile']
    for q,field,title,ylabel in [('T01-Q01','day','每日行程变化：高低值日期需结合背景信息核查','有效行程量（条）'),('T01-Q02','hour_profile','24小时日均出发量：分母为完整30天','该小时日均出发量（条/完整日）'),('T01-Q05','hour_profile','方式内归一化日内分布：各方式合计100%','该方式内部小时占比（%）')]:
        fig,ax=plt.subplots(figsize=(10.5,4.8))
        for mode,name in MODES.items():
            f=data[field].loc[data[field].transport_code==mode]
            if field=='day':f=f.sort_values('service_date');x=pd.to_datetime(f.service_date).dt.day;y=f.trip_count
            else:f=f.sort_values('local_hour');x=f.local_hour;y=f.within_mode_share*100 if q=='T01-Q05' else f.daily_average_trips
            ax.plot(x,y,label=name,color=colors[mode],marker='o',markersize=3)
        ax.set(xlabel='6月日期' if field=='day' else '纽约当地小时',ylabel=ylabel);ax.legend();ax.grid(axis='y',alpha=.15);save(fig,q,title)
    fig,axes=plt.subplots(1,2,figsize=(11.5,4.8))
    for ax,(mode,name) in zip(axes,MODES.items()):
        for week,style in [('周一至周五','-'),('周末','--')]:
            f=whp.loc[(whp.transport_code==mode)&(whp.week_type==week)].sort_values('local_hour');ax.plot(f.local_hour,f.daily_average_trips,label=f'{week}（{int(f.complete_days.iloc[0])}天）',linestyle=style,linewidth=2)
        ax.set(title=name,xlabel='纽约当地小时',ylabel='该小时日均出发量',xticks=range(0,24,4));ax.legend(fontsize=8);ax.grid(axis='y',alpha=.15)
    save(fig,'T01-Q03','周类型小时规律：分别按完整日期数计算日均量')
    fig,axes=plt.subplots(1,2,figsize=(10.5,4.8))
    for mode,name in MODES.items():
        s=data['mode_summary'][mode];axes[0].bar(name,s['average_duration_minutes'],color=colors[mode]);axes[0].text(name,s['average_duration_minutes'],f"{s['average_duration_minutes']:.2f}",ha='center',va='bottom')
    axes[0].set(title='平均时长（有效记录加权）',ylabel='分钟');distance=data['mode_summary']['taxi_yellow']['average_distance_km'];axes[1].bar('黄色出租车',distance,color=colors['taxi_yellow']);axes[1].text(0,distance,f'{distance:.2f}',ha='center',va='bottom');axes[1].set(title='平均距离（骑行真实距离不可用）',ylabel='公里');save(fig,'T01-Q04','行程特征：有效分子总和 ÷ 有效分母总和')
    fig,axes=plt.subplots(1,2,figsize=(13,7))
    for ax,(mode,name) in zip(axes,MODES.items()):
        f=data['zone_ranks'].loc[data['zone_ranks'].transport_code==mode].head(15).iloc[::-1];ax.barh(f.zone_name,f.departure_count,color=colors[mode]);ax.set(title=name,xlabel='月度出发量（条）');ax.tick_params(axis='y',labelsize=8)
    save(fig,'T02-Q01','区域出发Top15：仅计起点成功匹配的有效行程')
    fig,axes=plt.subplots(1,2,figsize=(16,9))
    for ax,(mode,name) in zip(axes,MODES.items()):
        f=data['od_top'].loc[data['od_top'].transport_code==mode].iloc[::-1];ax.barh(f.origin_zone_name+' → '+f.destination_zone_name,f.trip_count,color=colors[mode]);s=data['mode_summary'][mode]['od'];ax.set(title=f"{name} · Top20占全部OD {s['top20_concentration']*100:.2f}%",xlabel='月度OD量（条）');ax.tick_params(axis='y',labelsize=7)
    save(fig,'T02-Q02','各方式热门OD Top20：分母取完整OD数据，方向分别统计')
    fig,axes=plt.subplots(2,2,figsize=(12.5,8))
    for col,(mode,name) in enumerate(MODES.items()):
        f=data['region_hours'].loc[data['region_hours'].transport_code==mode].sort_values('local_hour');axes[0,col].plot(f.local_hour,f.selected_daily_average,color=colors[mode],linewidth=2);axes[0,col].set(title=name+' · '+f.zone_name.iloc[0],ylabel='该区域小时日均出发量',xlabel='纽约当地小时',xticks=range(0,24,4));axes[1,col].plot(f.local_hour,f.selected_share*100,label='选定区域',color=colors[mode]);axes[1,col].plot(f.local_hour,f.matched_global_share*100,label='全部起点匹配区域',linestyle='--',color='#ad6b42');axes[1,col].set(xlabel='纽约当地小时',ylabel='各自日内出发占比（%）',xticks=range(0,24,4));axes[1,col].legend(fontsize=8)
    save(fig,'T02-Q03','热点区域小时规律：绝对日均量与同口径总体形状对比')
    fig,ax=plt.subplots(figsize=(10.5,4.8))
    for i,(mode,name) in enumerate(MODES.items()):
        rates=data['mode_summary'][mode]['coverage'];values=[rates[k]*100 for k in ['origin_rate','destination_rate','both_rate']];bars=ax.bar([v+(i-.5)*.34 for v in range(3)],values,.34,label=name,color=colors[mode]);ax.bar_label(bars,fmt='%.2f%%',padding=3,fontsize=8)
    ax.set(xticks=range(3),xticklabels=['起点匹配率','终点匹配率','双端匹配率'],ylabel='占该方式全部有效行程（%）',ylim=(0,112));ax.legend();save(fig,'T02-Q04','空间覆盖率：区域出发和OD使用不同匹配子集')
    weather=data['weather']
    for q,metric,ylabel,title in [('T03-Q01','daily_average_trips','完整样本日日均行程量','同月同周类型降水组日均量：不直接比较组总量'),('T03-Q02','recomputed_ratio','相对各自未记录可测降水日比率','天气关联比率：各方式各周类型使用独立基准')]:
        fig,axes=plt.subplots(1,2,figsize=(12.5,5.2))
        for ax,week in zip(axes,['周一至周五','周末']):
            for i,(mode,name) in enumerate(MODES.items()):
                f=weather.loc[(weather.transport_code==mode)&(weather.week_type==week)].set_index('precipitation_group').reindex(ORDER);ax.bar([v+(i-.5)*.34 for v in range(3)],f[metric],.34,label=name,color=colors[mode])
            if q=='T03-Q02':ax.axhline(1,color='#a6683a',linestyle='--')
            ax.set(title=week,xticks=range(3),xticklabels=[WEATHER[k] for k in ORDER],ylabel=ylabel);ax.tick_params(axis='x',labelsize=8);ax.legend(fontsize=8)
        save(fig,q,title)
    fig,ax=plt.subplots(figsize=(10.5,4.8));samples=weather.loc[weather.transport_code=='taxi_yellow']
    for i,week in enumerate(['周一至周五','周末']):
        f=samples.loc[samples.week_type==week].set_index('precipitation_group').reindex(ORDER);bars=ax.bar([v+(i-.5)*.34 for v in range(3)],f.complete_days,.34,label=week);ax.bar_label(bars,fmt='%.0f天',padding=3)
    ax.set(xticks=range(3),xticklabels=[WEATHER[k] for k in ORDER],ylabel='独立天气日期数（天）');ax.legend();save(fig,'T03-Q03','天气对比样本：两种方式共享30个日期，不能相加')
    return files
def build(root,out,work):
    root,out,work=Path(root),Path(out),Path(work)
    for path in [out,work,out/'charts',out/'csv']:path.mkdir(parents=True,exist_ok=True)
    manifest_path=root/'outputs/marts-latest.json';m=json.loads(manifest_path.read_text(encoding='utf-8'))
    if not m.get('passed') or m.get('status')!='validated' or not m.get('publication',{}).get('passed'):raise ValueError('Validated historical publication required')
    if m['month']!='2025-06':raise ValueError('Only June 2025 verified')
    release=m['release_id'];tables={};sources=[]
    for table in ['dws.trip_day','dws.trip_hour','ads.zone_hour','ads.od_day','ads.weather_group']:
        frame,receipt=read_snapshot(root,table,m['output_versions'][table],release if table.startswith('ads.') else None)
        if len(frame)!=m['counts'][table]:raise ValueError('Pinned row count mismatch '+table)
        for c in ['service_date','event_date','month_start']:
            if c in frame:frame[c]=pd.to_datetime(frame[c]).dt.strftime('%Y-%m-%d')
        tables[table]=frame;sources.append(receipt)
    day=tables['dws.trip_day'];hour=tables['dws.trip_hour'];zone=tables['ads.zone_hour'];od=tables['ads.od_day'];weather=weather_metrics(tables['ads.weather_group']);dates=pd.date_range('2025-06-01','2025-06-30').strftime('%Y-%m-%d').tolist();validate_day_hour(day,hour,dates)
    if not zone.departure_complete.eq(True).all() or zone.arrival_complete.eq(True).any() or not zone.arrival_count.isna().all() or not zone.net_inflow.isna().all():raise ValueError('Spatial source completeness or arrival boundary changed')
    hp=hour_profile(hour);whp=hour_profile(hour,True);day=day.copy();day['week_type']=pd.to_datetime(day.service_date).dt.dayofweek.map(lambda d:'周一至周五' if d<5 else '周末');weekdays=day.groupby(['transport_code','week_type'],as_index=False).agg(trip_count=('trip_count','sum'),complete_days=('service_date','nunique'));weekdays['daily_average_trips']=weekdays.trip_count/weekdays.complete_days
    zr=zone.groupby(['transport_code','zone_key','zone_name','borough'],as_index=False).departure_count.sum().sort_values(['transport_code','departure_count','zone_key'],ascending=[True,False,True],kind='stable');zr['rank']=zr.groupby('transport_code').cumcount()+1;borough=zone.groupby(['transport_code','borough'],as_index=False).departure_count.sum().sort_values(['transport_code','departure_count'],ascending=[True,False]);zone['local_hour']=pd.to_datetime(zone.hour_start_utc,utc=True).dt.tz_convert('America/New_York').dt.hour
    summaries={};tops=[];regions=[];checks={}
    for mode in MODES:
        d=day.loc[day.transport_code==mode];h=hp.loc[hp.transport_code==mode];total=int(d.trip_count.sum());s={'total_trips':total,'complete_days':len(d),'average_duration_minutes':weighted_mean(d,'duration_sum_seconds','duration_count',60),'duration_records':int(d.duration_count.sum()),'average_distance_km':weighted_mean(d,'distance_sum_km','distance_count') if mode=='taxi_yellow' else None,'distance_records':int(d.distance_count.sum()) if mode=='taxi_yellow' else None}
        high=d.sort_values(['trip_count','service_date'],ascending=[False,True]).iloc[0];low=d.sort_values(['trip_count','service_date']).iloc[0];peak=h.sort_values(['trip_count','local_hour'],ascending=[False,True]).iloc[0];s.update(max_day={'date':high.service_date,'trips':int(high.trip_count)},min_day={'date':low.service_date,'trips':int(low.trip_count)},peak_hour={'local_hour':int(peak.local_hour),'daily_average_trips':float(peak.daily_average_trips),'within_mode_share':float(peak.within_mode_share)})
        s['coverage']={key:int(d[col].sum()) for key,col in [('origin_trips','origin_mapped_count'),('destination_trips','destination_mapped_count'),('both_trips','od_mapped_count')]}
        for key,n in [('origin_rate','origin_trips'),('destination_rate','destination_trips'),('both_rate','both_trips')]:s['coverage'][key]=safe_ratio(s['coverage'][n],total)
        top,osummary=ranked_od(od,mode);top['transport_code']=mode;top['rank']=range(1,len(top)+1);tops.append(top);s['od']=osummary;z=zr.loc[zr.transport_code==mode].iloc[0];s['top_origin']={'zone_key':int(z.zone_key),'zone_name':z.zone_name,'borough':z.borough,'departure_count':int(z.departure_count)}
        selected=zone.loc[(zone.transport_code==mode)&(zone.zone_key==z.zone_key)].groupby('local_hour',as_index=False).departure_count.sum().rename(columns={'departure_count':'selected_trips'});global_=zone.loc[zone.transport_code==mode].groupby('local_hour',as_index=False).departure_count.sum().rename(columns={'departure_count':'matched_global_trips'});selected=selected.merge(global_,on='local_hour');selected['transport_code']=mode;selected['zone_key']=int(z.zone_key);selected['zone_name']=z.zone_name;selected['complete_days']=30;selected['selected_daily_average']=selected.selected_trips/30;selected['matched_global_daily_average']=selected.matched_global_trips/30;selected['selected_share']=selected.selected_trips/selected.selected_trips.sum();selected['matched_global_share']=selected.matched_global_trips/selected.matched_global_trips.sum();regions.append(selected);rp=selected.sort_values(['selected_trips','local_hour'],ascending=[False,True]).iloc[0];gp=selected.sort_values(['matched_global_trips','local_hour'],ascending=[False,True]).iloc[0];s['region_peak']={'selected_local_hour':int(rp.local_hour),'selected_daily_average':float(rp.selected_daily_average),'matched_global_peak_hour':int(gp.local_hour)}
        s['week_type']=records(weekdays.loc[weekdays.transport_code==mode]);s['week_hour_peaks']={}
        for week in ['周一至周五','周末']:
            v=whp.loc[(whp.transport_code==mode)&(whp.week_type==week)].sort_values(['trip_count','local_hour'],ascending=[False,True]).iloc[0];s['week_hour_peaks'][week]={'local_hour':int(v.local_hour),'daily_average_trips':float(v.daily_average_trips),'complete_days':int(v.complete_days)}
        summaries[mode]=s;checks[mode]={'day_hour_totals_match':total==int(h.trip_count.sum()),'zone_matches_origin':int(zone.loc[zone.transport_code==mode].departure_count.sum())==s['coverage']['origin_trips'],'od_matches_both':osummary['all_matched_od_trips']==s['coverage']['both_trips'],'normalization_sums_to_one':abs(h.within_mode_share.sum()-1)<1e-12,'weather_days_per_mode':int(weather.loc[weather.transport_code==mode].complete_days.sum())==30,'weather_trips_match':int(weather.loc[weather.transport_code==mode].trip_count.sum())==total}
    if not all(all(v.values()) for v in checks.values()):raise ValueError('Analysis reconciliation failed')
    error=(weather.recomputed_ratio-weather.demand_ratio).abs().max()
    if error>1e-10:raise ValueError('Weather ratio mismatch')
    samples=weather.pivot(index=['is_weekday','precipitation_group'],columns='transport_code',values='complete_days')
    if not samples.nunique(axis=1).eq(1).all():raise ValueError('Weather sample mismatch across ways')
    data={'day':day,'hour_profile':hp,'week_hour_profile':whp,'weekdays':weekdays,'zone_ranks':zr,'borough':borough,'od_top':pd.concat(tops,ignore_index=True),'region_hours':pd.concat(regions,ignore_index=True),'weather':weather,'mode_summary':summaries}
    for name in ['day','hour_profile','week_hour_profile','weekdays','zone_ranks','borough','od_top','region_hours','weather']:data[name].to_csv(out/'csv'/f'{name}.csv',index=False,encoding='utf-8-sig',lineterminator='\n')
    charts=draw_charts(data,out,work);result={'passed':True,'created_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'month':'2025-06','release_id':release,'source_manifest_sha256':checksum(manifest_path),'source_versions':{s['table']:s['delta_version'] for s in sources},'source_row_counts':{s['table']:s['rows'] for s in sources},'read_method':'Exact complete JSON Delta add/remove log replay; serving rows filtered by validated release_id','mode_summary':summaries,'checks':checks,'weather_ratio_max_error':float(error),'weather':records(weather),'charts':charts,'scope_limits':['June 2025 and two ingested sources only','No complete event arrivals/net inflow','Monday-Friday is not holiday-adjusted business days','Single station daily weather; association only','No causal inference, significance test or forecast']};result['question_answers']=make_answers(data)
    (work/'source-snapshots.json').write_text(json.dumps(portable({'manifest_sha256':checksum(manifest_path),'release_id':release,'sources':sources}),ensure_ascii=False,indent=2),encoding='utf-8');(out/'source-provenance.json').write_text(json.dumps(portable({'manifest_sha256':checksum(manifest_path),'release_id':release,'sources':sources}),ensure_ascii=False,indent=2),encoding='utf-8');result['source_provenance_file']='source-provenance.json';(out/'analysis-results.json').write_text(json.dumps(portable(result),ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8');write_report(result,out,root);write_page(result,out);return result
def make_answers(data):
    s=data['mode_summary'];taxi=s['taxi_yellow'];bike=s['citibike'];a={}
    a['T01-Q01']='；'.join(f"{MODES[m]}共{v['total_trips']:,}条，日最高为{v['max_day']['date']}（{v['max_day']['trips']:,}条），最低为{v['min_day']['date']}（{v['min_day']['trips']:,}条）" for m,v in s.items())+'。高低值本身不能解释原因。'
    a['T01-Q02']='；'.join(f"{MODES[m]}峰值为当地{v['peak_hour']['local_hour']:02d}时，小时日均{v['peak_hour']['daily_average_trips']:,.1f}条" for m,v in s.items())+'。均以完整30天为分母。'
    parts=[]
    for m,v in s.items():
        weeks={r['week_type']:r for r in v['week_type']};w=weeks['周一至周五'];e=weeks['周末'];change=(e['daily_average_trips']/w['daily_average_trips']-1)*100;parts.append(f"{MODES[m]}周一至周五{w['complete_days']}天日均{w['daily_average_trips']:,.1f}条，周末{e['complete_days']}天日均{e['daily_average_trips']:,.1f}条，周末相对前者{change:+.2f}%；两组小时峰值为{v['week_hour_peaks']['周一至周五']['local_hour']:02d}时、{v['week_hour_peaks']['周末']['local_hour']:02d}时")
    a['T01-Q03']='；'.join(parts)+'。周一至周五没有剔除法定假日。'
    a['T01-Q04']=f"出租车平均时长{taxi['average_duration_minutes']:.2f}分钟（有效分母{taxi['duration_records']:,}条），骑行平均时长{bike['average_duration_minutes']:.2f}分钟（有效分母{bike['duration_records']:,}条）。出租车平均距离{taxi['average_distance_km']:.2f}公里（有效分母{taxi['distance_records']:,}条）；骑行真实距离不可用。均值从有效分子分母重新汇总，不能据此判断拥堵成因或出行效率。"
    a['T01-Q05']='；'.join(f"{MODES[m]}最活跃的{v['peak_hour']['local_hour']:02d}时占该方式月度行程{v['peak_hour']['within_mode_share']*100:.2f}%" for m,v in s.items())+'。各方式24小时占比合计100%，不是全市交通方式分担率，也不证明两方式互相替代。'
    a['T02-Q01']='；'.join(f"{MODES[m]}出发量最高区域为{v['top_origin']['zone_name']}（{v['top_origin']['borough']}），月度{v['top_origin']['departure_count']:,}条" for m,v in s.items())+'。完整Top15及行政区结果见CSV；只统计起点匹配记录，绝对量不是人口或面积标准化密度。'
    parts=[]
    for m,v in s.items():
        top=data['od_top'].loc[data['od_top'].transport_code==m].iloc[0];o=v['od'];parts.append(f"{MODES[m]}首位OD为{top.origin_zone_name}→{top.destination_zone_name}（{int(top.trip_count):,}条），Top20共{o['top20_trips']:,}条，占全部双端匹配OD {o['all_matched_od_trips']:,}条的{o['top20_concentration']*100:.2f}%")
    a['T02-Q02']='；'.join(parts)+'。保留方向及同区OD；分母取完整OD表，不从Top20导出反推。'
    a['T02-Q03']='；'.join(f"{MODES[m]}热点{v['top_origin']['zone_name']}的峰值为当地{v['region_peak']['selected_local_hour']:02d}时，小时日均{v['region_peak']['selected_daily_average']:,.1f}条；同方式全部起点匹配区域峰值为{v['region_peak']['matched_global_peak_hour']:02d}时" for m,v in s.items())+'。图中比较绝对日均量与各自归一化形状；参照统一为起点匹配子集，不含到达或缺车判断。'
    a['T02-Q04']='；'.join(f"{MODES[m]}起点、终点、双端匹配率为{v['coverage']['origin_rate']*100:.2f}%、{v['coverage']['destination_rate']*100:.2f}%、{v['coverage']['both_rate']*100:.2f}%" for m,v in s.items())+'。未匹配记录仍属于有效行程总量，空间结论只覆盖对应子集。'
    weather=data['weather'];parts=[];ratios=[]
    for m in MODES:
        for week in ['周一至周五','周末']:
            f=weather.loc[(weather.transport_code==m)&(weather.week_type==week)].set_index('precipitation_group');rain=f.loc['measurable'];base=f.loc['no_measurable'];parts.append(f"{MODES[m]}{week}可测降水日均{rain.daily_average_trips:,.1f}条（{int(rain.complete_days)}天），未记录可测降水组日均{base.daily_average_trips:,.1f}条（{int(base.complete_days)}天）");ratios.append(f"{MODES[m]}{week}可测降水比率{rain.recomputed_ratio:.3f}，相对基准{rain.change_percent:+.2f}%")
    a['T03-Q01']='；'.join(parts)+'。微量降水另列。比较为单站日天气背景下的关联，不能由此断言天气导致改变。'
    a['T03-Q02']='；'.join(ratios)+'。各项使用同方式同周类型的独立基准；日期因素和样本分布未被因果控制。'
    sample=weather.loc[weather.transport_code=='taxi_yellow'];parts=[]
    for week in ['周一至周五','周末']:
        f=sample.loc[sample.week_type==week].set_index('precipitation_group');parts.append(week+'：'+'、'.join(f'{WEATHER[k]}{int(f.loc[k].complete_days)}天' for k in ORDER))
    a['T03-Q03']='；'.join(parts)+'。两种方式共享这30个天气日期，不是60个独立样本。分组样本有限，仅作描述性对比，没有实施显著性检验。'
    return a

def write_report(result,out,root):
    titles={'T01':'出行需求与时间规律','T02':'区域出发与OD联系','T03':'天气与出行量对比'}
    text=['# 三个主题的数据分析结论','',f"生成时间（UTC）：{result['created_at_utc']}；纽约当地数据期间：2025年6月。",f"发布版本：{result['release_id']}。",'', '## 依据与复现','', '本次读取已验证清单固定的Delta版本，通过完整JSON日志add/remove动作选取有效文件，未累加目录内无效旧文件。ADS按同一release_id过滤。完整区域和OD数据用于聚合，每份输入文件记录SHA-256。没有修改事实数据。','', '```powershell',f'python tools/build_theme_analysis.py --output "{out.relative_to(root).as_posix()}" --work "work/{out.parent.name}/analysis"','```','', '核对通过：日小时总量一致；区域总量对齐起点匹配数；完整OD总量对齐双端匹配数；归一化合计100%；各方式天气样本各30天；天气比率与发布指标一致。','']
    for theme,title in titles.items():
        text+=['## '+theme+' '+title,'']
        for q,answer in result['question_answers'].items():
            if q.startswith(theme):
                link=os.path.relpath(out/'charts'/f'{q}.png',root/'docs').replace('\\','/');text+=['### '+q,'',answer,'',f'![{q}分析图]({link})','']
    text+=['## 范围限制','', '- 仅描述六月两类接入行程，不代表全市全部出行或未满足需求。','- 周一至周五未剔除节假日；小时为America/New_York。','- 区域排名未按人口、面积或来源覆盖偏差标准化；OD按六月出发队列统计。','- 不发布完整到达量、净流入、库存、缺车或供需缺口结论。','- 天气是中央公园单站日级观测；无因果推断、显著性检验或预测。','- 这是固定历史发布的离线分析，不替代当前服务健康验收。','', '## 分析输出','']
    for name in ['index.html','analysis-results.json']:text+=['- ['+name+']('+os.path.relpath(out/name,root/'docs').replace('\\','/')+')']
    full='\n'.join(text)+'\n';(root/'docs/18-三个主题的数据分析结论.md').write_text(full,encoding='utf-8');(out/'三个主题分析报告.md').write_text(full.replace('../outputs/'+out.parent.name+'/analysis/',''),encoding='utf-8')

def write_page(result,out):
    e=html.escape;titles={'T01':'出行需求与时间规律','T02':'区域出发与OD联系','T03':'天气与出行量对比'};cards=[]
    for q,answer in result['question_answers'].items():
        title=next(c['title'] for c in result['charts'] if c['question_id']==q);cards.append(f'<article data-theme="{q[:3]}"><p class="tag">{q}</p><h3>{e(title)}</h3><p>{e(answer)}</p><a href="charts/{q}.png"><img src="charts/{q}.svg" alt="{e(title)}" loading="lazy"></a><p class="small"><a href="charts/{q}.svg">SVG矢量图</a> · <a href="charts/{q}.png">PNG图片</a></p></article>')
    total=sum(s['total_trips'] for s in result['mode_summary'].values());page='''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>城市出行数仓 · 三个主题实证分析</title><style>body{margin:0;background:#f2f6f7;color:#203644;font:16px/1.8 "Microsoft YaHei",sans-serif}main{max-width:1180px;margin:auto;padding:35px 24px}header,article,.panel{background:white;border:1px solid #dbe5e8;border-radius:14px;padding:24px 30px;margin:20px 0}header{border-top:6px solid #14846f}h1{font-size:30px;line-height:1.4}h3{font-size:20px}a{color:#17608f}img{width:100%;height:auto}.tag{color:#14846f;font-weight:bold;font-size:13px}.small{color:#60717b;font-size:13px;overflow-wrap:anywhere}.summary{display:flex;flex-wrap:wrap;gap:12px}.stat{background:#eaf3f2;padding:14px 23px;border-radius:8px}.stat strong{display:block;font-size:26px}.note{border-left:4px solid #b88035;background:#fff7e9;padding:14px 18px}select,button{font:inherit;border:1px solid #bacdd7;border-radius:7px;background:#fff;padding:8px 12px}article[hidden]{display:none}.controls{display:flex;gap:18px;flex-wrap:wrap;align-items:center}.csv{columns:2}li{margin:7px 0}@media(max-width:600px){main{padding:15px}header,article,.panel{padding:18px}h1{font-size:24px}.csv{columns:1}}@media print{body{background:white}main{padding:0}.controls{display:none}article[hidden]{display:block}article{break-inside:avoid}}</style></head><body><main>'''
    page+=f'<header><p class="tag">城市出行离线数仓 · 可复现实证分析</p><h1>三个分析主题，十二个业务问题</h1><p>纽约2025年6月 · Yellow Taxi / Citi Bike · 已验证历史发布</p><div class="summary"><div class="stat"><strong>{total:,}</strong>有效行程</div><div class="stat"><strong>12</strong>业务问题与结论</div><div class="stat"><strong>12</strong>可导出分析图</div></div><p class="small">生成（UTC）：{e(result["created_at_utc"])}<br>发布版本：{e(result["release_id"])}</p></header>'
    page+='<p class="note">仅描述接入记录。天气比较表示观察性关联；周一至周五未剔除节假日；不提供完整到达量、净流入或供需缺口结论。</p><div class="panel controls"><label for="theme">主题 <select id="theme"><option value="all">全部主题</option>'+''.join(f'<option value="{k}">{v}</option>' for k,v in titles.items())+'</select></label><button id="print" type="button">打印 / 保存PDF</button><a href="analysis-results.json">机器可读结果与核对记录</a></div>'+''.join(cards)
    csvlinks=''.join(f'<li><a href="csv/{p.name}">{e(p.name)}</a></li>' for p in sorted((out/'csv').glob('*.csv')));page+='<section class="panel"><h2>分析CSV</h2><p>OD集中度使用完整OD表计算；导出的od_top是计算后的Top20，不能再次将其当作完整分母。</p><ul class="csv">'+csvlinks+'</ul></section><section class="panel"><h2>复现与限制</h2><p>读取固定Delta版本，按发布版本取完整数据。检查日小时总量、区域匹配、OD分母、归一化及天气比率。输入文件SHA-256记录见本页同目录的source-provenance.json，work目录保留同份工作记录。没有重算事实层，不替代服务健康验收。</p><p>各方式24小时占比不是全市方式分担率。两种方式共享30个天气日期，不是60个独立样本。图表可导出SVG和PNG，本页面无外部字体或脚本依赖。</p></section></main><script>const s=document.getElementById("theme");s.addEventListener("change",()=>document.querySelectorAll("article[data-theme]").forEach(a=>a.hidden=s.value!=="all"&&a.dataset.theme!==s.value));document.getElementById("print").addEventListener("click",()=>window.print());</script></body></html>'
    (out/'index.html').write_text(page,encoding='utf-8')
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--output',type=Path,required=True);p.add_argument('--work',type=Path,required=True);args=p.parse_args();root=args.root.resolve();out=args.output if args.output.is_absolute() else root/args.output;work=args.work if args.work.is_absolute() else root/args.work;result=build(root,out.resolve(),work.resolve());print(json.dumps({'passed':result['passed'],'release_id':result['release_id'],'questions':len(result['question_answers']),'charts':len(result['charts']),'output':str(out)},ensure_ascii=False))
if __name__=='__main__':main()
