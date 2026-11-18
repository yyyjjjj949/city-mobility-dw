"""Rebuild conceptual, logical and layering diagrams with Pillow; verify against source.

Usage: python tools/build_model_review.py --output-dir outputs/2026-11-18-theme-definition-e721eb90/model
Dependencies: Pillow; set --font/--bold-font to installed Chinese fonts on non-Windows systems.
The existing six-source reference-manifest.json is an input; this tool does not refresh network receipts.
"""
from pathlib import Path
import argparse
from PIL import Image, ImageDraw, ImageFont
import html, json, hashlib, re
ROOT=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output-dir',default='outputs/2026-11-18-theme-definition-e721eb90/model')
parser.add_argument('--font',default='C:/Windows/Fonts/msyh.ttc')
parser.add_argument('--bold-font',default='C:/Windows/Fonts/msyhbd.ttc')
args=parser.parse_args()
OUT=Path(args.output_dir)
if not OUT.is_absolute():OUT=ROOT/OUT
OUT=OUT.resolve();OUT.mkdir(parents=True,exist_ok=True)
FONT=Path(args.font);BOLD=Path(args.bold_font)
if not FONT.is_file() or not BOLD.is_file():raise SystemExit('Supply installed Chinese font paths with --font and --bold-font.')
class Canvas:
 def __init__(self,w,h,title,subtitle):
  self.w=w;self.h=h;self.parts=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" role="img" aria-label="{html.escape(title)}"><rect width="100%" height="100%" fill="#f5f8fa"/>'];self.im=Image.new('RGB',(w,h),'#f5f8fa');self.draw=ImageDraw.Draw(self.im);self.text(45,35,title,34,'#17384c',True);self.text(45,88,subtitle,20,'#546d7d')
 def text(self,x,y,text,size=22,fill='#17384c',bold=False):
  self.parts.append(f'<text x="{x}" y="{y+size}" font-family="Microsoft YaHei,Arial,sans-serif" font-size="{size}" fill="{fill}" font-weight="{700 if bold else 400}">{html.escape(text)}</text>');self.draw.text((x,y),text,font=ImageFont.truetype(str(BOLD if bold else FONT),size),fill=fill)
 def line(self,x1,y1,x2,y2,color='#7892a3',width=2):
  self.parts.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{width}"/>');self.draw.line((x1,y1,x2,y2),fill=color,width=width)
 def box(self,x,y,w,h,lines,fill='#ffffff',stroke='#9cb3c2',size=22):
  self.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="13" fill="{fill}" stroke="{stroke}" stroke-width="2"/>');self.draw.rounded_rectangle((x,y,x+w,y+h),radius=13,fill=fill,outline=stroke,width=2)
  for i,line in enumerate(lines):self.text(x+20,y+15+i*(size+11),line,size,'#17384c',i==0)
 def save(self,name):
  (OUT/(name+'.svg')).write_text('\n'.join(self.parts+['</svg>']),encoding='utf-8');self.im.save(OUT/(name+'.png'))

c=Canvas(1500,980,'城市历史出行：概念主题结构','表达业务对象与分析视角；不在概念层混入表字段、主外键或数据库实体关系')
c.box(470,150,560,105,['总主题：城市历史出行','范围：2025年6月 · Yellow Taxi / Citi Bike'],fill='#e0efec',size=25)
c.line(650,255,350,330);c.line(850,255,1150,330)
c.box(100,330,520,100,['业务过程：出租车行程','一次历史行程的完成记录'],fill='#e8f0fa',size=24)
c.box(880,330,520,100,['业务过程：共享单车骑行','一次历史骑行的完成记录'],fill='#e8f0fa',size=24)
for x in [270,750,1230]:
 c.line(350,430,x,530);c.line(1150,430,x,530)
c.box(50,530,430,145,['T01 出行需求与时间规律','日期、小时、周类型、交通方式','关注活动强度与日内差异'],fill='#ffffff',size=23)
c.box(535,530,430,145,['T02 区域出发与OD联系','起点/终点区域、行政区、时段','关注热点、联系与匹配覆盖'],fill='#ffffff',size=23)
c.box(1020,530,430,145,['T03 天气与出行量对比','周类型、降水组、方式、样本日','关注关联及证据适用范围'],fill='#ffffff',size=23)
c.box(70,745,1360,110,['共同分析保障：一致时间与空间定义 · 来源与质量追溯 · 样本和分母可核查','输出：业务问题 → 指标与分析维度 → 图表与数值证据 → 结论与边界'],fill='#eaf0f3',size=25)
c.text(70,900,'边界：接入行程≠全市总出行；观察关联≠因果；无边界来源不发布完整到达/净流入',22,'#6d5a38')
c.save('01-概念主题结构')

c=Canvas(1700,1270,'逻辑模型：两个行程星型与共享维度星座','事实角色键直接引用维度唯一键；一维度成员可关联多事实行（1:N），未知成员也必须可解析')
shared=[(40,'日期 dim_date','date_key','开始 / 结束'),(375,'分钟 dim_time','time_key','开始 / 结束'),(710,'区域 dim_zone','zone_key','起点 / 终点'),(1045,'方式 dim_transport','transport_code','受控方式'),(1380,'日天气 dim_weather_daily','weather_key','开始日期上下文')]
# routes behind nodes, target anchors avoid boxes
for x,*rest in shared:
 for tx,fy in [(420,490),(1260,490)]:
  c.line(x+140,310,x+140,355);c.line(x+140,355,tx,fy)
for x,title,key,role in shared:
 if key=='weather_key': c.box(x,165,280,145,['日天气观察上下文','dim_weather_daily','键: '+key,role],fill='#e4eef8',size=20)
 else: c.box(x,165,280,145,[title,'键: '+key,role],fill='#e4eef8',size=20)
c.box(70,490,710,245,['出租车事实 dwd.fact_taxi_trip','粒度：一条通过核心规则的源行程记录','逻辑键：trip_key = source_record_key','角色键：start/end_date_key、start/end_time_key','origin/destination_zone_key、transport_code、weather_key','度量：时长秒数；距离公里（资格规则单独控制）'],fill='#dceee3',size=21)
c.box(920,490,710,245,['骑行事实 dwd.fact_bike_trip','粒度：一个无业务内容冲突的有效 ride_id','逻辑键：ride_id','共享角色键同左；专用：origin/destination_station_key','bike_type_code；时长秒数（源无真实路线距离）','同ID同内容去重；同ID冲突阻断'],fill='#dceee3',size=21)
c.line(1110,735,1110,840);c.line(1440,735,1440,840)
c.box(895,840,400,140,['站点 dim_station','逻辑键: station_key','起站 / 终站；期间快照键'],fill='#e4eef8',size=21)
c.box(1330,840,325,140,['车型 dim_bike_type','逻辑键: bike_type_code','真实来源车型'],fill='#e4eef8',size=21)
c.box(70,840,740,140,['事实标识与审计（不画为业务维度）','ride_id可理解为退化维度；出租车trip_key为源行身份','source_record_key → ODS；batch_id → OPS'],fill='#fff2dc',size=21)
c.text(70,1040,'角色键示例：start_date_key 与 end_date_key → 同一 dim_date.date_key；不存在事实对事实明细JOIN。',22)
c.text(70,1095,'辅助引用：dim_station.zone_key → dim_zone；dim_weather_daily.date_key → dim_date。',22)
c.text(70,1150,'当前逻辑DIM实际存于 dwd.dim_*。天气数值仅作上下文，不能沿行程行求和；完整键关系见第15号文档。',21)
c.save('02-逻辑星座模型')

c=Canvas(1580,1110,'分层职责与维度模型：两种设计视角的对应','逻辑DIM与DWD共同组成明细维度模型；ODS/DWS/ADS描述加工与服务职责')
boxes=[(50,175,310,150,['ODS 原始引入','源文件 / 原始payload','对象、成员、记录定位'],'#f0ece3'),(440,175,1090,150,['公共模型层 CDM（逻辑组织）','DIM：公共描述、角色复用和版本 · DWD：业务过程最细粒度事实','DWS：跨主题复用的公共汇总与可重算组成项'],'#e2efec')]
for x,y,w,h,l,f in boxes:c.box(x,y,w,h,l,fill=f,size=24)
c.line(360,250,440,250)
c.box(470,390,440,160,['逻辑 DIM（实际 dwd.dim_*）','日期 / 分钟 / 区域 / 方式 / 天气','站点 / 车型；角色键与未知成员'],fill='#e4eef8',size=23)
c.box(1000,390,500,160,['DWD 两张事务事实','一有效出租车源行 / 一有效ride_id','事实 + 维度形成星型 / 共享星座'],fill='#dceee3',size=23)
c.line(910,465,1000,465)
c.line(1250,550,1250,620)
c.box(470,620,1030,140,['DWS 公共汇总','日 / 小时 / 区域小时 / OD日 / 天气组','保留次数、时长/距离分子分母、完整性与版本'],fill='#e2efec',size=24)
c.line(990,760,990,825)
c.box(470,825,1030,145,['ADS 应用与发布服务','派生均值/比率、补名称与行政区、候选校验、发布版本隔离','ClickHouse ads.bi_* → Superset / 地图 / 主题分析'],fill='#f7edda',size=24)
c.box(50,390,310,580,['OPS 管理旁路','来源对象','任务批次','质量结果 / 隔离','构建与发布审计','','Airflow编排','跨各加工步骤'],fill='#eaf0f3',size=24)
c.text(55,1020,'同粒度ADS允许存在：公共汇总可复用 + 面向应用的语义补全和发布隔离；不强制每层降粒度。',23)
c.save('03-分层与维度模型对应')


from verify_model_design import verify
report=verify(OUT)
(OUT/'model-design-review.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks']),'diagrams':3}))
if not report['passed']:raise SystemExit(1)
