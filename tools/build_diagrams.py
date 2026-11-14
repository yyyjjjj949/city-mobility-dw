"""Editable draw.io pages plus self-contained SVG/HTML previews for offline review."""
from pathlib import Path
from html import escape
import xml.etree.ElementTree as ET

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'docs/diagrams'
COLORS={'blue':('#dae8fc','#6c8ebf'),'green':('#d5e8d4','#82b366'),'yellow':('#fff2cc','#d6b656'),'gray':('#f5f5f5','#666666')}
PAGES=[]

def page(name,nodes,edges):PAGES.append((name,nodes,edges))
# Node: id, multiline title, x, y, width, height, color. Edge: source,target,label,exitX,exitY,entryX,entryY,waypoints,dashed.
page('系统架构',[
 ('source','官方月度行程 / 日天气 / 区域文件',30,20,740,55,'blue'),
 ('python','Python 批量采集 / 来源与文件校验',30,115,340,65,'blue'),
 ('spark','Spark Batch / Delta Lake',430,115,340,65,'blue'),
 ('ods','ODS 原始层\n源对象与原始记录',20,255,165,80,'green'),
 ('dwd','DWD 明细层\n7维度 / 2行程事实',220,255,165,80,'green'),
 ('dws','DWS 主题层\n日 / 小时 / 区域\nOD / 天气',420,255,165,80,'green'),
 ('ads','ADS 应用层\n历史分析与质量指标',620,255,165,80,'green'),
 ('airflow','Airflow\n完整离线链路 / 重试 / 监控',20,445,340,80,'gray'),
 ('quality','OPS 来源 / 批次 / 质量 / 发布记录',20,365,340,55,'yellow'),
 ('ch','ClickHouse\n校验与版本化发布',430,420,165,80,'blue'),
 ('bi','Superset / D3\n4个看板 / 地图筛选',620,420,165,80,'blue'),
 ],[
 ('source','python','下载',.2,1,.5,0,[],False),
 ('source','spark','离线批处理',.8,1,.5,0,[],True),
 ('python','ods','归档入仓',.2,1,.5,0,[],False),
 ('ods','dwd','清洗',1,.5,0,.5,[],False),
 ('dwd','dws','主题汇总',1,.5,0,.5,[],False),
 ('dws','ads','指标输出',1,.5,0,.5,[],False),
 ('ads','ch','验证后发布',.5,1,.5,0,[(702,385),(512,385)],False),
 ('ch','bi','查询',1,.5,0,.5,[],False),
 ('airflow','python','全链路调度',0,.5,0,.5,[(10,485),(10,147)],True),
 ('quality','ch','门禁',1,.5,0,.2,[(385,392),(385,436)],True),
 ])

page('行程维度模型',[
 ('date','日期维度\ndim_date',20,30,160,65,'blue'),('time','日内分钟维度\ndim_time',220,30,160,65,'blue'),
 ('zone','区域维度\ndim_zone',420,30,160,65,'blue'),('transport','交通方式维度\ndim_transport',620,30,160,65,'blue'),
 ('taxi','出租车行程事实\nfact_taxi_trip\n一条有效源行程记录',80,245,260,110,'green'),
 ('bike','共享单车骑行事实\nfact_bike_trip\n一个有效 ride_id',460,245,260,110,'green'),
 ('station','历史期间站点维度\ndim_station\n站点编号 × 源月份',20,470,240,90,'blue'),
 ('biketype','车型维度\ndim_bike_type',300,470,180,90,'blue'),
 ('weather','日天气观察维度\ndim_weather_daily\n气象站 × 观测日期',520,470,260,90,'yellow'),
 ],[
 ('date','taxi','开始/结束',.5,1,.15,0,[(100,140),(119,140)],False),
 ('date','bike','开始/结束',.8,1,.1,0,[(148,120),(486,120)],False),
 ('time','taxi','开始/结束',.25,1,.45,0,[(260,165),(197,165)],False),
 ('time','bike','开始/结束',.75,1,.35,0,[(340,145),(551,145)],False),
 ('zone','taxi','起点/终点',.25,1,.7,0,[(460,190),(262,190)],False),
 ('zone','bike','起点/终点',.75,1,.6,0,[(540,170),(616,170)],False),
 ('transport','taxi','方式',.25,1,.9,0,[(660,215),(314,215)],False),
 ('transport','bike','方式',.75,1,.85,0,[(740,195),(681,195)],False),
 ('station','bike','起站/终站',.5,0,.2,1,[(140,420),(512,420)],False),
 ('biketype','bike','车型',.5,0,.5,1,[(390,400),(590,400)],False),
 ('weather','bike','开始日期',.65,0,.85,1,[(689,400),(681,400)],False),
 ('weather','taxi','开始日期',.2,0,.8,1,[(572,440),(288,440)],False),
 ])

def build():
    OUT.mkdir(parents=True,exist_ok=True)
    mxfile=ET.Element('mxfile',host='app.diagrams.net',agent='CityMobilityModelBuilder')
    previews=[]
    for index,(name,nodes,edges) in enumerate(PAGES):
        diagram=ET.SubElement(mxfile,'diagram',id=f'page{index}',name=name)
        model=ET.SubElement(diagram,'mxGraphModel',dx='800',dy='600',grid='1',gridSize='10',page='1',pageWidth='800',pageHeight='600')
        root=ET.SubElement(model,'root');ET.SubElement(root,'mxCell',id='0');ET.SubElement(root,'mxCell',id='1',parent='0')
        ids={node[0]:str(i+2) for i,node in enumerate(nodes)}
        geometry={n[0]:n[2:6] for n in nodes}
        svg=['<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1200" viewBox="0 0 800 600">',
             '<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 Z" fill="#526477"/></marker></defs>',
             '<rect width="800" height="600" fill="white"/>']
        for i,edge in enumerate(edges):
            source,target,label,ex,ey,ix,iy,waypoints,dashed=edge
            sx,sy,sw,sh=geometry[source];tx,ty,tw,th=geometry[target]
            points=[(sx+sw*ex,sy+sh*ey),*waypoints,(tx+tw*ix,ty+th*iy)]
            style=f'edgeStyle=orthogonalEdgeStyle;rounded=0;html=1;endArrow=block;strokeWidth=1.5;exitX={ex};exitY={ey};entryX={ix};entryY={iy};'+('dashed=1;' if dashed else '')
            cell=ET.SubElement(root,'mxCell',id=str(len(nodes)+i+2),value=label,style=style,edge='1',parent='1',source=ids[source],target=ids[target])
            geo=ET.SubElement(cell,'mxGeometry',relative='1',attrib={'as':'geometry'})
            if waypoints:
                arr=ET.SubElement(geo,'Array',attrib={'as':'points'})
                for x,y in waypoints:ET.SubElement(arr,'mxPoint',x=str(x),y=str(y))
            svg.append(f'<polyline points="{" ".join(f"{x},{y}" for x,y in points)}" fill="none" stroke="#526477" stroke-width="1.4" marker-end="url(#arrow)"'+(' stroke-dasharray="5,4"' if dashed else '')+'/>')
            if label:
                mid=points[len(points)//2] if len(points)>2 else ((points[0][0]+points[-1][0])/2,(points[0][1]+points[-1][1])/2)
                svg.append(f'<text x="{mid[0]+5}" y="{mid[1]-7}" font-size="10" fill="#334155" font-family="Microsoft YaHei,sans-serif" paint-order="stroke" stroke="white" stroke-width="3">{escape(label)}</text>')
        for key,title,x,y,w,h,color in nodes:
            fill,stroke=COLORS[color]
            cell=ET.SubElement(root,'mxCell',id=ids[key],value=title,style=f'rounded=1;whiteSpace=wrap;html=0;fillColor={fill};strokeColor={stroke};fontSize=13;fontFamily=Microsoft YaHei;',vertex='1',parent='1')
            ET.SubElement(cell,'mxGeometry',x=str(x),y=str(y),width=str(w),height=str(h),attrib={'as':'geometry'})
            svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="9" fill="{fill}" stroke="{stroke}"/>')
            lines=title.split('\n')
            for line_index,line in enumerate(lines):
                svg.append(f'<text x="{x+w/2}" y="{y+h/2+(line_index-(len(lines)-1)/2)*20+5}" text-anchor="middle" font-size="13" fill="#172b4d" font-family="Microsoft YaHei,sans-serif">{escape(line)}</text>')
        svg.append('</svg>')
        filename=f'{index+1:02d}-{name}.svg';(OUT/filename).write_text('\n'.join(svg),encoding='utf-8');previews.append((name,filename))
    ET.ElementTree(mxfile).write(OUT/'城市出行数仓设计.drawio',encoding='utf-8',xml_declaration=True)
    body=''.join(f'<article><h2>{escape(name)}</h2><img src="{escape(file)}" alt="{escape(name)}"/></article>' for name,file in previews)
    html='<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>城市出行数仓设计图</title><style>body{font-family:"Microsoft YaHei",sans-serif;background:#f0f4f8;color:#172b4d;margin:24px auto;max-width:1050px}article{background:white;margin:24px 0;padding:20px;border-radius:12px}img{width:100%;height:auto}a{color:#1667ad}</style><h1>城市出行数仓设计图</h1><p>离线模型设计 v2.0 · 可离线查看。<a href="城市出行数仓设计.drawio">下载可编辑的 draw.io 文件</a></p>'+body+'</html>'
    (OUT/'设计图预览.html').write_text(html,encoding='utf-8')
    print('Generated 2 draw.io pages and offline SVG/HTML previews.')

if __name__=='__main__':build()
