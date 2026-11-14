"""Export small published result sets for Excel/WPS and create an offline index."""
import csv
import html
import json
from pathlib import Path
from publish_marts import ClickHouse

ROOT = Path(__file__).resolve().parents[1]

def main():
    ch = ClickHouse(); out = ROOT / 'outputs/marts'; out.mkdir(parents=True, exist_ok=True)
    queries = {
        '六月每日出行.csv': 'SELECT * FROM ads.bi_trip_day ORDER BY service_date,transport_code',
        '六月每小时出行.csv': 'SELECT * FROM ads.bi_trip_hour ORDER BY hour_start_utc,transport_code',
        '六月天气分组.csv': 'SELECT * FROM ads.bi_weather_group ORDER BY transport_code,is_weekday,precipitation_group',
        '热门OD前20.csv': "SELECT origin_zone_name,destination_zone_name,transport_code,SUM(trip_count) AS monthly_trips FROM ads.bi_od_day GROUP BY origin_zone_name,destination_zone_name,transport_code ORDER BY monthly_trips DESC LIMIT 20",
        '六月数据质量.csv': 'SELECT * FROM ads.bi_quality_batch ORDER BY source_id',
    }
    for filename, sql in queries.items():
        rows = ch.query(sql)
        with (out / filename).open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
    dashboards = json.loads((ROOT / 'outputs/superset-historical-dashboards.json').read_text(encoding='utf-8'))['dashboards']
    links = ''.join(f'<a class="card" href="{d["url"]}"><b>{html.escape(d["title"])}</b><span>打开 Superset 离线分析看板 →</span></a>' for d in dashboards)
    files = ''.join(f'<li><a href="{html.escape(name)}">{html.escape(name)}</a></li>' for name in queries)
    links += '<a class="card" href="../map/map.html"><b>地图与联动筛选</b><span>日期 / 方式 / 行政区 / 区域 · 离线地图与CSV →</span></a>'
    (out / '六月历史指标与看板.html').write_text('''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>六月历史指标与看板</title>
<style>body{font:16px/1.8 "Microsoft YaHei",sans-serif;background:#f3f6fa;color:#172d48;max-width:1050px;margin:50px auto;padding:0 25px}h1{font-size:36px;margin-bottom:4px}.sub{color:#53647a}.grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin:28px 0}.card{background:white;border-radius:12px;padding:25px;text-decoration:none;color:#143d6a;box-shadow:0 3px 12px #13334b0d}.card b{display:block;font-size:23px}.card span{color:#63768b;font-size:14px}section{background:white;border-radius:12px;padding:24px;margin-top:24px}strong{font-size:24px}a{color:#1768a8}</style>
<h1>城市出行 · 六月历史分析</h1><div class="sub">纽约 · 2025 年 6 月 · Yellow Taxi / Citi Bike · 官方真实数据</div>
<section><strong>9,012,795 条有效行程</strong><br>黄色出租车 4,254,337 条 ｜ 共享单车 4,758,458 条<br>已完成 DWD → DWS → ADS → ClickHouse → Superset。</section>
<div class="grid">''' + links + '''</div><section><b>怎么查看</b><p>先双击项目根目录的“启动项目.cmd”，再点击上方看板。首次进入需登录；账号在 outputs/local-credentials.md。下方 CSV 无需启动服务，可用 Excel / WPS 打开。</p><ul>''' + files + '''</ul></section>
<section><b>阅读口径</b><p>总量仅表示已接入的两类有效记录。小时趋势采用纽约当地小时；天气比较使用日均量，并区分周一至周五、周末与微量降水。尚未补齐五月边界数据，因此完整到达量及净流入保持不可用。OD 表示六月开始的行程。</p><p>CSV 是当前发布的导出快照；Superset 查询当前已验证发布。<a href="../运行状态.html">运行状态</a> · <a href="http://localhost:18080">Airflow 自动调度</a>。</p></section></html>''', encoding='utf-8')
    print('MARTS_PREVIEW_OK files=5')

if __name__ == '__main__': main()
