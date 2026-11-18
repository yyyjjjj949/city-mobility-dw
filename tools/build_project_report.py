"""Build the project report from checked model notes and reproducible analysis results.

Run python tools/build_project_report.py after tools/build_theme_analysis.py.
HTML and Markdown use only local project assets. Optional --pdf uses Playwright.
"""
from pathlib import Path
import argparse
import html
import json
import os
import re
import datetime as dt
from urllib.parse import urlsplit, unquote

ROOT = Path(__file__).resolve().parents[1]
CHAT = '2026-11-18-theme-definition-e721eb90'
OUT = ROOT / 'outputs' / CHAT

def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))

def build(output=None, pdf=False):
    # Report content is generated after analysis completion by the integration step.
    output = output or OUT / 'report'
    output.mkdir(parents=True, exist_ok=True)
    content = (ROOT / 'docs/20-城市出行离线数仓项目报告.md').read_text(encoding='utf-8')
    def rebase_link(match):
        value=match.group(2)
        parsed=urlsplit(value)
        if parsed.scheme or not parsed.path or parsed.path.startswith('/'):
            return match.group(0)
        resolved=(ROOT/'docs'/unquote(parsed.path)).resolve()
        relative=os.path.relpath(resolved,output).replace(os.sep,'/')
        if parsed.fragment:relative+='#'+parsed.fragment
        return match.group(1)+'('+relative+')'
    content=re.sub(r'(!?\[[^\]]*\])\(([^)]+)\)',rebase_link,content)
    target = output / '城市出行离线数仓项目报告.md'
    target.write_text(content, encoding='utf-8')
    try:
        import markdown
    except ImportError as exc:
        raise RuntimeError('Install requirements-review.txt to render report HTML') from exc
    # Source lives in docs; image/local document references are written relative to
    # the output report directory, so the Markdown export and HTML stay portable.
    body = markdown.markdown(content, extensions=['tables','fenced_code','toc'])
    css = '''@page{size:A4;margin:18mm 17mm 18mm}*{box-sizing:border-box}body{font:15px/1.8 "Microsoft YaHei","PingFang SC",sans-serif;color:#203644;margin:0;background:#eef2f4}main{max-width:1000px;margin:32px auto;padding:40px 48px;background:#fff}h1{color:#132c3b;font-size:30px;line-height:1.45;margin-top:8px}h2{font-size:22px;border-bottom:1px solid #bacdd7;padding-bottom:8px;margin-top:34px;break-after:avoid}h3{font-size:18px;margin-top:24px;break-after:avoid}p{orphans:3;widows:3}a{color:#146d91;overflow-wrap:anywhere}table{border-collapse:collapse;width:100%;font-size:13px;margin:14px 0;table-layout:auto}td,th{border:1px solid #c8d5dc;padding:8px 10px;text-align:left;vertical-align:top;overflow-wrap:anywhere}th{background:#eaf2f3}tr{break-inside:avoid}thead{display:table-header-group}img{max-width:100%;height:auto;max-height:235mm;object-fit:contain;display:block;margin:18px auto;break-inside:avoid}pre{background:#f3f6f7;padding:15px;white-space:pre-wrap;overflow-wrap:anywhere;font-size:12px}code{font:12px/1.6 Consolas,monospace;overflow-wrap:anywhere}blockquote{border-left:3px solid #689caa;padding:4px 14px;color:#45606f;margin:14px 0}.toolbar{background:#123e50;color:white;padding:14px 24px}.toolbar button{font:inherit;padding:6px 15px;cursor:pointer}.toolbar a{color:white;margin-left:18px}@media(max-width:700px){main{margin:0;padding:20px 15px}h1{font-size:25px}table{font-size:11px}td,th{padding:5px}}@media print{body{background:#fff;font-size:10pt;line-height:1.7}main{margin:0;max-width:none;padding:0}.toolbar{display:none}h1{font-size:23pt}h2{font-size:16pt}h3{font-size:12pt}table{font-size:8pt}td,th{padding:5px 6px}a{color:#203644;text-decoration:none}pre{font-size:8pt}img{max-height:215mm}}'''
    document = '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>城市出行离线数仓项目报告</title><style>'+css+'</style></head><body><div class="toolbar"><button onclick="window.print()">打印或保存PDF</button><a href="城市出行离线数仓项目报告.md" download>Markdown</a><a href="../交付入口.html">交付入口</a></div><main>'+body+'</main></body></html>'
    page_path = output / '城市出行离线数仓项目报告.html'
    page_path.write_text(document, encoding='utf-8')
    if pdf:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(channel='msedge', headless=True)
            page = browser.new_page(viewport={'width':1200,'height':1000})
            page.goto(page_path.as_uri(), wait_until='networkidle')
            page.evaluate('document.fonts.ready')
            missing = page.locator('img').evaluate_all('(imgs)=>imgs.filter(i=>!i.complete||i.naturalWidth===0).map(i=>i.src)')
            if missing:
                raise RuntimeError('Missing report images: '+str(missing))
            page.pdf(path=str(output/'城市出行离线数仓项目报告.pdf'), print_background=True, prefer_css_page_size=True, display_header_footer=True, header_template='<span></span>', footer_template='<div style="font-size:8px;text-align:center;width:100%;color:#607783">城市出行离线数仓项目报告 · <span class="pageNumber"></span> / <span class="totalPages"></span></div>')
            browser.close()
    return {'markdown':str(target.relative_to(ROOT)), 'html':str(page_path.relative_to(ROOT)), 'pdf':pdf}

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path)
    parser.add_argument('--pdf',action='store_true')
    args=parser.parse_args()
    print(json.dumps(build(args.output_dir,args.pdf), ensure_ascii=False,indent=2))
