"""Check offline review pages, interactions, local links and mobile overflow.
Optional browser dependency: pip install playwright; Windows Edge is used by default.
"""
from pathlib import Path
from urllib.parse import urlsplit, unquote
import argparse, json, datetime as dt
from playwright.sync_api import sync_playwright
ROOT=Path(__file__).resolve().parents[1]
CHAT='2026-11-18-theme-definition-e721eb90'

def check_pages(output, channel='msedge'):
    base=ROOT/'outputs'/CHAT
    output.mkdir(parents=True,exist_ok=True)
    shots=ROOT/'work'/CHAT/'browser-review'
    shots.mkdir(parents=True,exist_ok=True)
    result={'passed':False,'checked_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'scope':'Offline HTML rendering, interactions and existing map regression','pages':[],'errors':[]}
    paths=[base/'03-分析主题说明.html',base/'analysis/index.html',base/'report/城市出行离线数仓项目报告.html',base/'交付入口.html']
    with sync_playwright() as p:
        browser=p.chromium.launch(channel=channel,headless=True)
        page=browser.new_page(viewport={'width':1440,'height':1050})
        page.on('pageerror',lambda err:result['errors'].append(str(err)))
        for index,path in enumerate(paths):
            if not path.is_file():
                raise AssertionError('Required deliverable absent: '+str(path.relative_to(ROOT)))
            page.goto(path.as_uri(),wait_until='networkidle')
            page.evaluate('document.fonts.ready')
            page.evaluate('document.querySelectorAll("img").forEach(i=>i.loading="eager")')
            page.wait_for_function('Array.from(document.images).every(i=>i.complete)')
            assert page.locator('h1').count()>=1,path
            broken=page.locator('img').evaluate_all('(imgs)=>imgs.filter(i=>i.naturalWidth===0).map(i=>i.getAttribute("src"))')
            assert not broken,(path,broken)
            links=page.locator('a[href]').evaluate_all('(items)=>items.map(i=>i.getAttribute("href"))')
            checked=0
            for link in links:
                parsed=urlsplit(link)
                if parsed.scheme or not parsed.path or parsed.path.startswith('/'):
                    continue
                local=(path.parent/unquote(parsed.path)).resolve()
                assert local.is_file(),(path,link)
                checked+=1
            if index==0:
                assert page.locator('.question:visible').count()==12
                page.select_option('#theme-filter','T02');assert page.locator('.question:visible').count()==4
                page.fill('#question-search','覆盖率');assert page.locator('.question:visible').count()==1
                page.fill('#question-search','没有结果的词语');assert page.locator('#empty-result').is_visible()
                page.fill('#question-search','');page.select_option('#theme-filter','all')
            if index==1:
                assert page.locator('article[data-theme]:visible').count()==12
                page.select_option('#theme','T03');assert page.locator('article[data-theme]:visible').count()==3
                page.select_option('#theme','all')
                assert page.locator('img').count()==12
            page.screenshot(path=str(shots/f'{index+1}-desktop.png'),full_page=False)
            page.set_viewport_size({'width':390,'height':844})
            page.wait_for_timeout(100)
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1'), 'Mobile overflow: '+str(path)
            page.screenshot(path=str(shots/f'{index+1}-mobile.png'),full_page=False)
            page.set_viewport_size({'width':1440,'height':1050})
            result['pages'].append({'file':path.relative_to(ROOT).as_posix(),'local_links':checked,'images':page.locator('img').count(),'mobile_no_overflow':True})
        # The existing standalone map must remain usable after integration.
        page.goto((ROOT/'outputs/map/map.html').as_uri())
        page.locator('body[data-ready=true]').wait_for()
        assert page.locator('path.region').count()==262
        assert page.locator('#total').get_attribute('data-value')=='9012795'
        page.select_option('#transport','citibike')
        page.get_by_role('button',name='应用筛选',exact=True).click()
        assert page.locator('#total').get_attribute('data-value')=='4758458'
        result['map_regression']={'regions':262,'all_trips':9012795,'bike_filter':4758458,'passed':True}
        browser.close()
    result['passed']=not result['errors']
    (output/'page-verification.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    if not result['passed']:raise AssertionError(result['errors'])
    return result
if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,default=ROOT/'outputs'/CHAT/'verification')
    parser.add_argument('--channel',default='msedge')
    args=parser.parse_args()
    print(json.dumps(check_pages(args.output_dir,args.channel),ensure_ascii=False,indent=2))
