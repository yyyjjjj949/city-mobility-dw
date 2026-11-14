const {chromium}=require('D:/CodexData/cache/npm/_npx/31e32ef8478fbf80/node_modules/playwright');
const fs=require('fs'),path=require('path'),assert=require('assert');
const {pathToFileURL}=require('url');
const base='D:/CodexData/workspaces/city-mobility-share-2026-11-14';
const root=process.env.MOBILITY_PREVIEW_ROOT||base+'/work/CityMobility-Preview';
(async()=>{
 const browser=await chromium.launch({channel:'msedge',headless:true});
 const result={passed:false,errors:[],pages:[]};
 try{
  const p=await browser.newPage({viewport:{width:1440,height:1100}});
  await p.route('https://**/*',r=>r.abort());p.on('pageerror',e=>result.errors.push(e.message));
  for(const file of ['打开展示.html','outputs/dwd/六月数据清洗结果.html','docs/diagrams/设计图预览.html']){
   await p.goto(pathToFileURL(path.join(root,file)).href);
   if(file==='打开展示.html')await p.waitForURL('**/outputs/**');
   const links=await p.locator('a').evaluateAll(es=>es.map(e=>e.getAttribute('href')));
   const actual=file==='打开展示.html'?'outputs/项目总入口.html':file;
   const missing=links.filter(h=>h&&!/^(https?:|#|mailto:)/.test(h)&&!fs.existsSync(path.resolve(root,path.dirname(actual),decodeURIComponent(h.split('#')[0]))));
   assert.deepEqual(missing,[]);result.pages.push({file,linksValid:true});
  }
  await p.goto(pathToFileURL(root+'/outputs/map/map.html').href);
  await p.locator('body[data-ready=true]').waitFor();
  assert.equal(await p.locator('path.region').count(),262);
  assert.equal(await p.locator('#total').getAttribute('data-value'),'9012795');
  await p.locator('#transport').selectOption('citibike');
  await p.getByRole('button',{name:'应用筛选',exact:true}).click();
  assert.equal(await p.locator('#total').getAttribute('data-value'),'4758458');
  await p.screenshot({path:base+'/outputs/展示包实测.png',fullPage:true});
  result.mapRegions=262;result.filterTripCount=4758458;result.passed=result.errors.length===0;
 }finally{await browser.close();fs.writeFileSync(base+'/outputs/preview-package-verification.json',JSON.stringify(result,null,2));}
 console.log(JSON.stringify(result));if(!result.passed)process.exitCode=1;
})().catch(e=>{console.error(e);process.exit(1)});
