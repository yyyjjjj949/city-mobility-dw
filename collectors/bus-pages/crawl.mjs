import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
import robotsParser from 'robots-parser';
import {ORIGIN,USER_AGENT,SEED,validateTarget,parseIndex,parseService} from './parse.mjs';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..');
const chat='2026-11-18-theme-definition-e721eb90';
const args=process.argv.slice(2),option=(flag,fallback)=>{const n=args.indexOf(flag);if(n<0)return fallback;if(!args[n+1]||args[n+1].startsWith('--'))throw new Error('Missing option value');return args[n+1]};
const max=Number(option('--max-pages','20'));
if(!Number.isInteger(max)||max<1||max>100)throw new Error('max-pages must be 1..100');
const work=path.resolve(option('--work-root',path.join(root,'work',chat,'business-page-crawl')));
const output=path.resolve(option('--output-root',path.join(root,'outputs',chat,'business-page-crawl')));
if(!/^[dD]:[\\/]/.test(work)||!/^[dD]:[\\/]/.test(output))throw new Error('Generated files must remain on D:');
const runId=new Date().toISOString().replace(/[-:]/g,'').replace(/\.\d+Z$/,'Z')+'-'+crypto.randomBytes(3).toString('hex');
const run=path.join(work,runId),pub=path.join(output,runId);
await fs.mkdir(path.join(run,'responses'),{recursive:true});await fs.mkdir(pub,{recursive:true});
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const now=()=>new Date().toISOString();
const report={schema_version:1,run_id:runId,started_at_utc:now(),collector:'Node.js fetch + Cheerio HTML parser',user_agent:USER_AGENT,seed_url:SEED,scope:'Service links listed on Cambridge locality page; default displayed schedule date or service calendar only; includes services extending beyond the city',bulk_dataset_downloads:0,api_requests:0,requests:[],successful_pages:[],failed_pages:[],discovered_service_urls:[],data_files:{},passed:false};
let last=0,sequence=0,delay=2200,robots;
async function fetchPage(url,kind) {
 validateTarget(url,kind);
 if(kind!=='robots'&&robots.isAllowed(url,USER_AGENT)!==true)throw new Error('Robots policy did not allow target');
 await new Promise(r=>setTimeout(r,Math.max(0,delay-(Date.now()-last))));last=Date.now();
 const started=now();
 let response;try{response=await fetch(url,{redirect:'manual',headers:{'User-Agent':USER_AGENT,'Accept':'text/html,text/plain;q=0.9'},signal:AbortSignal.timeout(22000)})}catch(e){report.requests.push({url,kind,requested_at_utc:started,error_type:e.name});throw new Error('Network request failed; details withheld')}
 const entry={url,kind,requested_at_utc:started,status:response.status,content_type:response.headers.get('content-type'),etag:response.headers.get('etag'),last_modified:response.headers.get('last-modified')};report.requests.push(entry);
 if(response.status!==200) {await response.body?.cancel();throw new Error('HTTP '+response.status+'; no redirect or access-control bypass')}
 if(kind!=='robots'&&!entry.content_type?.includes('text/html')){await response.body?.cancel();throw new Error('Expected business HTML, not a dataset file')}
 const chunks=[];let bytes=0;for await(const chunk of response.body){bytes+=chunk.length;if(bytes>3*1024*1024){await response.body.cancel().catch(()=>{});throw new Error('HTML response exceeded 3 MiB limit')}chunks.push(chunk)}
 const body=Buffer.concat(chunks);entry.response_bytes=body.length;entry.sha256=sha(body);entry.body_file='responses/'+String(++sequence).padStart(3,'0')+'.html';await fs.writeFile(path.join(run,entry.body_file),body);return {html:body.toString('utf8'),entry};
}
try {
 const policy=await fetchPage(ORIGIN+'/robots.txt','robots');robots=robotsParser(ORIGIN+'/robots.txt',policy.html);const crawlDelay=robots.getCrawlDelay(USER_AGENT);if(crawlDelay)delay=Math.max(delay,crawlDelay*1000);
 report.robots={status:200,sha256:policy.entry.sha256,minimum_request_interval_ms:delay};
 const index=await fetchPage(SEED,'index'),listing=parseIndex(index.html);report.index_title=listing.title;report.discovered_service_urls=listing.service_urls;
 const selected=listing.service_urls.slice(0,max);report.selected_service_count=selected.length;report.selection_truncated=selected.length<listing.service_urls.length;
 for(const url of selected) {
  try {
   const {html,entry}=await fetchPage(url,'service'),data=parseService(html,url);data.snapshot_run_id=runId;data.fetched_at_utc=entry.requested_at_utc;data.source_response_sha256=entry.sha256;data.source_response_file=entry.body_file;
   const filename='service-'+String(report.successful_pages.length+1).padStart(3,'0')+'.json';await fs.writeFile(path.join(run,filename),JSON.stringify(data));
   const rows=data.tables.flatMap(t=>t.rows),nonempty=rows.flatMap(r=>r.cells).filter(c=>c.raw_text);
   report.successful_pages.push({url,service_slug:data.service_slug,route_name:data.route_name,schedule_date:data.schedule_date,calendar_id:data.calendar_id,calendar_label:data.calendar_label,schedule_scope:data.schedule_scope,tables:data.tables.length,stop_rows:rows.length,nonempty_time_cells:nonempty.length,parsed_file:filename,source_response_sha256:entry.sha256,operators:data.operators,attribution:data.attribution});
   console.log(JSON.stringify({event:'page_parsed',route:data.route_name,completed:report.successful_pages.length,selected:selected.length,stop_rows:rows.length,nonempty_time_cells:nonempty.length}));
  }catch(e){report.failed_pages.push({url,error:e.message});console.log(JSON.stringify({event:'page_failed',url,error:e.message}));if(report.requests.at(-1)?.status===429){report.stopped_for_rate_limit=true;break;}}
 }
 report.passed=report.successful_pages.length>0&&report.failed_pages.length===0&&report.successful_pages.length===selected.length;
}catch(e){report.fatal_error=e.message;}
report.finished_at_utc=now();report.successful_service_pages=report.successful_pages.length;report.failed_service_pages=report.failed_pages.length;report.stop_rows=report.successful_pages.reduce((s,p)=>s+p.stop_rows,0);report.nonempty_time_cells=report.successful_pages.reduce((s,p)=>s+p.nonempty_time_cells,0);report.data_files.local_work_run_directory=path.relative(root,run).replaceAll('\\','/');
await fs.writeFile(path.join(run,'crawl-manifest.json'),JSON.stringify(report,null,2)+'\n');await fs.writeFile(path.join(pub,'crawl-manifest.json'),JSON.stringify(report,null,2)+'\n');await fs.writeFile(path.join(output,'latest.json'),JSON.stringify({run_id:runId,manifest:runId+'/crawl-manifest.json',passed:report.passed},null,2)+'\n');
console.log(JSON.stringify({event:'complete',passed:report.passed,run_id:runId,pages:report.successful_service_pages,discovered:report.discovered_service_urls.length,stop_rows:report.stop_rows,nonempty_time_cells:report.nonempty_time_cells,work_run:run,public_receipt:pub}));if(!report.passed)process.exitCode=1;
