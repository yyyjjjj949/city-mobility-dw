import * as cheerio from 'cheerio';
export const ORIGIN='https://bustimes.org';
export const USER_AGENT='CityMobilityCourseCrawler/0.1 (educational timetable page collection)';
export const SEED=ORIGIN+'/localities/cambridge-cambs';
const clean=s=>String(s??'').replace(/\s+/g,' ').trim();
export function validateTarget(value,kind='service') {
  const u=new URL(value,ORIGIN);
  if(u.origin!==ORIGIN||u.username||u.password||u.search||u.hash)throw new Error('Target outside fixed public HTML scope');
  const allowed=kind==='robots'?u.pathname==='/robots.txt':kind==='index'?u.pathname==='/localities/cambridge-cambs':/^\/services\/[a-zA-Z0-9-]+$/.test(u.pathname);
  if(!allowed)throw new Error('Target is not an allowed index or service HTML page');
  return u.href;
}
export function parseIndex(html) {
 const $=cheerio.load(html),links=new Set();
 $('a[href^="/services/"]').each((_,el)=>{try{links.add(validateTarget($(el).attr('href')))}catch{}});
 if(!links.size)throw new Error('No service links found in locality HTML');
 return {title:clean($('h1').first().text()),service_urls:[...links]};
}
export function parseService(html,url) {
 validateTarget(url);
 const $=cheerio.load(html),heading=$('h1.service-header');
 if(!heading.length)throw new Error('Expected a bus service detail page');
 const selected=$('select[name="date"] option[selected]').first();
 const calendar=$('select[name="calendar"] option[selected]').first();
 const calendarText=clean($('#timetable > noscript').first().text());
 const dateValue=selected.attr('value')||(/^\d{4}-\d{2}-\d{2}$/.test(calendarText)?calendarText:null);
 const calendarLabel=clean(calendar.text())||(!dateValue&&calendarText?calendarText:null);
 const operatorLinks=heading.nextAll('p').first().find('a[href^="/operators/"]');
 const tables=[];
 $('table.timetable').each((tableIndex,table)=>{
   const rows=[],pending=new Map();
   $(table).find('tbody tr').each((physicalRow,tr)=>{
     const stop=$(tr).find('th.stop-name a[href^="/stops/"]').first();
     const occupied=new Set(pending.keys()),nextPending=new Map();
     for(const [col,remaining] of pending)if(remaining>1)nextPending.set(col,remaining-1);
     const cells=[];let column=0;
     $(tr).children('td').each((cellIndex,td)=>{
       while(occupied.has(column))column++;
       const span=Number($(td).attr('colspan')||1),rowspan=Number($(td).attr('rowspan')||1);
       if(!Number.isInteger(span)||span<1||span>1000||!Number.isInteger(rowspan)||rowspan<1||rowspan>1000)throw new Error('Invalid HTML table span');
       for(let offset=0;offset<span;offset++){if(occupied.has(column+offset))throw new Error('Overlapping HTML spans');if(rowspan>1)nextPending.set(column+offset,rowspan-1);}
       cells.push({source_cell_index:cellIndex+1,column_start:column+1,column_span:span,row_span:rowspan,raw_text:clean($(td).text()),html_title:$(td).attr('title')||null});
       column+=span;
     });
     pending.clear();for(const [col,remaining] of nextPending)pending.set(col,remaining);
     if(stop.length)rows.push({source_row_index:physicalRow+1,stop_sequence:rows.length+1,stop_id:stop.attr('href').split('/').pop(),stop_name:clean(stop.text()),stop_url:ORIGIN+stop.attr('href'),source_column_count:Math.max(column,...[...occupied].map(x=>x+1)),cells});
   });
   if(rows.length)tables.push({table_index:tableIndex+1,direction_label:clean($(table).closest('.grouping').children('h2').first().text())||null,rows});
 });
 if(!tables.length)throw new Error('Service page has no parseable timetable; no rows manufactured');
 return {source_url:url,service_slug:new URL(url).pathname.split('/').pop(),route_name:clean(heading.find('.name').text()),description:clean(heading.find('.description').text()),operators:operatorLinks.map((_,e)=>({name:clean($(e).text()),source_path:$(e).attr('href')})).get(),schedule_date:dateValue,schedule_date_label:clean(selected.text())||null,calendar_id:calendar.attr('value')||null,calendar_label:calendarLabel,schedule_scope:dateValue?'explicit_date':calendarLabel?'service_calendar':'unspecified',attribution:clean($('#timetable .credit').text())||null,tables};
}
