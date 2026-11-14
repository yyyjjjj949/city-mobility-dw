/* All values derive from one immutable, verified publication snapshot. */
(() => {
 'use strict';
 const $=id=>document.getElementById(id), fmt=n=>Number(n).toLocaleString('zh-CN');
 try {
  const data=window.MOBILITY_MAP;
  if(!data||!window.d3)throw Error('地图数据或本地绘图库未加载。请重新构建地图，或查看历史CSV。');
  const zones=new Map(data.zones.map(z=>[String(z.id),z]));
  const defaults={start:data.start,end:data.end,transport:'all',borough:'all',zone:'all'};
  let state={...defaults},zoom=1,currentRows=[];
  const colors=['#e9f1f5','#b4d4e5','#71abc9','#347fa7','#124e72'];
  const limits=[0,1000,10000,50000,150000];
  const fill=n=>n===0?'#e5e9ec':colors[n<1000?0:n<10000?1:n<50000?2:n<150000?3:4];
  const projection=d3.geoMercator().fitExtent([[24,18],[726,610]],data.geojson);
  const path=d3.geoPath(projection);
  $('snapshot').textContent=`生成于 ${new Date(data.generated_at_utc).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false})}（北京时间）`;
  $('provenance').textContent=`发布版本：${data.release_id}；来源与总量已在生成时校验。地图快照有效行程：${fmt(data.valid_trips)}。`;
  [...new Set(data.zones.map(z=>z.borough))].sort().forEach(b=>$('borough').add(new Option(b,b)));
  data.zones.slice().sort((a,b)=>a.name.localeCompare(b.name)).forEach(z=>$('zone').add(new Option(z.name+' · '+z.borough,String(z.id))));
  const group=d3.select('#regions');
  group.selectAll('path').data(data.geojson.features).join('path').attr('d',path).attr('class','region').attr('role','button').attr('tabindex',0)
   .attr('data-zone',f=>f.properties.LocationID).on('click',(event,f)=>choose(String(f.properties.LocationID)))
   .on('keydown',(event,f)=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();choose(String(f.properties.LocationID));}})
   .on('mouseenter',(event,f)=>detail(String(f.properties.LocationID))).on('focus',(event,f)=>detail(String(f.properties.LocationID))).append('title');
  function error(message){$('error').hidden=!message;$('error').textContent=message;}
  function valid(s){return /^2025-06-(0[1-9]|[12]\d|30)$/.test(s.start)&&/^2025-06-(0[1-9]|[12]\d|30)$/.test(s.end)&&s.start<=s.end&&['all','taxi_yellow','citibike'].includes(s.transport)&&($('borough').querySelector(`option[value="${CSS.escape(s.borough)}"]`))&&(s.zone==='all'||zones.has(s.zone))&&(s.zone==='all'||s.borough==='all'||zones.get(s.zone).borough===s.borough);}
  function sync(){Object.keys(defaults).forEach(k=>$(k).value=state[k]);}
  function restore(){const p=new URLSearchParams(location.hash.slice(1));const s=Object.fromEntries(Object.entries(defaults).map(([k,v])=>[k,p.get(k)||v]));if(valid(s)){state=s;error('');}else{state={...defaults};error('地址中的筛选条件无效，已恢复完整六月范围。');}sync();render();}
  function commit(s){if(!valid(s)){error('请选择2025年6月内的有效日期区间，开始日期不得晚于结束日期；所选区域需属于当前行政区。');return;}state=s;error('');const hash=new URLSearchParams(state).toString();history.pushState(null,'','#'+hash);sync();render();}
  function choose(id){const z=zones.get(id);if(!z||(state.borough!=='all'&&z.borough!==state.borough))return;commit({...state,zone:state.zone===id?'all':id});detail(id);}
  function detail(id){const z=zones.get(id),r=currentRows.find(r=>r.id===Number(id));if(z)$('detail').textContent=`${z.name} · ${z.borough} ｜ 当前区间出发量 ${fmt(r?r.trips:0)} 条${r?'':'（不在当前区域筛选内）'}`;}
  function render(){
   const selected=data.rows.filter(r=>r[0]>=state.start&&r[0]<=state.end&&(state.transport==='all'||r[1]===state.transport));
   const counts=new Map(), daily=new Map();
   for(const r of selected){const z=zones.get(String(r[2]));if(!z||(state.borough!=='all'&&z.borough!==state.borough)||(state.zone!=='all'&&String(r[2])!==state.zone))continue;counts.set(r[2],(counts.get(r[2])||0)+r[3]);daily.set(r[0],(daily.get(r[0])||0)+r[3]);}
   currentRows=data.zones.filter(z=>(state.borough==='all'||z.borough===state.borough)&&(state.zone==='all'||String(z.id)===state.zone)).map(z=>({...z,trips:counts.get(z.id)||0})).sort((a,b)=>b.trips-a.trips||a.id-b.id);
   const globals=data.totals.filter(r=>r.date>=state.start&&r.date<=state.end&&(state.transport==='all'||r.transport_code===state.transport));
   const total=d3.sum(globals,r=>r.trips),mapped=d3.sum(globals,r=>r.mapped),n=d3.sum(currentRows,r=>r.trips);
   $('mapped').textContent=fmt(n);$('mapped').dataset.value=String(n);$('total').textContent=fmt(total);$('total').dataset.value=String(total);
   $('coverage').textContent=total?(mapped/total*100).toFixed(2)+'%':'不可用';$('unmapped').textContent=`未映射至纽约区域 ${fmt(total-mapped)} 条`;
   $('active').textContent=`${state.start} 至 ${state.end}（含） · ${$('transport').selectedOptions[0].text} · ${$('borough').selectedOptions[0].text} · ${state.zone==='all'?'全部区域':zones.get(state.zone).name}`;
   const ids=new Set(currentRows.map(r=>r.id));
   group.selectAll('path').attr('fill',f=>fill(counts.get(f.properties.LocationID)||0)).classed('dim',f=>!ids.has(f.properties.LocationID)).classed('selected',f=>String(f.properties.LocationID)===state.zone)
    .attr('aria-pressed',f=>String(f.properties.LocationID)===state.zone?'true':'false').attr('aria-label',f=>`${zones.get(String(f.properties.LocationID)).name}，出发量 ${fmt(counts.get(f.properties.LocationID)||0)} 条`).select('title').text(f=>`${zones.get(String(f.properties.LocationID)).name}：${fmt(counts.get(f.properties.LocationID)||0)} 条`);
   $('legend').replaceChildren();[['#e5e9ec','0条'],...colors.map((c,i)=>[c,`${fmt(Math.max(1,limits[i]))}${i===4?'条及以上':'–'+fmt(limits[i+1]-1)+'条'}`])].forEach(([c,t])=>{const span=document.createElement('span'),i=document.createElement('i');i.style.background=c;span.append(i,document.createTextNode(t));$('legend').append(span);});
   $('ranking').replaceChildren();const max=currentRows[0]?.trips||1;
   currentRows.filter(r=>r.trips>0).slice(0,15).forEach((r,i)=>{const li=document.createElement('li'),b=document.createElement('button'),name=document.createElement('span'),value=document.createElement('b');name.className='rank-name';name.textContent=`${i+1}. ${r.name}`;value.textContent=fmt(r.trips);b.style.setProperty('--bar',r.trips/max*100+'%');b.append(name,value);b.addEventListener('click',()=>choose(String(r.id)));li.append(b);$('ranking').append(li);});$('empty').hidden=n>0;
   $('trend').replaceChildren();const dates=data.dates.filter(d=>d>=state.start&&d<=state.end),peak=Math.max(1,...dates.map(d=>daily.get(d)||0));
   dates.forEach((date,i)=>{const bar=document.createElement('div'),label=document.createElement('span'),v=daily.get(date)||0;bar.className='day-bar';bar.style.height=(v/peak*140)+'px';bar.title=`${date}：${fmt(v)} 条`;bar.setAttribute('aria-label',bar.title);if(dates.length<=16||i%3===0){label.textContent=date.slice(-2);bar.append(label);}$('trend').append(bar);});
   $('detail').textContent=state.zone==='all'?'选择地图区域查看精确数值。':`${zones.get(state.zone).name} ｜ 出发量 ${fmt(n)} 条`;
   document.body.dataset.ready='true';
  }
  $('filters').addEventListener('submit',e=>{e.preventDefault();commit(Object.fromEntries(Object.keys(defaults).map(k=>[k,$(k).value])));});
  $('borough').addEventListener('change',()=>{$('zone').value='all';});
  $('reset').addEventListener('click',()=>{zoom=1;group.attr('transform',null);commit({...defaults});});
  const zoomTo=v=>{zoom=Math.min(4,Math.max(1,v));group.attr('transform',`translate(${375*(1-zoom)},${315*(1-zoom)}) scale(${zoom})`);};
  $('zoom-in').onclick=()=>zoomTo(zoom*1.3);$('zoom-out').onclick=()=>zoomTo(zoom/1.3);$('zoom-reset').onclick=()=>zoomTo(1);
  $('export').onclick=()=>{const safe=s=>'"'+String(s).replaceAll('"','""')+'"';const csv='\ufeff'+[['区域编号','区域','行政区','开始日期','结束日期','交通方式','出发量'],...currentRows.map(r=>[r.id,r.name,r.borough,state.start,state.end,state.transport,r.trips])].map(r=>r.map(safe).join(',')).join('\r\n');const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'}));a.download='区域出发量-'+state.start+'-'+state.end+'.csv';a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000);};
  window.addEventListener('popstate',restore);window.addEventListener('hashchange',restore);restore();
 }catch(e){$('error').hidden=false;$('error').textContent='无法加载地图：'+e.message;document.body.dataset.ready='error';}
})();
