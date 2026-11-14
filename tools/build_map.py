"""Export a self-contained, versioned map from the validated ClickHouse release."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import shutil
from publish_marts import ClickHouse

ROOT=Path(__file__).resolve().parents[1]


def main():
    ch=ClickHouse()
    pointers=ch.query("SELECT release_id FROM ads.release_pointer WHERE dataset_id='historical'")
    assert len(pointers)==1
    release=pointers[0]['release_id']
    # Pin every query to the same release; a concurrent atomic publication cannot mix versions.
    zones=ch.query(f"SELECT zone_key AS id,any(zone_name) AS name,any(borough) AS borough FROM ads.zone_hour WHERE release_id='{release}' GROUP BY zone_key ORDER BY zone_key")
    rows=ch.query(f"SELECT toString(event_date) AS date,transport_code,zone_key,sum(departure_count) AS trips FROM ads.zone_hour WHERE release_id='{release}' GROUP BY event_date,transport_code,zone_key ORDER BY date,transport_code,zone_key")
    totals=ch.query(f"SELECT toString(service_date) AS date,transport_code,sum(trip_count) AS trips,sum(origin_mapped_count) AS mapped FROM ads.trip_day WHERE release_id='{release}' GROUP BY service_date,transport_code ORDER BY date,transport_code")
    assert sum(int(r['trips']) for r in rows)==sum(int(t['mapped']) for t in totals)
    assert sum(int(t['trips']) for t in totals)==9012795
    geometry=json.loads((ROOT/'work/dwd/zones.geojson').read_text(encoding='utf-8'))
    ids={int(z['id']) for z in zones}
    features=[]
    for feature in geometry['features']:
        zone_id=int(feature['properties']['LocationID'])
        if zone_id not in ids:continue
        feature['properties']={'LocationID':zone_id}
        features.append(feature)
    assert len(ids)==len(features)==262 and {f['properties']['LocationID'] for f in features}==ids
    dates=sorted({t['date'] for t in totals})
    data={'release_id':release,'generated_at_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
          'start':dates[0],'end':dates[-1],'dates':dates,'valid_trips':sum(int(t['trips']) for t in totals),
          'zones':[{**z,'id':int(z['id'])} for z in zones],
          'rows':[[r['date'],r['transport_code'],int(r['zone_key']),int(r['trips'])] for r in rows],
          'totals':[{**t,'trips':int(t['trips']),'mapped':int(t['mapped'])} for t in totals],
          'geojson':{'type':'FeatureCollection','features':features}}
    out=ROOT/'outputs/map';out.mkdir(parents=True,exist_ok=True)
    for name in ['map.html','map.css','map.js','d3.min.js','D3-LICENSE']:
        shutil.copyfile(ROOT/'web'/name,out/name)
    payload=json.dumps(data,ensure_ascii=False,separators=(',',':'))
    temp=out/'map-data.tmp';temp.write_text('window.MOBILITY_MAP='+payload+';\n',encoding='utf-8');temp.replace(out/'map-data.js')
    report={'passed':True,'release_id':release,'checked_at_utc':data['generated_at_utc'],'zones':len(ids),
            'daily_zone_rows':len(rows),'valid_trips':data['valid_trips'],'mapped_departures':sum(r[3] for r in data['rows']),
            'file_sha256':hashlib.sha256((out/'map-data.js').read_bytes()).hexdigest(),'local_dependencies_only':True}
    (ROOT/'outputs/map-build-verification.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print('MAP_BUILD_OK '+json.dumps(report))


if __name__=='__main__':main()
