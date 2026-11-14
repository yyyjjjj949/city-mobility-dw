"""Official zone reprojection and indexed endpoint matching; no coordinate imputation."""
import hashlib
import json
import math
import sys
import zipfile
from pathlib import Path

DEPS = '/workspace/work/dwd/python-deps-np126'
if DEPS not in sys.path:
    sys.path.insert(0, DEPS)


def prepare_zones(archive, expected_sha, output):
    import shapefile
    import pyproj
    import shapely
    from shapely.geometry import shape, mapping
    from shapely.ops import transform, unary_union

    assert hashlib.sha256(Path(archive).read_bytes()).hexdigest() == expected_sha
    folder = Path(output).parent / 'official-shapefile'
    folder.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as z:
        for name in z.namelist():
            target = (folder / name).resolve()
            if not target.is_relative_to(folder.resolve()):
                raise ValueError('Unsafe shapefile member')
        z.extractall(folder)
    files = list(folder.rglob('*.shp'))
    assert len(files) == 1
    prj = files[0].with_suffix('.prj').read_text()
    source = pyproj.CRS.from_wkt(prj)
    assert source.equals(pyproj.CRS('EPSG:2263'), ignore_axis_order=True), source
    transformer = pyproj.Transformer.from_crs(source, 'EPSG:4326', always_xy=True)
    grouped = {}
    for record in shapefile.Reader(str(files[0])).iterShapeRecords():
        props = record.record.as_dict()
        loc = int(props['LocationID'])
        geometry = transform(transformer.transform, shape(record.shape.__geo_interface__))
        if not geometry.is_valid:
            raise ValueError('Invalid official geometry: ' + str(loc))
        grouped.setdefault(loc, []).append(geometry)
    features = []
    for loc, parts in sorted(grouped.items()):
        geometry = unary_union(parts)
        assert geometry.is_valid and not geometry.is_empty
        features.append({'type': 'Feature', 'properties': {'LocationID': loc}, 'geometry': mapping(geometry)})
    data = {'type': 'FeatureCollection', 'features': features,
            'provenance': {'source_crs': 'EPSG:2263', 'target_crs': 'EPSG:4326',
                           'source_sha256': expected_sha, 'always_xy': True,
                           'shapely': shapely.__version__, 'pyproj': pyproj.__version__}}
    Path(output).write_text(json.dumps(data, ensure_ascii=False, separators=(',', ':')))
    return data


class ZoneMatcher:
    def __init__(self, features):
        from shapely.geometry import shape
        from shapely.strtree import STRtree
        self.ids = [int(f['properties']['LocationID']) for f in features]
        self.tree = STRtree([shape(f['geometry']) for f in features])

    def match(self, lat, lon):
        from shapely.geometry import Point
        if lat is None or lon is None or not math.isfinite(lat) or not math.isfinite(lon) or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return 0, 0
        hits = {self.ids[int(i)] for i in self.tree.query(Point(lon, lat), predicate='covered_by')}
        return (min(hits), len(hits)) if hits else (0, 0)


_MATCHER = None


def match_endpoints(a, b, c, d):
    global _MATCHER
    if _MATCHER is None:
        path = Path('/workspace/work/dwd/zones.geojson')
        _MATCHER = ZoneMatcher(json.loads(path.read_text())['features'])
    origin, n1 = _MATCHER.match(a, b)
    destination, n2 = _MATCHER.match(c, d)
    return origin, destination, n1 > 1 or n2 > 1
