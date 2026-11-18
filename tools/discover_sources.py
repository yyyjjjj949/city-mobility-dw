"""Discover official monthly data links from HTML and a public object listing.

No business data is invented. Downloads/ODS remain in ingest_sources.py.
All generated evidence defaults to this chat's D: work/output folders.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import time
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser
import uuid
import xml.etree.ElementTree as ET

import requests

ROOT = Path(__file__).resolve().parents[1]
CHAT = '2026-11-18-theme-definition-e721eb90'
DEFAULT_WORK = ROOT / 'work' / CHAT / 'source-discovery'
DEFAULT_OUTPUT = ROOT / 'outputs' / CHAT / 'crawl'
USER_AGENT = 'CityMobilityCourseProject/1.1 (official-open-data-coursework)'
ALLOWED_HOSTS = frozenset({'www.nyc.gov', 'www1.nyc.gov', 'citibikenyc.com',
    's3.amazonaws.com', 'tripdata.s3.amazonaws.com', 'd37ci6vzurychx.cloudfront.net'})
TLC_PAGE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
BIKE_PAGE = 'https://citibikenyc.com/system-data'
BIKE_LISTING = 'https://tripdata.s3.amazonaws.com/'
S3_NS = '{http://s3.amazonaws.com/doc/2006-03-01/}'


class DiscoveryError(RuntimeError):
    """A safe-to-log discovery failure without transport/proxy secrets."""


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def validate_month(month):
    if not re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])', month):
        raise DiscoveryError('Month must use YYYY-MM')
    return month


def validate_url(url):
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise DiscoveryError('Invalid URL port') from exc
    if (parsed.scheme != 'https' or parsed.hostname not in ALLOWED_HOSTS
            or parsed.username is not None or parsed.password is not None
            or port not in (None, 443)):
        raise DiscoveryError('URL is outside the HTTPS official-source allowlist')
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ''))


class Links(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == 'a':
            self.hrefs.extend(v for k, v in attrs if k.lower() == 'href' and v)


def parse_links(html, page_url):
    parser = Links()
    parser.feed(html)
    links = set()
    for href in parser.hrefs:
        try:
            links.add(urljoin(page_url, href.strip()))
        except ValueError:
            # Unrelated malformed site navigation must not crash a source run.
            continue
    return sorted(links)


def parse_tlc(html, month, page_url=TLC_PAGE):
    validate_month(month)
    target = f'/trip-data/yellow_tripdata_{month}.parquet'
    matches = []
    for link in parse_links(html, page_url):
        if urlsplit(link).path == target:
            valid = validate_url(link)
            if urlsplit(valid).hostname != 'd37ci6vzurychx.cloudfront.net':
                raise DiscoveryError('TLC target link is on an unexpected host')
            matches.append(valid)
    if len(set(matches)) != 1:
        raise DiscoveryError('Expected exactly one TLC target-month link in official HTML')
    return matches[0]


def parse_bike_index(html):
    links = parse_links(html, BIKE_PAGE)
    for link in links:
        p = urlsplit(link)
        if ((p.hostname == 's3.amazonaws.com' and p.path.rstrip('/') in ('/tripdata', '/tripdata/index.html'))
                or (p.hostname == 'tripdata.s3.amazonaws.com' and p.path in ('', '/', '/index.html'))):
            return validate_url(link)
    raise DiscoveryError('Official Citi Bike HTML does not link to its public tripdata bucket')


def parse_bike_listing(payload, month):
    validate_month(month)
    if b'<!DOCTYPE' in payload.upper() or b'<!ENTITY' in payload.upper():
        raise DiscoveryError('DTD/entity declarations are not allowed in listing XML')
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise DiscoveryError('Malformed public bucket listing XML') from exc
    if root.tag != S3_NS + 'ListBucketResult':
        raise DiscoveryError('Unexpected public bucket XML root/namespace')
    if root.findtext(S3_NS + 'Name') != 'tripdata':
        raise DiscoveryError('Unexpected public bucket name')
    expected_key = month.replace('-', '') + '-citibike-tripdata.zip'
    matches = []
    for entry in root.findall(S3_NS + 'Contents'):
        key = entry.findtext(S3_NS + 'Key', '')
        if key == expected_key:
            try:
                size = int(entry.findtext(S3_NS + 'Size', '0'))
            except ValueError as exc:
                raise DiscoveryError('Invalid object byte size') from exc
            if size <= 0:
                raise DiscoveryError('Target object has no positive byte size')
            matches.append({'url': 'https://s3.amazonaws.com/tripdata/' + key,
                'key': key, 'bytes': size,
                'etag': entry.findtext(S3_NS + 'ETag'),
                'last_modified': entry.findtext(S3_NS + 'LastModified')})
    truncated = root.findtext(S3_NS + 'IsTruncated')
    if truncated not in ('true', 'false'):
        raise DiscoveryError('Missing/invalid listing pagination flag')
    token = root.findtext(S3_NS + 'NextContinuationToken')
    if truncated == 'true' and not token:
        raise DiscoveryError('Truncated listing has no continuation token')
    return matches, token if truncated == 'true' else None


class DiscoveryClient:
    """Bounded crawler, explicit proxy, robots policy and source evidence."""
    def __init__(self, work_dir, proxy=None, session=None, interval=0.6, sleeper=time.sleep):
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.session.headers.update({'User-Agent': USER_AGENT, 'Accept-Encoding': 'identity'})
        if proxy:
            self.session.proxies.update({'https': proxy, 'http': proxy})
        self.interval = interval
        self.sleeper = sleeper
        self.last_request = 0.0
        self.robots = {}
        self.evidence = []

    def _wait(self):
        elapsed = time.monotonic() - self.last_request
        if elapsed < self.interval:
            self.sleeper(self.interval - elapsed)
        self.last_request = time.monotonic()

    def _record(self, url, status, body, content_type):
        seq = len(self.evidence) + 1
        extension = '.xml' if 'xml' in content_type else '.html' if 'html' in content_type else '.txt'
        filename = f'{seq:03d}-response{extension}'
        (self.work_dir / filename).write_bytes(body)
        item = {'request_url': url, 'received_at_utc': now(), 'status': status,
            'content_type': content_type, 'bytes': len(body),
            'sha256': hashlib.sha256(body).hexdigest(), 'raw_file': filename}
        self.evidence.append(item)
        (self.work_dir / 'requests.json').write_text(json.dumps(self.evidence, ensure_ascii=False, indent=2), encoding='utf-8')

    def _request(self, url, *, allowed_status=(200,), redirects=0, is_robots=False):
        url = validate_url(url)
        if redirects > 3:
            raise DiscoveryError('Too many redirects')
        for attempt in range(2):
            self._wait()
            try:
                response = self.session.get(url, timeout=(10, 25), stream=True, allow_redirects=False)
                try:
                    body = bytearray()
                    for chunk in response.iter_content(65536):
                        body.extend(chunk)
                        if len(body) > 8 * 1024 * 1024:
                            raise DiscoveryError('Discovery response exceeds 8MiB limit')
                    self._record(url, response.status_code, bytes(body), response.headers.get('Content-Type', ''))
                    if response.status_code in (301, 302, 303, 307, 308):
                        target = validate_url(urljoin(url, response.headers.get('Location', '')))
                        # Each redirect target is checked against its own robots policy.
                        if not is_robots:
                            self.check_robots(target)
                        return self._request(target, allowed_status=allowed_status, redirects=redirects + 1, is_robots=is_robots)
                    if response.status_code in allowed_status:
                        return bytes(body), response.status_code
                    if response.status_code in (429, 500, 502, 503, 504) and attempt == 0:
                        self.sleeper(1)
                        continue
                    raise DiscoveryError(f'Official endpoint returned HTTP {response.status_code}')
                finally:
                    response.close()
            except requests.RequestException as exc:
                if attempt == 0:
                    self.sleeper(1)
                    continue
                # Deliberately omit str(exc): a proxy URL could contain credentials.
                raise DiscoveryError('Official endpoint request failed: ' + type(exc).__name__) from exc
        raise DiscoveryError('Discovery request failed')

    def check_robots(self, url):
        p = urlsplit(validate_url(url))
        origin = f'{p.scheme}://{p.netloc}'
        if origin not in self.robots:
            payload, status = self._request(origin + '/robots.txt', allowed_status=(200, 404, 410), is_robots=True)
            policy = RobotFileParser()
            if status in (404, 410):
                policy.parse([])
            else:
                policy.parse(payload.decode('utf-8', errors='replace').splitlines())
            self.robots[origin] = policy
        if not self.robots[origin].can_fetch(USER_AGENT, url):
            raise DiscoveryError('robots.txt disallows the requested official URL')

    def get(self, url):
        self.check_robots(url)
        return self._request(url)[0]


def expected_objects(month):
    catalog = json.loads((ROOT / 'docs/data-source-catalog.json').read_text(encoding='utf-8'))
    result = {}
    for source in catalog['sources']:
        if source['id'] in ('tlc_yellow', 'citibike_trips'):
            entries = [obj for obj in source.get('objects', []) if obj.get('month') == month]
            if len(entries) != 1:
                raise DiscoveryError('Requested month is not in the validated source catalog')
            result[source['id']] = entries[0]
    return result


def discover(month='2025-06', sources=('citibike_trips',), *, proxy=None,
             work_dir=DEFAULT_WORK, output_dir=DEFAULT_OUTPUT, client=None):
    validate_month(month)
    unknown = set(sources) - {'tlc_yellow', 'citibike_trips'}
    if unknown:
        raise DiscoveryError('Unsupported discovery source')
    run_id = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
    run_work = Path(work_dir) / run_id
    run_work.mkdir(parents=True, exist_ok=True)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    client = client or DiscoveryClient(run_work, proxy=proxy)
    expected = expected_objects(month)
    report = {'schema_version': 1, 'run_id': run_id, 'month': month, 'started_at_utc': now(),
        'method': 'official HTML anchor parsing + public bucket XML listing parsing',
        'requested_sources': list(sources), 'results': [],
        'policy': {'https_host_allowlist': sorted(ALLOWED_HOSTS), 'robots': '200: parse rules; 404/410: no rules; all other errors: fail closed',
                   'min_request_interval_seconds': client.interval, 'max_attempts_per_request': 2,
                   'max_listing_pages': 10, 'max_response_bytes': 8 * 1024 * 1024,
                   'catalog_identity_must_match': True},
        'raw_evidence_directory': str(client.work_dir.relative_to(ROOT)) if client.work_dir.is_relative_to(ROOT) else str(client.work_dir)}
    for source in sources:
        item = {'source_id': source, 'status': 'failed'}
        try:
            if source == 'tlc_yellow':
                payload = client.get(TLC_PAGE)
                url = parse_tlc(payload.decode('utf-8', errors='replace'), month)
                discovered = {'url': url, 'landing_page': TLC_PAGE, 'parser': 'HTML a[href] exact month/file'}
            else:
                html = client.get(BIKE_PAGE).decode('utf-8', errors='replace')
                index = parse_bike_index(html)
                matches = []; token = None; visited = set(); pages = 0
                while pages < 10:
                    params = {'list-type': '2', 'prefix': month.replace('-', ''), 'max-keys': 1000}
                    if token:
                        params['continuation-token'] = token
                    page = client.get(BIKE_LISTING + '?' + urlencode(params))
                    found, token = parse_bike_listing(page, month)
                    matches.extend(found); pages += 1
                    if not token:
                        break
                    if token in visited:
                        raise DiscoveryError('Repeated listing continuation token')
                    visited.add(token)
                else:
                    raise DiscoveryError('Public listing exceeds 10-page bound')
                if len(matches) != 1:
                    raise DiscoveryError('Expected exactly one Citi Bike target-month object')
                discovered = {**matches[0], 'landing_page': BIKE_PAGE, 'index_link': index,
                    'listing_endpoint': BIKE_LISTING, 'listing_pages': pages,
                    'parser': 'HTML bucket link + namespaced XML Contents/Key'}
                if discovered['bytes'] != expected[source]['object_bytes']:
                    raise DiscoveryError('Discovered byte size differs from validated source catalog')
            if discovered['url'] != expected[source]['url']:
                raise DiscoveryError('Discovered URL differs from validated source catalog')
            item.update(status='discovered', object=discovered, catalog_match=True)
        except DiscoveryError as exc:
            item['error'] = str(exc)
        report['results'].append(item)
    report.update(passed=all(x['status'] == 'discovered' for x in report['results']),
        finished_at_utc=now(), requests=client.evidence)
    report_file = output_dir / f'discovery-{month}-{run_id}.json'
    report_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    (output_dir / 'latest.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    report['receipt_file'] = str(report_file)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--month', default='2025-06')
    parser.add_argument('--source', choices=['citibike', 'tlc', 'all'], default='citibike')
    parser.add_argument('--proxy', default=None, help='Explicit task-local proxy; never modifies OS settings')
    parser.add_argument('--work-dir', type=Path, default=DEFAULT_WORK)
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    sources = {'citibike': ('citibike_trips',), 'tlc': ('tlc_yellow',), 'all': ('tlc_yellow', 'citibike_trips')}[args.source]
    result = discover(args.month, sources, proxy=args.proxy, work_dir=args.work_dir, output_dir=args.output_dir)
    print(json.dumps({'passed': result['passed'], 'receipt_file': result['receipt_file'],
        'results': result['results']}, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result['passed'] else 1)


if __name__ == '__main__':
    main()
