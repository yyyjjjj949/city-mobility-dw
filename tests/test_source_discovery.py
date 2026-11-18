"""Regression coverage for real-source discovery and offline reuse boundaries."""
from pathlib import Path
import json
import sys
import tempfile
import unittest
from unittest import mock

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import discover_sources as discovery
import ingest_sources as ingestion

WORK = ROOT / 'work' / discovery.CHAT / 'crawl-tests'
WORK.mkdir(parents=True, exist_ok=True)


def listing(key='202506-citibike-tripdata.zip', token=None, namespace=True):
    ns = ' xmlns="http://s3.amazonaws.com/doc/2006-03-01/"' if namespace else ''
    token_xml = '<NextContinuationToken>' + token + '</NextContinuationToken>' if token else ''
    return (f'<ListBucketResult{ns}><Name>tripdata</Name><IsTruncated>{str(bool(token)).lower()}</IsTruncated>'
        f'{token_xml}<Contents><Key>{key}</Key><Size>929043436</Size></Contents></ListBucketResult>').encode()


class Response:
    def __init__(self, status=200, body=b'', headers=None):
        self.status_code = status
        self.body = body
        self.headers = headers or {'Content-Type': 'text/plain'}
    def iter_content(self, chunk_size):
        yield self.body
    def close(self):
        pass


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}
        self.proxies = {}
        self.calls = []
    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class DiscoveryRegression(unittest.TestCase):
    def test_tlc_parses_anchor_for_exact_month(self):
        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-06.parquet'
        self.assertEqual(discovery.parse_tlc('<a href="' + url + '">June</a>', '2025-06'), url)
        with self.assertRaises(discovery.DiscoveryError):
            discovery.parse_tlc('<a href="' + url + '">June</a>', '2025-07')
        with self.assertRaises(discovery.DiscoveryError):
            discovery.parse_tlc('<script>"' + url + '"</script>', '2025-06')

    def test_offsite_or_non_https_target_is_rejected(self):
        for host in ['https://evil.invalid', 'http://d37ci6vzurychx.cloudfront.net',
                     'https://user:password@d37ci6vzurychx.cloudfront.net']:
            with self.assertRaises(discovery.DiscoveryError):
                discovery.parse_tlc('<a href="' + host + '/trip-data/yellow_tripdata_2025-06.parquet">x</a>', '2025-06')

    def test_citibike_requires_official_bucket_anchor(self):
        html = '<a href="https://s3.amazonaws.com/tripdata/index.html">Download</a>'
        self.assertEqual(discovery.parse_bike_index(html), 'https://s3.amazonaws.com/tripdata/index.html')
        with self.assertRaises(discovery.DiscoveryError):
            discovery.parse_bike_index('<a href="https://evil.invalid/tripdata/index.html">x</a>')

    def test_xml_exact_month_and_namespace(self):
        matches, token = discovery.parse_bike_listing(listing(), '2025-06')
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]['bytes'], 929043436)
        self.assertIsNone(token)
        self.assertEqual(discovery.parse_bike_listing(listing(), '2025-07')[0], [])
        with self.assertRaises(discovery.DiscoveryError):
            discovery.parse_bike_listing(listing(namespace=False), '2025-06')

    def test_malformed_or_entity_xml_is_rejected(self):
        for payload in [b'<broken', b'<!DOCTYPE foo [<!ENTITY ext SYSTEM "file:///C:/secret">]><foo/>']:
            with self.assertRaises(discovery.DiscoveryError):
                discovery.parse_bike_listing(payload, '2025-06')

    def test_robots_denial_stops_source_request(self):
        session = Session([Response(body=b'User-agent: *\nDisallow: /system-data\n')])
        with tempfile.TemporaryDirectory(dir=WORK) as folder:
            client = discovery.DiscoveryClient(folder, session=session, interval=0)
            with self.assertRaisesRegex(discovery.DiscoveryError, 'disallows'):
                client.get(discovery.BIKE_PAGE)
            self.assertEqual(len(session.calls), 1)

    def test_robots_403_is_fail_closed(self):
        session = Session([Response(403, b'Access denied')])
        with tempfile.TemporaryDirectory(dir=WORK) as folder:
            client = discovery.DiscoveryClient(folder, session=session, interval=0)
            with self.assertRaisesRegex(discovery.DiscoveryError, 'HTTP 403'):
                client.get(discovery.TLC_PAGE)
            self.assertEqual(len(session.calls), 1)

    def test_offsite_redirect_is_not_followed(self):
        session = Session([Response(404), Response(302, headers={'Location': 'https://evil.invalid/collect'})])
        with tempfile.TemporaryDirectory(dir=WORK) as folder:
            client = discovery.DiscoveryClient(folder, session=session, interval=0)
            with self.assertRaises(discovery.DiscoveryError):
                client.get(discovery.BIKE_PAGE)
            self.assertEqual(len(session.calls), 2)
            self.assertFalse(session.calls[-1][1]['allow_redirects'])

    def test_network_failure_is_bounded_and_secret_free(self):
        session = Session([requests.ConnectionError('secret-proxy-user:password'), requests.ConnectionError('secret')])
        with tempfile.TemporaryDirectory(dir=WORK) as folder:
            client = discovery.DiscoveryClient(folder, session=session, interval=0, sleeper=lambda _: None)
            with self.assertRaises(discovery.DiscoveryError) as caught:
                client.get(discovery.BIKE_PAGE)
            self.assertEqual(len(session.calls), 2)
            self.assertNotIn('secret', str(caught.exception))

    def test_live_failure_has_receipt_and_no_catalog_fallback(self):
        session = Session([Response(404), Response(body=b'<html>No bucket anchor</html>')])
        with tempfile.TemporaryDirectory(dir=WORK) as folder:
            client = discovery.DiscoveryClient(Path(folder) / 'raw', session=session, interval=0)
            report = discovery.discover('2025-06', ('citibike_trips',), client=client,
                work_dir=Path(folder) / 'runs', output_dir=Path(folder) / 'outputs')
            self.assertFalse(report['passed'])
            self.assertEqual(report['results'][0]['status'], 'failed')
            self.assertTrue(Path(report['receipt_file']).is_file())

    def test_existing_source_skips_network_discovery_but_not_sha_checks(self):
        sample = ROOT / 'data/samples/bike_2025-06_first25.csv'
        source = {'source_id': 'citibike_trips', 'source_period': '2025-06',
                  'relative_path': sample.relative_to(ROOT).as_posix()}
        with mock.patch.object(discovery, 'discover', side_effect=AssertionError('Network must not be used')):
            receipt = ingestion.ensure_source_discovery([source])
            self.assertEqual(receipt['mode'], 'existing-files-reuse-check')
        with tempfile.TemporaryDirectory(dir=WORK) as folder:
            file = Path(folder) / 'copy.csv'
            file.write_bytes(sample.read_bytes())
            file.with_suffix('.csv.download.json').write_text(json.dumps({'source_url': 'https://s3.amazonaws.com/tripdata/test',
                'sha256': 'tampered', 'bytes': file.stat().st_size}), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
                ingestion.download({'relative_path': file.relative_to(ROOT).as_posix(),
                    'url': 'https://s3.amazonaws.com/tripdata/test'}, None)

    def test_missing_source_requires_successful_discovery(self):
        source = {'source_id': 'citibike_trips', 'source_period': '2025-06', 'relative_path': 'work/nonexistent-new-source-test.zip'}
        with mock.patch.object(discovery, 'discover', return_value={'passed': False, 'receipt_file': 'failure.json'}):
            with self.assertRaisesRegex(RuntimeError, 'no silent catalog fallback'):
                ingestion.ensure_source_discovery([source])

    def test_robots_redirect_chain_does_not_recursively_fetch_robots(self):
        session = Session([
            Response(302, headers={'Location': '/static/robots-v1.txt'}),
            Response(302, headers={'Location': '/static/robots-final.txt'}),
            Response(body=b'User-agent: *\nAllow: /\n'),
            Response(body=b'<html>data page</html>')])
        with tempfile.TemporaryDirectory(dir=WORK) as folder:
            client = discovery.DiscoveryClient(folder, session=session, interval=0)
            self.assertEqual(client.get(discovery.BIKE_PAGE), b'<html>data page</html>')
            self.assertEqual(len(session.calls), 4)

    def test_malformed_unrelated_anchor_is_ignored(self):
        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-06.parquet'
        html = '<a href="https://[invalid">bad</a><a href="' + url + '">June</a>'
        self.assertEqual(discovery.parse_tlc(html, '2025-06'), url)
        with self.assertRaises(discovery.DiscoveryError):
            discovery.validate_url('https://[invalid')

    def test_invalid_month_rejected(self):
        for month in ['2025-6', '../2025-06', '2025-13']:
            with self.assertRaises(discovery.DiscoveryError):
                discovery.validate_month(month)


if __name__ == '__main__':
    unittest.main()
