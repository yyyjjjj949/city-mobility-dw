import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from ods_snapshot import verified_month_version


class SnapshotTests(unittest.TestCase):
    def test_metadata_and_other_month_changes_preserve_verified_input(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); base = root / 'data/ods/source_record'
            logs = base / '_delta_log'; logs.mkdir(parents=True)
            (base / 'june.parquet').touch()
            june = {'path': 'june.parquet', 'partitionValues': {'source_period': '2025-06'}, 'size': 1}
            (logs / f'{0:020}.json').write_text(json.dumps({'add': june}))
            (logs / f'{1:020}.json').write_text(json.dumps({'metaData': {'description': 'Offline scope'}}))
            (logs / f'{2:020}.json').write_text(json.dumps({'add': {'path': 'july.parquet', 'partitionValues': {'source_period': '2025-07'}}}))
            ingestion = {'month': '2025-06', 'run_id': 'ingest'}
            verification = {'passed': True, 'month': '2025-06', 'latest_run_id': 'ingest', 'committed_ods_version': 0}
            self.assertEqual(verified_month_version(root, ingestion, verification), 0)
            (logs / f'{3:020}.json').write_text(json.dumps({'remove': {'path': 'june.parquet'}}))
            with self.assertRaisesRegex(AssertionError, 'Historical ODS files changed'):
                verified_month_version(root, ingestion, verification)

    def test_new_ingestion_requires_new_verification(self):
        with self.assertRaisesRegex(AssertionError, 'matching verification'):
            verified_month_version('.', {'month': '2025-06', 'run_id': 'new'},
                                   {'passed': True, 'month': '2025-06', 'latest_run_id': 'old'})
