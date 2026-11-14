"""Small real-source regressions: source ordinal identity and retry reuse."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('ingest_sources',ROOT/'tools/ingest_sources.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class IngestionRegression(unittest.TestCase):
    def test_taxi_identical_contents_keep_distinct_positions(self):
        row=json.loads((ROOT/'data/samples/taxi_2025-06_first25.json').read_text())[0]
        content=module.canonical(row)
        self.assertEqual(module.canonical(json.loads(content)),content)
        self.assertNotEqual(module.key('real_source_object','',1),module.key('real_source_object','',2))

    def test_csv_member_identity_and_reuse_from_real_sample(self):
        (ROOT/'work/ingestion').mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=ROOT/'work/ingestion',prefix='regression-') as temp:
            folder=Path(temp);archive=folder/'source.zip'
            sample=(ROOT/'data/samples/bike_2025-06_first25.csv').read_bytes()
            # Same real rows in two archive members test source identity; no fabricated business facts.
            with zipfile.ZipFile(archive,'w') as z:
                z.writestr('part1.csv',sample);z.writestr('part2.csv',sample)
            obj=module.inspect_object({'source_id':'citibike_trips','source_period':'validation','url':'https://example.invalid/local-real-sample-regression','relative_path':archive.relative_to(ROOT).as_posix(),'kind':'csv_zip','sha256':module.file_hash(archive)})
            old_work=module.WORK
            try:
                module.WORK=folder/'scratch'
                prepared=module.prepare_object(obj,'regression')
                self.assertEqual(prepared['prepared']['rows'],50)
                self.assertEqual(prepared['prepared']['member_counts'],{'part1.csv':25,'part2.csv':25})
                second=module.prepare_object(obj,'retry')
                self.assertEqual(prepared['prepared'],second['prepared'])
                self.assertNotEqual(module.key(obj['object_id'],'part1.csv',1),module.key(obj['object_id'],'part2.csv',1))
                shard=folder/'scratch/staging'/obj['object_id']/prepared['prepared']['shards'][0]['name']
                with shard.open('ab') as f:f.write(b'changed')
                with self.assertRaisesRegex(ValueError,'Staging checksum mismatch'):module.prepare_object(obj,'retry')
            finally:module.WORK=old_work

if __name__=='__main__':unittest.main()
