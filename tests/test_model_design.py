"""Regression checks: documentation verification must reject plausible model regressions."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('verify_model_design', ROOT / 'tools/verify_model_design.py')
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)
OUTPUT = ROOT / review.DEFAULT_OUTPUT


class ModelDesignReviewTests(unittest.TestCase):
    def evaluate_mutation(self, path, mutate):
        original = review.source
        def read(name):
            text = original(name)
            return mutate(text) if name == path else text
        with patch.object(review, 'source', side_effect=read):
            return review.verify(OUTPUT)

    def assert_failed(self, result, check):
        failures = [c['check'] for c in result['checks'] if not c['passed']]
        self.assertFalse(result['passed'])
        self.assertIn(check, failures)

    def test_current_model_matches_documented_invariants(self):
        self.assertTrue(review.verify(OUTPUT)['passed'])

    def test_wrong_type_of_role_key_is_rejected(self):
        def change(text):
            data = json.loads(text)
            fact = next(t for t in data['tables'] if t['name'] == 'dwd.fact_bike_trip')
            next(c for c in fact['columns'] if c['name'] == 'origin_zone_key')['type'] = 'STRING'
            return json.dumps(data)
        result = self.evaluate_mutation('config/model/schema-contract.json', change)
        self.assert_failed(result, 'dwd_references_resolve_with_matching_types')

    def test_crossed_date_and_time_reference_is_rejected(self):
        def change(text):
            data = json.loads(text)
            fact = next(t for t in data['tables'] if t['name'] == 'dwd.fact_taxi_trip')
            next(c for c in fact['columns'] if c['name'] == 'end_date_key')['reference'] = 'dwd.dim_time.time_key'
            return json.dumps(data)
        result = self.evaluate_mutation('config/model/schema-contract.json', change)
        self.assert_failed(result, 'dwd.fact_taxi_trip_role_playing')

    def test_averages_cannot_drop_their_denominator(self):
        def change(text):
            data = json.loads(text)
            table = next(t for t in data['tables'] if t['name'] == 'dws.trip_day')
            table['columns'] = [c for c in table['columns'] if c['name'] != 'duration_count']
            return json.dumps(data)
        result = self.evaluate_mutation('config/model/schema-contract.json', change)
        self.assert_failed(result, 'day_and_hour_retain_weighted_mean_parts')

    def test_incomplete_arrivals_cannot_be_marked_complete(self):
        def change(text):
            changed = text.replace("withColumn('arrival_complete', F.lit(False))", "withColumn('arrival_complete', F.lit(True))")
            self.assertNotEqual(changed, text)
            return changed
        result = self.evaluate_mutation('jobs/build_marts.py', change)
        self.assert_failed(result, 'incomplete_arrivals_remain_unavailable')


if __name__ == '__main__':
    unittest.main()
