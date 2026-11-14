"""Failure boundaries for the offline warehouse queue and compute lock."""
import datetime as dt
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
import run_airflow_worker
from compute_lock import compute_lock


class OrchestrationRegression(unittest.TestCase):
    def test_receipt_recovers_after_transient_windows_sharing_violation(self):
        (ROOT/'work/tests').mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=ROOT/'work/tests') as folder:
            target=Path(folder)/'state.json';target.write_text('{"running":false}',encoding='utf-8')
            original=Path.replace;attempts=[]
            def interrupted(source,dest):
                attempts.append(1)
                if len(attempts)==1:
                    self.assertFalse(json.loads(target.read_text())['running'])
                    raise PermissionError('Windows reader holds the destination')
                return original(source,dest)
            with patch.object(Path,'replace',interrupted):
                run_airflow_worker.save(target,{'running':True})
            self.assertEqual(len(attempts),2)
            self.assertTrue(json.loads(target.read_text())['running'])
    def test_spark_lock_prevents_second_process_and_releases_after_exit(self):
        (ROOT/'work/tests').mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=ROOT/'work/tests') as folder:
            lock=Path(folder)/'compute.lock'
            command=[sys.executable,'-c','import sys; from compute_lock import compute_lock; h=compute_lock(sys.argv[1]); h.__enter__()',str(lock)]
            with compute_lock(lock):
                denied=subprocess.run(command,cwd=ROOT/'tools',capture_output=True)
            self.assertNotEqual(denied.returncode,0)
            self.assertIn(b'Spark is busy',denied.stderr)
            granted=subprocess.run(command,cwd=ROOT/'tools',capture_output=True)
            self.assertEqual(granted.returncode,0)
    def test_rejects_unknown_expired_and_malformed_requests_without_execution(self):
        (ROOT/'work/tests').mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=ROOT/'work/tests') as folder,patch.object(run_airflow_worker,'WORK',Path(folder)),patch.object(run_airflow_worker.subprocess,'Popen') as launch:
            base=Path(folder)
            for n in ['results','running','logs']:(base/n).mkdir()
            for index,payload in enumerate([
                {'action':'arbitrary_shell','deadline_utc':'2099-01-01T00:00:00+00:00'},
                {'action':'monitor_project','deadline_utc':'2000-01-01T00:00:00+00:00'},
                {'action':'build_historical_dwd','deadline_utc':'2099-01-01T00:00:00+00:00','parameters':{'month':'2025-06;whoami'}},
                {'action':'ingest_historical','deadline_utc':'2099-01-01T00:00:00+00:00','parameters':{'month':'2025-07'}},
                {'action':'ingest_historical','deadline_utc':'2099-01-01T00:00:00+00:00','parameters':{'month':'2025-06','cmd':'whoami'}},
                None,
            ]):
                job=str(index)*64;path=base/(job+'.json')
                path.write_text(json.dumps({**payload,'job_id':job}) if payload else '{broken',encoding='utf-8')
                run_airflow_worker.execute(path)
                self.assertFalse(json.loads((base/'results'/path.name).read_text(encoding='utf-8'))['passed'])
            launch.assert_not_called()


if __name__=='__main__':unittest.main()
