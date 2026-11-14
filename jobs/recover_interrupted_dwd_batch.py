"""Close only the unfinished source attempts belonging to one interrupted DWD run."""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

parser = argparse.ArgumentParser()
parser.add_argument('--run-id', required=True)
args = parser.parse_args()
if not re.fullmatch('[0-9a-f]{32}', args.run_id):
    raise ValueError('Invalid DWD run ID')
folder = Path('/workspace/work/dwd/runs') / args.run_id
manifest = json.loads((folder / 'manifest.json').read_text())
ids = [hashlib.sha256(json.dumps([args.run_id, sid, 'dwd'], ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
       for sid in ['tlc_yellow', 'citibike_trips']]
spark = (SparkSession.builder.appName('CityMobility-RecoverInterruptedDWD')
         .config('spark.sql.session.timeZone', 'UTC')
         .config('spark.databricks.delta.snapshotPartitions', '2')
         .config('spark.sql.warehouse.dir', '/workspace/work/dwd/recovery-warehouse').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
try:
    table = DeltaTable.forPath(spark, '/workspace/data/ops/ingest_batch')
    condition = F.col('batch_id').isin(ids) & (F.col('status') == 'running')
    pending = [r.asDict() for r in table.toDF().where(condition).select('batch_id', 'source_id', 'status').collect()]
    if pending:
        table.update(condition=condition, set={
            'status': F.lit('failed'), 'finished_at_utc': F.current_timestamp(),
            'error_summary': F.lit('Spark process terminated before source validation; interrupted attempt retained for recovery audit.')})
    report = {'run_id': args.run_id, 'recovered_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
              'attempts_marked_failed': pending, 'completed_attempts_unchanged': True}
    (folder / 'interrupted-batch-recovery.json').write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print('INTERRUPTED_BATCH_RECOVERY_OK ' + str(len(pending)), flush=True)
finally:
    spark.stop()
