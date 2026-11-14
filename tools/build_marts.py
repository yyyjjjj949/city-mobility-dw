"""Run pinned historical marts then load/validate/activate ClickHouse candidates."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid
from compute_lock import exclusive_compute

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / 'work/marts'
DOCKER = r'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'
os.environ['DOCKER_CONFIG'] = r'D:\CodexData\app-data\docker-profile\cli'


def now(): return dt.datetime.now(dt.timezone.utc).isoformat()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp'); tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8'); tmp.replace(path)


def version(name):
    return max(int(p.stem) for p in (ROOT / 'data' / name.replace('.', '/') / '_delta_log').glob('[0-9]' * 20 + '.json'))


def execute(args):
    dwd = json.loads((ROOT / 'outputs/dwd-latest.json').read_text(encoding='utf-8'))
    audit = json.loads((ROOT / 'outputs/dwd-verification.json').read_text(encoding='utf-8'))
    assert dwd['passed'] and audit['passed'] and dwd['run_id'] == audit['run_id']
    versions = {k: v for k, v in dwd['output_versions'].items() if k.startswith('dwd.')}
    assert all(version(k) == v for k, v in versions.items() if '.fact_' in k), 'Historical facts changed since validation'
    # Preserve the validated dimension and operational snapshots for reproducibility.
    prior_path = ROOT / 'outputs/marts-latest.json'
    prior = json.loads(prior_path.read_text(encoding='utf-8')) if prior_path.exists() else {}
    pinned_ops = prior.get('input_versions', {}) if prior.get('dwd_run_id') == dwd['run_id'] else {}
    versions.update({n:v for n,v in pinned_ops.items() if n.startswith('dwd.dim_')})
    versions.update({n: pinned_ops.get(n, version(n)) for n in ['ops.ingest_batch', 'ops.quality_result']})
    policies = json.loads((ROOT / 'config/model/policies.json').read_text(encoding='utf-8'))
    code = {n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest() for n in ['jobs/build_marts.py', 'config/model/schema-contract.json', 'config/model/policies.json']}
    fingerprint = hashlib.sha256(json.dumps({'dwd_run': dwd['run_id'], 'versions': versions, 'code': code}, sort_keys=True).encode()).hexdigest()
    previous = ROOT / 'outputs/marts-latest.json'
    if args.resume_release:
        release = args.resume_release
        assert len(release) == 32 and all(c in '0123456789abcdef' for c in release)
        run = WORK / 'runs' / release
        manifest = json.loads((run / 'manifest.json').read_text(encoding='utf-8'))
        assert manifest['input_fingerprint'] == fingerprint, 'Candidate inputs or build code changed'
        result = json.loads((run / 'result.json').read_text(encoding='utf-8'))
        assert result['passed'] and result['status'] == 'validated'
    elif previous.exists() and not args.force:
        prior = json.loads(previous.read_text(encoding='utf-8'))
        if prior.get('input_fingerprint') == fingerprint and all(version(n) == v for n, v in prior['output_versions'].items()):
            from publish_marts import verify_active
            verify_active(prior)
            record_publication(WORK / 'runs' / prior['release_id'], prior)
            save(ROOT / 'outputs/marts-rerun-verification.json', {'passed': True, 'release_id': prior['release_id'], 'versions_unchanged': True, 'active_pointer_unchanged': True, 'checked_at_utc': now()})
            print('MARTS_REUSED ' + prior['release_id'], flush=True); return
        return new_build(args, dwd, versions, policies, code, fingerprint)
    else:
        return new_build(args, dwd, versions, policies, code, fingerprint)
    finish(run, result)


@exclusive_compute
def new_build(args, dwd, versions, policies, code, fingerprint):
    release = uuid.uuid4().hex; run = WORK / 'runs' / release; run.mkdir(parents=True)
    manifest = dict(release_id=release, month=dwd['month'], dwd=dwd, versions=versions, policies=policies, code_sha256=code, input_fingerprint=fingerprint, created_at_utc=now())
    save(run / 'manifest.json', manifest)
    shutil.copyfile(ROOT / 'config/model/schema-contract.json', WORK / 'schema-contract.json')
    shutil.copyfile(ROOT / 'sql/spark/001_create_lakehouse.sql', WORK / 'lake.sql')
    container = subprocess.check_output([DOCKER, 'compose', '-f', str(ROOT / 'compose.yaml'), 'ps', '-q', 'spark'], cwd=ROOT, text=True).strip()
    limits = json.loads(subprocess.check_output([DOCKER, 'inspect', container], text=True))[0]['HostConfig']
    subprocess.run([DOCKER, 'update', '--memory', '4g', '--memory-swap', '4g', container], check=True, capture_output=True)
    command = [DOCKER, 'compose', '-f', str(ROOT / 'compose.yaml'), 'exec', '-T', 'spark', '/opt/spark/bin/spark-submit', '--driver-memory', '2g', '/workspace/jobs/build_marts.py', '--manifest', '/workspace/' + (run / 'manifest.json').relative_to(ROOT).as_posix()]
    print('MARTS_START ' + release + ' log=' + str(run / 'spark.log'), flush=True)
    try:
        with (run / 'spark.log').open('w', encoding='utf-8') as out:
            proc = subprocess.run(command, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT)
    finally:
        subprocess.run([DOCKER, 'update', '--memory', str(limits['Memory']), '--memory-swap', str(limits['MemorySwap']), container], check=True, capture_output=True)
    if proc.returncode: raise RuntimeError('Marts build failed; inspect ' + str(run / 'spark.log'))
    result = json.loads((run / 'result.json').read_text(encoding='utf-8'))
    assert result['passed']
    finish(run, result)


def finish(run, result):
    from publish_marts import publish
    publication = publish(run, result)
    result.update(publication=publication, serving_active=True)
    save(ROOT / 'outputs/marts-latest.json', result)
    record_publication(run, result)
    print('MARTS_PUBLISHED ' + result['release_id'], flush=True)


def record_publication(run, result):
    receipt = run / 'publication-ops.json'
    if receipt.exists():
        saved = json.loads(receipt.read_text(encoding='utf-8'))
        if saved.get('passed') and saved.get('release_id') == result['release_id']: return
    command = [DOCKER, 'compose', '-f', str(ROOT / 'compose.yaml'), 'exec', '-T', 'spark', '/opt/spark/bin/spark-submit',
               '/workspace/jobs/record_marts_publication.py', '--release-id', result['release_id']]
    with (run / 'publication-ops.log').open('w', encoding='utf-8') as out:
        proc = subprocess.run(command, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT)
    if proc.returncode: raise RuntimeError('Serving publication is active; metadata recording needs recovery. Inspect ' + str(run / 'publication-ops.log'))


def main():
    p = argparse.ArgumentParser(description=__doc__); p.add_argument('--force', action='store_true'); p.add_argument('--resume-release'); args = p.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    lock = WORK / 'writer.lock'; fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    try:
        os.write(fd, json.dumps({'pid': os.getpid(), 'started_at_utc': now()}).encode()); os.close(fd)
        execute(args)
    finally: lock.unlink(missing_ok=True)


if __name__ == '__main__': main()
