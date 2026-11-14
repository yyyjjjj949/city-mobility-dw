"""Validate that a verified monthly ODS snapshot still matches current files."""
import json
from pathlib import Path
from urllib.parse import unquote


def monthly_files(root, version, month):
    base = Path(root) / 'data/ods/source_record/_delta_log'
    active = {}
    for number in range(version + 1):
        for line in (base / f'{number:020}.json').read_text(encoding='utf-8').splitlines():
            action = json.loads(line)
            if 'add' in action: active[action['add']['path']] = action['add']
            if 'remove' in action: active.pop(action['remove']['path'], None)
    return {p:a for p,a in active.items() if a['partitionValues']['source_period'] == month}


def verified_month_version(root, ingestion, verification):
    assert verification['passed'] and ingestion['month'] == verification['month']
    assert ingestion['run_id'] == verification['latest_run_id'], 'ODS ingestion requires a matching verification'
    base = Path(root) / 'data/ods/source_record'
    pinned = verification['committed_ods_version']
    active = {}; verified = None
    logs = sorted((base / '_delta_log').glob('[0-9]' * 20 + '.json'))
    assert logs and [int(p.stem) for p in logs] == list(range(int(logs[-1].stem) + 1)), 'ODS audit requires complete Delta logs'
    def monthly():
        return {p: a for p, a in active.items() if a['partitionValues']['source_period'] == ingestion['month']}
    for log in logs:
        for line in log.read_text(encoding='utf-8').splitlines():
            action = json.loads(line)
            if 'add' in action: active[action['add']['path']] = action['add']
            if 'remove' in action: active.pop(action['remove']['path'], None)
        if int(log.stem) == pinned: verified = monthly()
    assert verified is not None and verified, 'Verified ODS snapshot is missing'
    assert monthly() == verified, 'Historical ODS files changed after verification; verify ODS before DWD'
    assert all((base / unquote(p)).is_file() for p in verified), 'Verified ODS files are unavailable'
    return pinned
