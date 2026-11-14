"""Check the pinned historical validation lineage and current fact versions."""
import json
from pathlib import Path
from build_marts import version

ROOT=Path(__file__).resolve().parents[1]


def main():
    def read(name):return json.loads((ROOT/'outputs'/name).read_text(encoding='utf-8'))
    dwd=read('dwd-latest.json');audit=read('dwd-verification.json');ods=read('ods-verification.json')
    assert dwd['passed'] and audit['passed'] and ods['passed']
    assert dwd['run_id']==audit['run_id']
    from ods_snapshot import verified_month_version, monthly_files
    verified_month_version(ROOT,read('ods-ingestion-latest.json'),ods)
    assert monthly_files(ROOT,dwd['ods_version'],dwd['month']) == monthly_files(ROOT,ods['committed_ods_version'],dwd['month']), 'DWD input month differs from verified ODS'
    for name,expected in dwd['output_versions'].items():
        if name.startswith('dwd.fact_'):assert version(name)==expected,'Unvalidated historical fact changes: '+name
    print('HISTORICAL_PREFLIGHT_OK month='+dwd['month']+' dwd_run='+dwd['run_id'])


if __name__=='__main__':main()
