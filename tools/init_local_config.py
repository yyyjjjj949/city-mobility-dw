"""Create fresh development credentials without copying the original machine's secrets."""
import base64
import getpass
import json
from pathlib import Path
import re
import secrets

ROOT=Path(__file__).resolve().parents[1]


def main():
    env=ROOT/'.env'
    airflow=ROOT/'config/airflow/passwords.json'
    if env.exists() or airflow.exists():
        raise SystemExit('Local configuration already exists; no files were changed.')
    password=getpass.getpass('Choose a local demo password (blank generates one): ')
    if not password:
        password=secrets.token_urlsafe(18)
    elif not re.fullmatch(r'[A-Za-z0-9_-]+',password):
        raise SystemExit('Use letters, numbers, underscore or dash for connection URL compatibility.')
    values={
        'POSTGRES_PASSWORD':password,'CLICKHOUSE_PASSWORD':password,
        'SUPERSET_ADMIN_PASSWORD':password,'AIRFLOW_ADMIN_PASSWORD':password,
        'SUPERSET_SECRET_KEY':secrets.token_urlsafe(48),
        'AIRFLOW_FERNET_KEY':base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('ascii'),
        'AIRFLOW_JWT_SECRET':secrets.token_urlsafe(48),
    }
    airflow.parent.mkdir(parents=True,exist_ok=True)
    # Exclusive creation prevents replacement of a configuration created meanwhile.
    with env.open('x',encoding='utf-8') as stream:
        stream.write('# Local only; do not commit this file.\n')
        stream.writelines(k+'='+v+'\n' for k,v in values.items())
    try:
        with airflow.open('x',encoding='utf-8') as stream:
            json.dump({'admin':password},stream,ensure_ascii=False,indent=2)
    except Exception:
        env.unlink()
        raise
    print('Created .env and config/airflow/passwords.json. Read the password locally in .env.')


if __name__=='__main__':
    main()
