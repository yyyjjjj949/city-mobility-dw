FROM apache/superset:6.1.0
USER root
RUN uv pip install --python /app/.venv/bin/python --no-cache clickhouse-connect==0.9.2 psycopg2-binary==2.9.11
USER superset
