import os
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset.extensions import appbuilder, db
    from superset.models.core import Database
    security = appbuilder.sm
    if security.find_user(username='admin') is None:
        user = security.add_user(
            username='admin', first_name='Project', last_name='Admin',
            email='admin@localhost.invalid', role=security.find_role('Admin'),
            password=os.environ['SUPERSET_ADMIN_PASSWORD'],
        )
        if not user:
            raise RuntimeError('Could not create Superset administrator')
    name = 'City Mobility ClickHouse'
    database = db.session.query(Database).filter_by(database_name=name).one_or_none()
    uri = 'clickhousedb://dw_admin:' + os.environ['CLICKHOUSE_PASSWORD'] + '@clickhouse:8123/ads'
    if database is None:
        database = Database(database_name=name, sqlalchemy_uri=uri, expose_in_sqllab=True)
        db.session.add(database)
    else:
        database.sqlalchemy_uri = uri
    db.session.commit()
print('Superset administrator and ClickHouse connection are configured.')
