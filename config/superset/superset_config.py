import os

SECRET_KEY = os.environ['SUPERSET_SECRET_KEY']
SQLALCHEMY_DATABASE_URI = (
    'postgresql+psycopg2://platform:'
    + os.environ['META_DATABASE_PASSWORD']
    + '@postgres:5432/superset'
)
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'
ENABLE_PROXY_FIX = False
ROW_LIMIT = 10000
SQL_MAX_ROW = 10000
SUPERSET_WEBSERVER_TIMEOUT = 120


def configure_project_routes(app):
    # Render trusted local HTML with Superset's per-request CSP nonce. Keep the
    # existing strict CSP for the rest of the application.
    from pathlib import Path
    from flask import render_template_string
    @app.get('/city-mobility/map/')
    def city_mobility_map():
        page = Path('/app/superset/static/assets/city-mobility-map/map.html').read_text(encoding='utf-8')
        page = page.replace('<head>', '<head><base href="/static/assets/city-mobility-map/">')
        page = page.replace('<script src=', '<script nonce="{{ csp_nonce() }}" src=')
        page = page.replace('../项目总入口.html', '/superset/welcome/')
        page = page.replace('../marts/六月历史指标与看板.html', '/superset/dashboard/city-mobility-overview/')
        return render_template_string(page)


FLASK_APP_MUTATOR = configure_project_routes
