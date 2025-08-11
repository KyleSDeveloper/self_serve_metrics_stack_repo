from prefect import flow, task
import duckdb, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]

@task
def run_sql(path):
    con = duckdb.connect(database=str(ROOT / 'analytics.duckdb'))
    sql = (ROOT / path).read_text()
    con.execute(sql)
    con.close()

@flow(name='self-serve-metrics-build')
def build():
    run_sql('models/staging/stg_events.sql')
    run_sql('models/marts/mart_sessions.sql')
    run_sql('models/marts/mart_funnel.sql')
    run_sql('models/marts/mart_retention.sql')

if __name__ == '__main__':
    build()
