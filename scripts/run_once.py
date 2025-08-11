import duckdb, pathlib, pandas as pd
import matplotlib.pyplot as plt
import os

# Work from repo root
ROOT = pathlib.Path(__file__).resolve().parents[1]
os.chdir(ROOT)
PATH = pathlib.Path(".")

# Ensure dirs (again, at runtime)
(PATH / "outputs").mkdir(parents=True, exist_ok=True)
(PATH / "images").mkdir(parents=True, exist_ok=True)

print("DEBUG cwd:", pathlib.Path.cwd())
print("DEBUG outputs exists:", (PATH/"outputs").exists())
print("DEBUG images exists:", (PATH/"images").exists())

con = duckdb.connect(database=str(PATH / 'analytics.duckdb'))

# Build models
for rel in [
    'models/staging/stg_events.sql',
    'models/marts/mart_sessions.sql',
    'models/marts/mart_funnel.sql',
    'models/marts/mart_retention.sql'
]:
    con.execute((PATH / rel).read_text())

# Export marts to CSVs
for tbl in ['mart_funnel','mart_retention','mart_sessions']:
    df = con.execute(f"SELECT * FROM {tbl}").df()
    out = PATH / 'outputs' / f"{tbl}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

# Charts
funnel = con.execute('SELECT * FROM mart_funnel ORDER BY event_date').df()
if not funnel.empty:
    row = funnel.tail(1).iloc[0]
    stages = ['visits','signups','activations','purchases']
    vals = [row[s] for s in stages]
    plt.figure(figsize=(6,4))
    plt.bar(stages, vals)
    plt.title('Funnel (Last Day)')
    plt.tight_layout()
    plt.savefig(PATH / 'images' / 'funnel_last_day.png', dpi=150)
    plt.close()

ret = con.execute("""
WITH base AS (
  SELECT day_num, CAST(active_users AS DOUBLE)/NULLIF(cohort_size,0) AS retention
  FROM mart_retention
)
SELECT day_num, AVG(retention) AS avg_retention
FROM base GROUP BY day_num ORDER BY day_num
""").df()
if not ret.empty:
    plt.figure(figsize=(6,4))
    plt.plot(ret['day_num'], ret['avg_retention'])
    plt.title('7-Day Retention (Average)')
    plt.xlabel('Day'); plt.ylabel('Retention')
    plt.tight_layout()
    plt.savefig(PATH / 'images' / 'retention_curve.png', dpi=150)
    plt.close()

print('Built models → outputs/*.csv and images/*.png')
