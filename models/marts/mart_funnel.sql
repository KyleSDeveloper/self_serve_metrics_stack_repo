-- daily funnel
CREATE OR REPLACE TABLE mart_funnel AS
WITH base AS (
  SELECT user_id, session_id, event_date,
         MAX(CASE WHEN event='visit' THEN 1 ELSE 0 END) AS visit,
         MAX(CASE WHEN event='signup' THEN 1 ELSE 0 END) AS signup,
         MAX(CASE WHEN event='activate' THEN 1 ELSE 0 END) AS activate,
         MAX(CASE WHEN event='purchase' THEN 1 ELSE 0 END) AS purchase
  FROM stg_events GROUP BY user_id, session_id, event_date
),
daily AS (
  SELECT event_date, SUM(visit) AS visits, SUM(signup) AS signups,
         SUM(activate) AS activations, SUM(purchase) AS purchases
  FROM base GROUP BY event_date
)
SELECT * FROM daily ORDER BY event_date;
