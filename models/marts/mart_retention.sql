-- 7-day retention by signup cohort (DuckDB-friendly)
CREATE OR REPLACE TABLE mart_retention AS
WITH signups AS (
  SELECT user_id, MIN(event_date) AS signup_date
  FROM stg_events
  WHERE event = 'signup'
  GROUP BY user_id
),
visits AS (
  SELECT user_id, event_date
  FROM stg_events
  WHERE event = 'visit'
),
joined AS (
  SELECT
    s.signup_date,
    v.user_id,
    v.event_date,
    DATE_DIFF('day', s.signup_date, v.event_date) AS day_num
  FROM signups s
  JOIN visits v USING (user_id)
  WHERE DATE_DIFF('day', s.signup_date, v.event_date) BETWEEN 0 AND 7
),
cohorts AS (
  SELECT signup_date, COUNT(DISTINCT user_id) AS cohort_size
  FROM signups
  GROUP BY signup_date
),
active AS (
  SELECT signup_date, day_num, COUNT(DISTINCT user_id) AS active_users
  FROM joined
  GROUP BY signup_date, day_num
)
SELECT a.signup_date, a.day_num, c.cohort_size, a.active_users
FROM active a
JOIN cohorts c USING (signup_date)
ORDER BY a.signup_date, a.day_num;
