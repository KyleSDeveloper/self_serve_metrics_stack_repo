-- 7-day retention
CREATE OR REPLACE TABLE mart_retention AS
WITH signups AS (
  SELECT DISTINCT user_id, MIN(event_date) OVER (PARTITION BY user_id) AS signup_date
  FROM stg_events WHERE event='signup'
),
activity AS (
  SELECT e.user_id, e.event_date
  FROM stg_events e JOIN signups s USING (user_id)
  WHERE e.event_date >= s.signup_date AND e.event='visit'
),
joined AS (
  SELECT s.signup_date, a.event_date,
         DATE_DIFF('day', s.signup_date, a.event_date) AS day_num,
         e.user_id
  FROM activity a
  JOIN stg_events e ON e.user_id=a.user_id AND e.event_date=a.event_date
  JOIN signups s ON s.user_id=e.user_id
  WHERE DATE_DIFF('day', s.signup_date, a.event_date) BETWEEN 0 AND 7
)
SELECT signup_date, day_num,
       COUNT(DISTINCT CASE WHEN day_num=0 THEN user_id END) OVER (PARTITION BY signup_date) AS cohort_size,
       COUNT(DISTINCT user_id) AS active_users
FROM (SELECT DISTINCT signup_date, day_num, user_id FROM joined)
GROUP BY signup_date, day_num
ORDER BY signup_date, day_num;
