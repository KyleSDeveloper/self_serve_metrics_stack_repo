-- session facts
CREATE OR REPLACE TABLE mart_sessions AS
WITH bounds AS (
  SELECT session_id, MIN(ts) AS session_start, MAX(ts) AS session_end
  FROM stg_events GROUP BY session_id
)
SELECT session_id, session_start, session_end,
       EXTRACT('epoch' FROM (session_end - session_start))/60.0 AS session_minutes
FROM bounds;
