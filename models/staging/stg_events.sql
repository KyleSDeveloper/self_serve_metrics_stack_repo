-- staging model
CREATE OR REPLACE TABLE stg_events AS
SELECT
  CAST(user_id AS VARCHAR) AS user_id,
  CAST(session_id AS VARCHAR) AS session_id,
  CAST(event AS VARCHAR) AS event,
  CAST(ts AS TIMESTAMP) AS ts,
  DATE_TRUNC('day', ts) AS event_date
FROM read_csv_auto('seeds/events.csv', HEADER TRUE);
