CREATE TABLE Users (
  user_id INT64,
  event_id STRING(8176),
  full_name STRING(MAX),
  age INT64,
  created_at TIMESTAMP
) PRIMARY KEY(user_id, event_id);
