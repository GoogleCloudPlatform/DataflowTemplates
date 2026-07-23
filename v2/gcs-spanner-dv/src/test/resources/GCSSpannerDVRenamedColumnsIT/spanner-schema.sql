CREATE TABLE Users (
    user_id INT64 NOT NULL,
    event_id STRING(MAX) NOT NULL,
    name STRING(MAX),
    age INT64,
    created_at TIMESTAMP,
) PRIMARY KEY (user_id, event_id);
