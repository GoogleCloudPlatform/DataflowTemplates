CREATE TABLE Members (
    user_id INT64 NOT NULL,
    event_id STRING(MAX) NOT NULL,
    full_name STRING(MAX),
    age INT64,
    created_at TIMESTAMP
) PRIMARY KEY (user_id, event_id);
