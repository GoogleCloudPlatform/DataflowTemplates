CREATE TABLE Users (
    user_id INT64 NOT NULL,
    event_id STRING(MAX) NOT NULL,
    full_name STRING(MAX),
    age INT64,
    created_at TIMESTAMP
) PRIMARY KEY (user_id, event_id);

CREATE TABLE AccountRoles (
    role_id INT64 NOT NULL,
    role_name STRING(MAX)
) PRIMARY KEY (role_id);
