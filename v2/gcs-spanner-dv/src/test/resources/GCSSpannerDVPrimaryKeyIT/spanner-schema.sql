CREATE TABLE Users (
    user_id INT64,
    event_id STRING(MAX),
    full_name STRING(MAX),
    age INT64,
    created_at TIMESTAMP
) PRIMARY KEY (user_id, full_name);

CREATE TABLE AccountRoles (
    role_id INT64,
    role_name STRING(MAX)
) PRIMARY KEY (role_name);

CREATE TABLE Users_TimestampPK (
    user_id INT64,
    event_id STRING(MAX),
    full_name STRING(MAX),
    age INT64,
    created_at TIMESTAMP
) PRIMARY KEY (created_at);
