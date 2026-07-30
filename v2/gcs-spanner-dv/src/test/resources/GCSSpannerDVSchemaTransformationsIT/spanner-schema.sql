-- Test Case 1: Data type change (STRING -> INT64) automatically handled by the pipeline
CREATE TABLE AccountRoles (
    role_id INT64,
    role_name INT64
) PRIMARY KEY(role_id);

-- Test Case 2: Column 'age' dropped in Spanner
CREATE TABLE Users (
    user_id INT64,
    event_id STRING(MAX),
    full_name STRING(MAX),
    created_at TIMESTAMP
) PRIMARY KEY(user_id, event_id);

-- Test Case 3: Column 'status' added in Spanner
CREATE TABLE Users_AddedColumn (
    user_id INT64,
    event_id STRING(MAX),
    full_name STRING(MAX),
    age INT64,
    created_at TIMESTAMP,
    status STRING(MAX)
) PRIMARY KEY(user_id, event_id);
