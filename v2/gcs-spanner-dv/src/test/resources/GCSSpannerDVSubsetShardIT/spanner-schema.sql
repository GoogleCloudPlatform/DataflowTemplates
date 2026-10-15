CREATE TABLE AccountRoles (
role_id INT64 NOT NULL,
role_name STRING(255)
) PRIMARY KEY (role_id);

CREATE TABLE Users (
user_id INT64 NOT NULL,
event_id STRING(255) NOT NULL,
full_name STRING(255),
age INT64,
created_at TIMESTAMP
) PRIMARY KEY (user_id, event_id);

CREATE TABLE AuditLog (
log_id INT64 NOT NULL,
message STRING(255)
) PRIMARY KEY (log_id);
