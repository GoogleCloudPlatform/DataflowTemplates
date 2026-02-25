CREATE TABLE `Users` (
    id INT64 NOT NULL,
    first_name STRING(200),
    last_name STRING(200),
    age INT64,
    status BOOL,
    col1 INT64,
    col2 INT64,
) PRIMARY KEY(id);