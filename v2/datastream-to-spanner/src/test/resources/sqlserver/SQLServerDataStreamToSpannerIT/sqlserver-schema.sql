CREATE TABLE source_table1 (
    id INT PRIMARY KEY,
    name VARCHAR(200),
    age INT,
    member VARCHAR(200),
    entry_added VARCHAR(200)
);

CREATE TABLE source_table2 (
    id INT PRIMARY KEY,
    name VARCHAR(200),
    age INT,
    member VARCHAR(200),
    entry_added VARCHAR(200)
);

EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'source_table1', @role_name = NULL, @supports_net_changes = 1;

EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'source_table2', @role_name = NULL, @supports_net_changes = 1;

EXEC sys.sp_cdc_stop_job @job_type = N'capture';

EXEC sys.sp_cdc_change_job @job_type = N'capture', @pollinginterval = 5;

EXEC sys.sp_cdc_start_job @job_type = N'capture';
