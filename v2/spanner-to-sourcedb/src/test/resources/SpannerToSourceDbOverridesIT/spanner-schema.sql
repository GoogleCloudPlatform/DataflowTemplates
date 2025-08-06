CREATE TABLE Target_Table_1 (
    id_col1 INT64 NOT NULL,
    Target_Name_Col_1 STRING(255),
    data_col1 STRING(MAX)
) PRIMARY KEY (id_col1);

CREATE TABLE source_table2 (
    key_col2 STRING(50) NOT NULL,
    Target_Category_Col_2 STRING(100),
    value_col2 STRING(MAX)
) PRIMARY KEY (key_col2);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);