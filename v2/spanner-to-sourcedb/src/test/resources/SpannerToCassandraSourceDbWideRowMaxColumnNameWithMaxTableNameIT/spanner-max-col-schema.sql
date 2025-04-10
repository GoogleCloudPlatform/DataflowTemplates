CREATE TABLE testtable_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK (
    id STRING(100) NOT NULL,
    col_qcbF69RmXTRe3B_03TpCoVF16ED0KLxM3 STRING(100)
) PRIMARY KEY (id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);
