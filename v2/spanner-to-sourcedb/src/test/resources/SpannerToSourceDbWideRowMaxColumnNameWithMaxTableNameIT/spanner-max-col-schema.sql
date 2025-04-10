CREATE TABLE testtable_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvYZPAeGeqiO (
    id STRING(100) NOT NULL,
    col_qcbF69RmXTRe3B_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvY STRING(100)
) PRIMARY KEY (id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);
