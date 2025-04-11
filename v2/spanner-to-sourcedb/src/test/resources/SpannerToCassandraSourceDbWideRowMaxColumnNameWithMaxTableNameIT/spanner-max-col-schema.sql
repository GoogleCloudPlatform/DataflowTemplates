CREATE TABLE testtable_03tpcovf16ed0klxm3v808ch3btgq0uk (
    id STRING(100) NOT NULL,
    col_qcbf69rmxtre3b_03tpcovf16ed STRING(100)
) PRIMARY KEY (id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);
