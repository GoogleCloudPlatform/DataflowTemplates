CREATE TABLE IF NOT EXISTS Users (
    id INT64 NOT NULL,
    full_name STRING(25),
    `from` STRING(25)
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS Users2 (
    id INT64 NOT NULL,
    name STRING(25),
    ) PRIMARY KEY(id);

CREATE TABLE TableWithVirtualGeneratedColumn (
    id INT64 NOT NULL,
    column1 INT64,
    virtual_generated_column INT64 AS (column1 + id),
) PRIMARY KEY(id);

CREATE TABLE TableWithStoredGeneratedColumn (
    id INT64 NOT NULL,
    column1 INT64,
    stored_generated_column INT64 AS (column1 + id) STORED,
) PRIMARY KEY(id);

 CREATE TABLE IF NOT EXISTS testtable_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvYZPAeGeqiO(
 id INT64 NOT NULL,
 col_qcbF69RmXTRe3B_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvY STRING(25),
 ) PRIMARY KEY(id);

CREATE CHANGE STREAM allstream
  FOR ALL OPTIONS (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d',
  allow_txn_exclusion = true
);