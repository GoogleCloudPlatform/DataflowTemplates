CREATE TABLE IF NOT EXISTS DatastreamToSpanner_1 (
  row_id NUMERIC NOT NULL,
  name STRING(MAX),
  age NUMERIC,
  member STRING(MAX),
  entry_added STRING(MAX),
) PRIMARY KEY (row_id);

CREATE TABLE IF NOT EXISTS DatastreamToSpanner_2 (
  row_id NUMERIC NOT NULL,
  name STRING(MAX),
  age NUMERIC,
  member STRING(MAX),
  entry_added STRING(MAX),
) PRIMARY KEY (row_id);
