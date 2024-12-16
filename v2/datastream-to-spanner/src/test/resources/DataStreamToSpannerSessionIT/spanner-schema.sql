CREATE TABLE IF NOT EXISTS Category (
  category_id INT64 NOT NULL,
  full_name STRING(25),
) PRIMARY KEY(category_id);

CREATE TABLE Books (
   id INT64 NOT NULL,
   title STRING(200),
   author_id INT64,
   synth_id STRING(50),
   extraCol1 INT64,
) PRIMARY KEY(synth_id);
