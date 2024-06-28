CREATE TABLE parent1 (
 id INT64 NOT NULL,
 update_ts TIMESTAMP,
 in_ts TIMESTAMP,
) PRIMARY KEY(id);


CREATE TABLE child11 (
 child_id INT64 NOT NULL,
 parent_id INT64,
 update_ts TIMESTAMP,
 in_ts TIMESTAMP,
) PRIMARY KEY(child_id);


CREATE INDEX par_ind ON child11(parent_id);






CREATE TABLE parent2 (
 id INT64 NOT NULL,
 update_ts TIMESTAMP,
 in_ts TIMESTAMP,
) PRIMARY KEY(id);


CREATE TABLE child21 (
 child_id INT64 NOT NULL,
 id INT64 NOT NULL,
 update_ts TIMESTAMP,
 in_ts TIMESTAMP,
) PRIMARY KEY(id, child_id),
 INTERLEAVE IN PARENT parent2 ON DELETE CASCADE;


CREATE INDEX par_ind_5 ON child21(id);


CREATE CHANGE STREAM allstream
 FOR ALL OPTIONS (
 value_capture_type = 'NEW_ROW',
 retention_period = '7d'
);
