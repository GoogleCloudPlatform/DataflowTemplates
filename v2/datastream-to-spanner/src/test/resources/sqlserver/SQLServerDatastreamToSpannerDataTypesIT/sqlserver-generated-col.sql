-- TODO: SQL Server Change Data Capture (CDC) does not capture values of computed columns (even if PERSISTED).
-- In CDC change tables, computed columns are always recorded as NULL.
-- When a computed column is the primary key (as in generated_pk_column), CDC DELETE and INSERT
-- records contain NULL for the primary key, causing CDC replication to fail for tables with computed primary keys.
-- DELETE FROM generated_pk_column;
-- INSERT INTO generated_pk_column VALUES ('CC', 'CC');

DELETE FROM generated_non_pk_column;
INSERT INTO generated_non_pk_column VALUES (2, 'CC', 'CC'), (3, 'DD', 'EE'), (11, 'AA', 'BB');

DELETE FROM non_generated_to_generated_column;
INSERT INTO non_generated_to_generated_column VALUES ('CC', 'CC', 'CC ');

-- TODO: SQL Server Change Data Capture (CDC) does not capture values of computed columns (even if PERSISTED).
-- In CDC change tables, computed columns are always recorded as NULL.
-- When a computed column is the primary key (as in generated_to_non_generated_column), CDC DELETE and INSERT
-- records contain NULL for the primary key, causing CDC replication to fail for tables with computed primary keys.
-- DELETE FROM generated_to_non_generated_column;
-- INSERT INTO generated_to_non_generated_column VALUES ('CC', 'CC', 'CC ');
