CREATE TABLE IF NOT EXISTS SingleShardWithTransformationTable (
  pkid SERIAL PRIMARY KEY,
  name VARCHAR(20),
  status VARCHAR(20)
);

INSERT INTO SingleShardWithTransformationTable (pkid, name, status)
VALUES (1, 'Alice', 'active'), (2, 'Bob', 'inactive'), (3, 'Carol', 'pending'), (4, 'David', 'complete'),  (5, 'Emily', 'error');
