CREATE TABLE TestKeyComposite (
  pk1 STRING(MAX) NOT NULL,
  pk2 STRING(MAX) NOT NULL,
  value STRING(MAX)
) PRIMARY KEY (pk1, pk2);

INSERT INTO TestKeyComposite (pk1, pk2, value)
VALUES (REPEAT('a', 4000), REPEAT('b', 4000), 'Valid composite key size.');

INSERT INTO TestKeyComposite (pk1, pk2, value)
VALUES (REPEAT('a', 5000), REPEAT('b', 5000), 'Exceeds key size limit.');
