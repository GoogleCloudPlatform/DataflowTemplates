DELETE FROM generated_pk_column;
INSERT INTO generated_pk_column VALUES ('CC', 'CC');

DELETE FROM generated_non_pk_column;
INSERT INTO generated_non_pk_column VALUES (2, 'CC', 'CC'), (3, 'DD', 'EE'), (11, 'AA', 'BB');

DELETE FROM non_generated_to_generated_column;
INSERT INTO non_generated_to_generated_column VALUES ('CC', 'CC', 'CC ');

DELETE FROM generated_to_non_generated_column;
INSERT INTO generated_to_non_generated_column VALUES ('CC', 'CC', 'CC ');
