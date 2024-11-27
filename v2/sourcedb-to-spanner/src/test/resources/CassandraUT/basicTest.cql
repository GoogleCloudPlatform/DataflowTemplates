CREATE KEYSPACE test_keyspace
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1
        };
USE test_keyspace;
CREATE TABLE basic_test_table(
                       id varchar,
                       name varchar,
                       PRIMARY KEY(id));

INSERT INTO basic_test_table(id, name) values('1234','Albert');
INSERT INTO basic_test_table(id, name) values('5678','Einstein');