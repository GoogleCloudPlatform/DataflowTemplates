CREATE TABLE t_float (id float4 PRIMARY KEY, col varchar(20));
INSERT INTO t_float (id, col) VALUES (1.5, 'a'), (2.5, 'b'), (3.5, 'c');

CREATE TABLE t_double (id float8 PRIMARY KEY, col varchar(20));
INSERT INTO t_double (id, col) VALUES (1.5, 'a'), (2.5, 'b'), (3.5, 'c');

CREATE TABLE t_numeric (id numeric(10,2) PRIMARY KEY, col varchar(20));
INSERT INTO t_numeric (id, col) VALUES (1.50, 'a'), (2.50, 'b'), (3.50, 'c');

CREATE TABLE t_decimal (id decimal(10,2) PRIMARY KEY, col varchar(20));
INSERT INTO t_decimal (id, col) VALUES (1.50, 'a'), (2.50, 'b'), (3.50, 'c');

CREATE TABLE t_date (id date PRIMARY KEY, col varchar(20));
INSERT INTO t_date (id, col) VALUES ('2023-01-01', 'a'), ('2023-01-15', 'b'), ('2023-01-31', 'c');
