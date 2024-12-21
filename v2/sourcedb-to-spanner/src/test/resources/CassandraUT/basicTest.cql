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

// Primitive types
CREATE TABLE primitive_types_table (
                                 primary_key UUID PRIMARY KEY,
                                 ascii_col ASCII,
                                 bigint_col BIGINT,
                                 blob_col BLOB,
                                 boolean_col BOOLEAN,
                                 date_col DATE,
                                 decimal_col DECIMAL,
                                 double_col DOUBLE,
                                 duration_col DURATION,
                                 float_col FLOAT,
                                 inet_col INET,
                                 int_col INT,
                                 smallint_col SMALLINT,
                                 text_col TEXT,
                                 time_col TIME,
                                 timestamp_col TIMESTAMP,
                                 timeuuid_col TIMEUUID,
                                 tinyint_col TINYINT,
                                 uuid_col UUID,
                                 varchar_col VARCHAR,
                                 varint_col VARINT);

-- Inserting 3 Randomly generated rows.
INSERT INTO primitive_types_table (primary_key, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col)
VALUES (
           dfcad8f3-3cdc-49c7-bce9-575f307c0637,
           'ascii1',
           1234567890,
           0xCAFEBABE,
           true,
           '2023-12-03',
           123.456,
           123.456789,
           12h34m56s,
           123.45,
           '127.0.0.1',
           12345,
           123,
           'text1',
           '12:34:56.789',
           1733296987117000,
           9b9419da-b210-11ef-890e-9d9a41af9e54,
           123,
           c3de3455-6b4e-4a81-a6d7-ab61610f08c6,
           'varchar1',
           1234567890123456789
       );


INSERT INTO primitive_types_table (primary_key, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col)
VALUES (
           fe3263a0-1577-4851-95f8-3af47628baa4,
           'ascii2',
           9876543210,
           0xDEADBEEF,
           false,
           '2022-11-02',
           987.654,
           987.654321,
           -12h34m56s,
           987.65,
           '::1',
           98765,
           987,
           'text2',
           '23:59:59.999',
           1733296987122000,
           9b94dd2a-b210-11ef-890e-9d9a41af9e54,
           -123,
           6324e301-94fb-44fe-95ac-91d2f7236e2e,
           'varchar2',
           -9876543210987654321
       );
INSERT INTO primitive_types_table (primary_key, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col)
VALUES (
           9a0acb7d-674c-4ee1-9644-9da24b7a72f4,
           'ascii3',
           1010101010,
           0xFACEFEED,
           true,
           '2024-01-01',
           10.101,
           10.101010,
           1y2mo3d4h5m6s,
           10.10,
           '192.168.1.1',
           10101,
           101,
           'text3',
           '00:00:00.000',
           1733296987127000,
           9b95a07a-b210-11ef-890e-9d9a41af9e54,
           101,
           f0e1d922-06b5-4f07-a7a6-ec0c9f23e172,
           'varchar3',
           10101010101010101010
       );

-- Inserting data with all columns null (except primary key)
INSERT INTO primitive_types_table (primary_key) VALUES (e6bc8562-2575-420f-9344-9fedc4945f61);

-- Inserting data with minimum values for each column (where applicable)
INSERT INTO primitive_types_table (primary_key, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col)
VALUES (
           29e38561-6376-4b45-b1a0-1709e11cfc8c,
           '',               -- Minimum ASCII (empty string)
           -9223372036854775808, -- Minimum BIGINT
           0x00,             -- Minimum BLOB (empty)
           false,            -- Minimum BOOLEAN
           '1000-01-01',     -- Minimum DATE
           -10000000000000000000000000000000000000,
           -1.7976931348623157E+308, -- Minimum DOUBLE
           P0000-00-00T89:09:09, -- TODO Min
           -3.4028234663852886E+38, -- Minimum FLOAT
           '0.0.0.0',       -- Minimum INET
           -2147483648,      -- Minimum INT
           -32768,           -- Minimum SMALLINT
           '',               -- Minimum TEXT (empty string)
           '00:00:00.000',   -- Minimum TIME
           0,                -- Minimum TIMESTAMP (epoch)
           null,  -- TODO time uuid
           -128,             -- Minimum TINYINT
           fff6d876-560f-48bc-8088-90c69e5a0c40,
           '',               -- Minimum VARCHAR (empty string)
           -9223372036854775808  -- Minimum VARINT
       );

-- Inserting data with maximum values for each column (where applicable)
INSERT INTO primitive_types_table (primary_key, ascii_col, bigint_col, blob_col, boolean_col, date_col, decimal_col, double_col, duration_col, float_col, inet_col, int_col, smallint_col, text_col, time_col, timestamp_col, timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col)
VALUES (
           a389de30-f01f-4395-a0c6-c407bfbe81d0,
           'zzzzzzzzzz',    --
           9223372036854775807, -- Maximum BIGINT
           0xFFFFFFFF,       --
           true,             -- Maximum BOOLEAN
           '9999-12-31',     -- Maximum DATE
           10000000000000000000000000000000000000, --
           1.7976931348623157E+308, -- Maximum DOUBLE
           P0000-00-00T89:09:09, -- TODO Max
           3.4028234663852886E+38, -- Maximum FLOAT
           '255.255.255.255',-- Maximum INET
           2147483647,      -- Maximum INT
           32767,           -- Maximum SMALLINT
           'abcdef',   --
           '23:59:59.999',   -- Maximum TIME
           9223372036854775807,
           null,  -- TODO time uuid
           127,             -- Maximum TINYINT
          00e4afef-52f8-4e1f-9afa-0632c8ccf790,
           'abcdef',
           9223372036854775807  -- Maximum VARINT
       );
