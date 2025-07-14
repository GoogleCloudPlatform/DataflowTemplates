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
CREATE TABLE list_types_table (
                                  primary_key UUID PRIMARY KEY,
                                  ascii_list LIST<ASCII>,
                                  frozen_ascii_list FROZEN<LIST<ASCII>>,
                                  bigint_list LIST<BIGINT>,
                                  blob_list LIST<BLOB>,
                                  boolean_list LIST<BOOLEAN>,
                                  date_list LIST<DATE>,
                                  decimal_list LIST<DECIMAL>,
                                  double_list LIST<DOUBLE>,
                                  duration_list LIST<DURATION>,
                                  float_list LIST<FLOAT>,
                                  inet_list LIST<INET>,
                                  int_list LIST<INT>,
                                  smallint_list LIST<SMALLINT>,
                                  text_list LIST<TEXT>,
                                  time_list LIST<TIME>,
                                  timestamp_list LIST<TIMESTAMP>,
                                  timeuuid_list LIST<TIMEUUID>,
                                  tinyint_list LIST<TINYINT>,
                                  uuid_list LIST<UUID>,
                                  varchar_list LIST<VARCHAR>,
                                  varint_list LIST<VARINT>
);

CREATE TABLE set_types_table (
                                 primary_key UUID PRIMARY KEY,
                                 ascii_set SET<ASCII>,
                                 frozen_ascii_set FROZEN<SET<ASCII>>,
                                 bigint_set SET<BIGINT>,
                                 blob_set SET<BLOB>,
                                 boolean_set SET<BOOLEAN>,
                                 date_set SET<DATE>,
                                 decimal_set SET<DECIMAL>,
                                 double_set SET<DOUBLE>,
                                 -- duration can not be added as a type set in cassandra.
                                 float_set SET<FLOAT>,
                                 inet_set SET<INET>,
                                 int_set SET<INT>,
                                 smallint_set SET<SMALLINT>,
                                 text_set SET<TEXT>,
                                 time_set SET<TIME>,
                                 timestamp_set SET<TIMESTAMP>,
                                 timeuuid_set SET<TIMEUUID>,
                                 tinyint_set SET<TINYINT>,
                                 uuid_set SET<UUID>,
                                 varchar_set SET<VARCHAR>,
                                 varint_set SET<VARINT>
);

CREATE TABLE map_types_table (
                                 primary_key UUID PRIMARY KEY,
                                 ascii_text_map MAP<ASCII, TEXT>,
                                 bigint_boolean_map MAP<BIGINT, BOOLEAN>,
                                 blob_int_map MAP<BLOB, INT>,
                                 boolean_decimal_map MAP<BOOLEAN, DECIMAL>,
                                 date_double_map MAP<DATE, DOUBLE>,
                                 decimal_duration_map MAP<DECIMAL, DURATION>,
                                 double_float_map MAP<DOUBLE, FLOAT>,
                                 -- duration is not allowed to be a map key.
                                 double_inet_map MAP<DOUBLE, INET>,
                                 float_smallint_map MAP<FLOAT, SMALLINT>,
                                 inet_text_map MAP<INET, TEXT>,
                                 int_time_map MAP<INT, TIME>,
                                 smallint_timestamp_map MAP<SMALLINT, TIMESTAMP>,
                                 text_timeuuid_map MAP<TEXT, TIMEUUID>,
                                 time_tinyint_map MAP<TIME, TINYINT>,
                                 timestamp_uuid_map MAP<TIMESTAMP, UUID>,
                                 timeuuid_varchar_map MAP<TIMEUUID, VARCHAR>,
                                 tinyint_varint_map MAP<TINYINT, VARINT>,
                                 uuid_ascii_map MAP<UUID, ASCII>,
                                 varchar_bigint_map MAP<VARCHAR, BIGINT>,
                                 varint_blob_map MAP<VARINT, BLOB>
);



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
-- Inserts for list_types_table
-- Cassandra does not support null inside of collection values.
INSERT INTO list_types_table (primary_key, ascii_list, frozen_ascii_list, bigint_list, blob_list, boolean_list, date_list, decimal_list, double_list, duration_list, float_list, inet_list, int_list, smallint_list, text_list, time_list, timestamp_list, timeuuid_list, tinyint_list, uuid_list, varchar_list, varint_list)
VALUES (
           a389de30-f01f-4395-a0c6-c407bfbe81d0, -- primary_key
           ['a', 'b', 'c'], -- ascii_list
           ['d', 'e', 'f'], -- frozen_ascii_list
           [1, 2, 3], -- bigint_list
           [asciiAsBlob('Hello')], -- blob_list (Hex representation of "Hello")
           [true, false], -- boolean_list
           ['2024-10-27', '2024-10-28'], -- date_list
           [123.45, 678.90], -- decimal_list
           [1.23, 4.56], -- double_list
           [1y2mo3d4h5m6s, 2y2mo3d4h5m6s], -- duration_list
           [1.23, 4.56],  -- float_list
           ['192.168.1.1', '10.0.0.1'], -- inet_list
           [10, 20, 30], -- int_list
           [100, 200, 300], -- smallint_list
           ['hello', 'world'], -- text_list
           ['10:00:00', '12:00:00'], -- time_list
           ['2024-10-27T10:00:00', '2024-10-28T12:00:00'], -- timestamp_list
           [9b9419da-b210-11ef-890e-9d9a41af9e54], -- timeuuid_list
           [1, 2, 3],  -- tinyint_list
           [f0e1d922-06b5-4f07-a7a6-ec0c9f23e172], -- uuid_list
           ['varchar1', 'varchar2'], -- varchar_list
           [1234567890, 9876543210] -- varint_list
       );



-- Inserts for set_types_table
-- Cassandra does not support null inside of collection values.
INSERT INTO set_types_table (primary_key, ascii_set, frozen_ascii_set, bigint_set, blob_set, boolean_set, date_set, decimal_set, double_set, float_set, inet_set, int_set, smallint_set, text_set, time_set, timestamp_set, timeuuid_set, tinyint_set, uuid_set, varchar_set, varint_set)
VALUES (
           a389de30-f01f-4395-a0c6-c407bfbe81d0, -- primary_key
           {'a', 'b', 'c'}, -- ascii_set
           {'d', 'e', 'f'}, -- frozen_ascii_set
           {1, 2, 3}, -- bigint_set
           {0x48656C6C6F}, -- blob_set (Hex representation of "Hello")
           {true, false}, -- boolean_set
           {'2024-10-27', '2024-10-28'}, -- date_set
           {123.45, 678.90}, -- decimal_set
           {1.23, 4.56}, -- double_set
           {1.23, 4.56},  -- float_set
           {'192.168.1.1', '10.0.0.1'}, -- inet_set
           {10, 20, 30}, -- int_set
           {100, 200, 300}, -- smallint_set
           {'hello', 'world'}, -- text_set
           {'10:00:00', '12:00:00'}, -- time_set
           {'2024-10-27T10:00:00', '2024-10-28T12:00:00'}, -- timestamp_set
           {9b9419da-b210-11ef-890e-9d9a41af9e54}, -- timeuuid_set
           {1, 2, 3},  -- tinyint_set
           {f0e1d922-06b5-4f07-a7a6-ec0c9f23e172}, -- uuid_set
           {'varchar1', 'varchar2'}, -- varchar_set
           {1234567890, 9876543210} -- varint_set
       );

-- Inserts into Map with values
INSERT INTO map_types_table (primary_key, ascii_text_map, bigint_boolean_map, blob_int_map, boolean_decimal_map, date_double_map, decimal_duration_map, double_float_map, double_inet_map, float_smallint_map, inet_text_map, int_time_map, smallint_timestamp_map, text_timeuuid_map, time_tinyint_map, timestamp_uuid_map, timeuuid_varchar_map, tinyint_varint_map, uuid_ascii_map, varchar_bigint_map, varint_blob_map)
VALUES (
           550e8400-e29b-41d4-a716-446655440000, -- primay_key
           {'key1': 'value1', 'key2': 'value2'}, -- ascii_text_map
           {123: true, 456: false}, -- bigint_boolean_map
           {0x010203: 456, 0x040506: 789}, --  blob_int_map
           {true: 123.456, false: 456.789}, -- boolean_decimal_map
           {'2024-01-15': 1.23, '2024-01-16': 4.56}, -- date_double_map
           {123.456: 1d12h34m56s, 456.789: 18h14m91s}, -- decimal_duration_map
           {12.34: 1.23, 45.67: 4.56}, --  double_float_map
           {3.14: '192.168.1.1', 6.28: '127.0.0.1'}, -- float_smallint_map
           {1.23: 10, 4.56: 20}, -- float_smallint_map
           {'192.168.1.1': 'some text', '127.0.0.1': 'other text'}, -- inet_text_map
           {10: '10:30:00', 20: '11:45:00'}, -- int_time_map
           {100: '2024-01-15T10:30:00Z', 200: '2024-01-16T11:45:00Z'}, -- smallint_timestamp_map
           {'text1': 9b9419da-b210-11ef-890e-9d9a41af9e54, 'text2': 9b9419da-b210-11ef-890e-9d9a41af9e54}, -- text_timeuuid_map
           {'10:30:00': 1, '11:45:00': 2}, -- time_tinyint_map
           {'2024-01-15T10:30:00Z': 550e8400-e29b-41d4-a716-446655440001, '2024-01-16T11:45:00Z': 550e8400-e29b-41d4-a716-446655440002}, -- timestamp_uuid_map
           {9b9419da-b210-11ef-890e-9d9a41af9e54: 'varchar1'}, -- timeuuid_varchar_map
           {1: 789, 2: 1234}, -- tinyint_varint_map
           {550e8400-e29b-41d4-a716-446655440003: 'ascii1', 550e8400-e29b-41d4-a716-446655440004: 'ascii2'}, -- uuid_ascii_map
           {'varchar1': 123, 'varchar2': 456}, --  varchar_bigint_map
           {789: 0x010203, 1234: 0x040506} -- varint_blob_map
       );

-- Insert with empty maps

INSERT INTO map_types_table (primary_key, ascii_text_map, bigint_boolean_map, blob_int_map, boolean_decimal_map, date_double_map, decimal_duration_map, double_float_map, double_inet_map, float_smallint_map, inet_text_map, int_time_map, smallint_timestamp_map, text_timeuuid_map, time_tinyint_map, timestamp_uuid_map, timeuuid_varchar_map, tinyint_varint_map, uuid_ascii_map, varchar_bigint_map, varint_blob_map)
VALUES (
           550e8400-e29b-41d4-a716-446655440005,
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {},
           {}
       );