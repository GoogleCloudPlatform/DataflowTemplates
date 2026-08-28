CREATE TABLE AllDatatypes (
    id NUMBER(38, 0) PRIMARY KEY,
    varchar_col VARCHAR2(2000),
    varchar2_col VARCHAR2(2000),
    char_col CHAR(255),
    nvarchar2_col NVARCHAR2(2000),
    nchar_col NCHAR(255),
    number_col NUMBER(38, 0),
    numeric_col NUMBER(38, 0),
    decimal_col NUMBER(38, 10),
    float_col FLOAT,
    double_precision_col DOUBLE PRECISION,
    binary_float_col BINARY_FLOAT,
    binary_double_col BINARY_DOUBLE,
    integer_col INTEGER,
    int_col INT,
    smallint_col SMALLINT,
    date_col DATE,
    timestamp_col TIMESTAMP(6),
    timestamp_tz_col TIMESTAMP(6) WITH TIME ZONE,
    timestamp_ltz_col TIMESTAMP(6) WITH LOCAL TIME ZONE,
    interval_ym_col INTERVAL YEAR TO MONTH,
    interval_ds_col INTERVAL DAY TO SECOND,
    raw_col RAW(2000),
    clob_col CLOB,
    nclob_col NCLOB,
    blob_col BLOB,
    rowid_col ROWID,
    json_col VARCHAR2(4000)
);

-- Row 1 (Standard Values)
INSERT INTO AllDatatypes (
    id, varchar_col, varchar2_col, char_col, nvarchar2_col, nchar_col,
    number_col, numeric_col, decimal_col, float_col, double_precision_col,
    binary_float_col, binary_double_col, integer_col, int_col, smallint_col,
    date_col, timestamp_col, timestamp_tz_col, timestamp_ltz_col,
    interval_ym_col, interval_ds_col, raw_col, clob_col, nclob_col, blob_col,
    rowid_col, json_col
) VALUES (
    1, 'varchar', 'varchar2', 'char', 'nvarchar2', 'nchar',
    12345, 12345, 123.456, 123.45, 123.45,
    123.45, 123.45, 12345, 12345, 123,
    TO_DATE('2024-01-01 10:00:00', 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP('2024-01-01 10:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'),
    TO_TIMESTAMP_TZ('2024-01-01 10:00:00 +00:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'),
    TO_TIMESTAMP('2024-01-01 10:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'),
    INTERVAL '1-2' YEAR TO MONTH,
    INTERVAL '1 02:03:04.555555' DAY TO SECOND,
    HEXTORAW('414243'),
    'clob text',
    'nclob text',
    HEXTORAW('414243'),
    'AAAB12AADAAAA12AAA',
    '{}'
);

-- Row 2 (All NULL values)
INSERT INTO AllDatatypes (id) VALUES (2);

-- Row 3 (Minimum Values)
INSERT INTO AllDatatypes (
    id, varchar_col, varchar2_col, char_col, nvarchar2_col, nchar_col,
    number_col, numeric_col, decimal_col, float_col, double_precision_col,
    binary_float_col, binary_double_col, integer_col, int_col, smallint_col,
    date_col, timestamp_col, timestamp_tz_col, timestamp_ltz_col,
    interval_ym_col, interval_ds_col, raw_col, clob_col, nclob_col, blob_col,
    rowid_col, json_col
) VALUES (
    3, 'a', 'a', 'a', 'a', 'a',
    -999999999999999999, -999999999999999999, -999999999999999.9999999999, -1.0E38, -1.0E308,
    -3.402823E+38, -1.7976931348623157E+308, -2147483648, -2147483648, -32768,
    TO_DATE('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP('1970-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'),
    TO_TIMESTAMP_TZ('1970-01-01 00:00:00 +00:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'),
    TO_TIMESTAMP('1970-01-01 00:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'),
    INTERVAL '-99-11' YEAR TO MONTH,
    INTERVAL '-99 23:59:59.999999' DAY TO SECOND,
    HEXTORAW('00'),
    'min clob',
    'min nclob',
    HEXTORAW('00'),
    'AAAAAAAAAAAAAAAAAA',
    '{}'
);

-- Row 4 (Maximum Values)
INSERT INTO AllDatatypes (
    id, varchar_col, varchar2_col, char_col, nvarchar2_col, nchar_col,
    number_col, numeric_col, decimal_col, float_col, double_precision_col,
    binary_float_col, binary_double_col, integer_col, int_col, smallint_col,
    date_col, timestamp_col, timestamp_tz_col, timestamp_ltz_col,
    interval_ym_col, interval_ds_col, raw_col, clob_col, nclob_col, blob_col,
    rowid_col, json_col
) VALUES (
    4, RPAD('Z', 2000, 'Z'), RPAD('Z', 2000, 'Z'), RPAD('Z', 255, 'Z'), RPAD('Z', 2000, 'Z'), RPAD('Z', 255, 'Z'),
    999999999999999999, 999999999999999999, 999999999999999.9999999999, 1.0E38, 1.0E308,
    3.402823E+38, 1.7976931348623157E+308, 2147483647, 2147483647, 32767,
    TO_DATE('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP('9999-12-31 23:59:59.999999', 'YYYY-MM-DD HH24:MI:SS.FF6'),
    TO_TIMESTAMP_TZ('9999-12-31 23:59:59 +00:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'),
    TO_TIMESTAMP('9999-12-31 23:59:59.999999', 'YYYY-MM-DD HH24:MI:SS.FF6'),
    INTERVAL '99-11' YEAR TO MONTH,
    INTERVAL '99 23:59:59.999999' DAY TO SECOND,
    HEXTORAW('FFFF'),
    RPAD('Z', 4000, 'Z'),
    RPAD('Z', 4000, 'Z'),
    HEXTORAW('FFFF'),
    'ZZZZZZZZZZZZZZZZZZ',
    '{}'
);
