CREATE TABLE "AllDatatypeTransformation" (
    "varchar_column" VARCHAR2(20) NOT NULL,
    "tinyint_column" INTEGER,
    "text_column" CLOB,
    "date_column" DATE,
    "int_column" INTEGER,
    "bigint_column" INTEGER,
    "float_column" BINARY_DOUBLE,
    "double_column" BINARY_DOUBLE,
    "decimal_column" NUMBER,
    "datetime_column" TIMESTAMP,
    "timestamp_column" TIMESTAMP,
    "time_column" VARCHAR2(50),
    "year_column" INTEGER,
    "blob_column" BLOB,
    "enum_column" VARCHAR2(10),
    "bool_column" NUMBER(1),
    "binary_column" RAW(2000),
    "bit_column" RAW(2000),
    "varbinary_column" RAW(2000),
    "char_column" CHAR(255),
    "longblob_column" BLOB,
    "longtext_column" CLOB,
    "mediumblob_column" BLOB,
    "mediumint_column" INTEGER,
    "mediumtext_column" CLOB,
    "set_column" VARCHAR2(200),
    "smallint_column" INTEGER,
    "tinyblob_column" RAW(255),
    "tinytext_column" VARCHAR2(255),
    "json_column" CLOB,
    "dropped_column" VARCHAR2(20),
    PRIMARY KEY ("int_column")
)

-- SPLIT --

INSERT INTO "AllDatatypeTransformation" (
    "varchar_column", "tinyint_column", "text_column", "date_column", "int_column",
    "bigint_column", "float_column", "double_column", "decimal_column", "datetime_column",
    "timestamp_column", "time_column", "year_column", "blob_column", "enum_column",
    "bool_column", "varbinary_column", "bit_column", "binary_column", "char_column", "longblob_column",
    "longtext_column", "mediumblob_column", "mediumint_column", "mediumtext_column", "set_column", "smallint_column",
    "tinyblob_column", "tinytext_column", "json_column", "dropped_column"
)
VALUES (
    'id1', 12, 'This is a text value', TO_DATE('2024-06-21', 'YYYY-MM-DD'), 100,
    134567890, 3.14159, 2.71828, 12345.6789, TO_TIMESTAMP('2024-06-21 17:10:01', 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP('2022-12-31 23:59:58', 'YYYY-MM-DD HH24:MI:SS'), '17:00:00', 2024, HEXTORAW('7835383030'), '2',
    0, HEXTORAW('7835383030000000000000000000000000000000'), HEXTORAW('0042'), HEXTORAW('7835383030000000000000000000000000000000'),
    'a', HEXTORAW('7835383030'), 'This is longtext', HEXTORAW('7835383030'), 2000, 'This is mediumtext',
    'v1,v2', 10, HEXTORAW('7835383030'), 'This is tinytext', '{"k1": "v1"}', 'dropped_value'
)

-- SPLIT --

COMMIT
