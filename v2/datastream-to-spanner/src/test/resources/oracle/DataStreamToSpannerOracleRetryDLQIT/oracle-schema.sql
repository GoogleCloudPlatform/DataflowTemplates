CREATE TABLE "Customers" (
    "CustomerId" NUMBER NOT NULL PRIMARY KEY,
    "CustomerName" VARCHAR2(255),
    "CreditLimit" NUMBER(10, 2) NOT NULL,
    "LoyaltyTier" VARCHAR2(50)
);

CREATE TABLE "Orders" (
    "CustomerId" NUMBER NOT NULL,
    "OrderId" NUMBER NOT NULL,
    "OrderValue" NUMBER(10, 2),
    "OrderSource" VARCHAR2(50) NOT NULL,
    PRIMARY KEY ("CustomerId", "OrderId")
);

CREATE TABLE "AllDataTypes" (
    "id" NUMBER PRIMARY KEY,
    "varchar_col" VARCHAR2(1000) DEFAULT NULL,
    "tinyint_col" NUMBER DEFAULT NULL,
    "tinyint_unsigned_col" NUMBER DEFAULT NULL,
    "text_col" CLOB DEFAULT NULL,
    "date_col" DATE DEFAULT NULL,
    "smallint_col" NUMBER DEFAULT NULL,
    "smallint_unsigned_col" NUMBER DEFAULT NULL,
    "mediumint_col" NUMBER DEFAULT NULL,
    "mediumint_unsigned_col" NUMBER DEFAULT NULL,
    "bigint_col" NUMBER DEFAULT NULL,
    "bigint_unsigned_col" NUMBER DEFAULT NULL,
    "float_col" BINARY_FLOAT DEFAULT NULL,
    "double_col" BINARY_DOUBLE DEFAULT NULL,
    "decimal_col" NUMBER(38,9) DEFAULT NULL,
    "datetime_col" TIMESTAMP DEFAULT NULL,
    "time_col" VARCHAR2(50) DEFAULT NULL,
    "year_col" VARCHAR2(4) DEFAULT NULL,
    "char_col" CHAR(255) DEFAULT NULL,
    "tinyblob_col" RAW(255) DEFAULT NULL,
    "tinytext_col" CLOB DEFAULT NULL,
    "blob_col" BLOB DEFAULT NULL,
    "mediumblob_col" BLOB DEFAULT NULL,
    "mediumtext_col" CLOB DEFAULT NULL,
    "test_json_col" VARCHAR2(4000) DEFAULT NULL,
    "longblob_col" BLOB DEFAULT NULL,
    "longtext_col" CLOB DEFAULT NULL,
    "enum_col" VARCHAR2(255) DEFAULT NULL,
    "bool_col" NUMBER(1) DEFAULT NULL,
    "binary_col" RAW(255) DEFAULT NULL,
    "varbinary_col" RAW(1000) DEFAULT NULL,
    "bit_col" RAW(8) DEFAULT NULL,
    "bit8_col" RAW(1) DEFAULT NULL,
    "bit1_col" NUMBER(1) DEFAULT NULL,
    "boolean_col" NUMBER(1) DEFAULT NULL,
    "int_col" NUMBER DEFAULT NULL,
    "integer_unsigned_col" NUMBER DEFAULT NULL,
    "timestamp_col" TIMESTAMP DEFAULT NULL,
    "set_col" VARCHAR2(255) DEFAULT NULL
);

