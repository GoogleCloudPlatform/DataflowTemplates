CREATE TABLE "Customers" (
    "CustomerId" NUMBER(10) NOT NULL PRIMARY KEY,
    "CustomerName" VARCHAR2(255),
    "CreditLimit" NUMBER(10, 2) NOT NULL,
    "LegacyRegion" VARCHAR2(50),
    CONSTRAINT "CHK_CreditLimit" CHECK ("CreditLimit" > 1000)
);

CREATE TABLE "Orders" (
    "CustomerId" NUMBER(10) NOT NULL,
    "OrderId" NUMBER(10) NOT NULL,
    "OrderValue" NUMBER(10, 2),
    "LegacyOrderSystem" VARCHAR2(50) NOT NULL,
    PRIMARY KEY ("CustomerId", "LegacyOrderSystem", "OrderId"),
    CONSTRAINT "FK_CustomerOrder" FOREIGN KEY ("CustomerId") REFERENCES "Customers"("CustomerId")
);

CREATE TABLE "AllDataTypes" (
    "id" NUMBER(10) PRIMARY KEY,
    "varchar_col" VARCHAR2(1000) DEFAULT NULL,
    "tinyint_col" NUMBER(3) DEFAULT NULL,
    "tinyint_unsigned_col" NUMBER(3) DEFAULT NULL,
    "text_col" CLOB DEFAULT NULL,
    "date_col" DATE DEFAULT NULL,
    "smallint_col" NUMBER(5) DEFAULT NULL,
    "smallint_unsigned_col" NUMBER(5) DEFAULT NULL,
    "mediumint_col" NUMBER(7) DEFAULT NULL,
    "mediumint_unsigned_col" NUMBER(7) DEFAULT NULL,
    "bigint_col" NUMBER(19) DEFAULT NULL,
    "bigint_unsigned_col" NUMBER(20) DEFAULT NULL,
    "float_col" BINARY_FLOAT DEFAULT NULL,
    "double_col" BINARY_DOUBLE DEFAULT NULL,
    "decimal_col" NUMBER(38, 10) DEFAULT NULL,
    "datetime_col" TIMESTAMP DEFAULT NULL,
    "time_col" VARCHAR2(50) DEFAULT NULL,
    "year_col" VARCHAR2(4) DEFAULT NULL,
    "char_col" CHAR(255) DEFAULT NULL,
    "tinyblob_col" BLOB DEFAULT NULL,
    "tinytext_col" CLOB DEFAULT NULL,
    "blob_col" BLOB DEFAULT NULL,
    "mediumblob_col" BLOB DEFAULT NULL,
    "mediumtext_col" CLOB DEFAULT NULL,
    "test_json_col" CLOB DEFAULT NULL,
    "longblob_col" BLOB DEFAULT NULL,
    "longtext_col" CLOB DEFAULT NULL,
    "enum_col" VARCHAR2(10) DEFAULT NULL,
    "bool_col" NUMBER(1) DEFAULT NULL,
    "binary_col" RAW(255) DEFAULT NULL,
    "varbinary_col" RAW(1000) DEFAULT NULL,
    "bit_col" RAW(8) DEFAULT NULL,
    "bit8_col" RAW(1) DEFAULT NULL,
    "bit1_col" RAW(1) DEFAULT NULL,
    "boolean_col" NUMBER(1) DEFAULT NULL,
    "int_col" NUMBER(10) DEFAULT NULL,
    "integer_unsigned_col" NUMBER(10) DEFAULT NULL,
    "timestamp_col" TIMESTAMP DEFAULT NULL,
    "set_col" VARCHAR2(50) DEFAULT NULL
);
