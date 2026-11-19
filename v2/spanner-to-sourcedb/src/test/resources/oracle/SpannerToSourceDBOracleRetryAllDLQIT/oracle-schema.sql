CREATE TABLE "Customers" (
    "CustomerId" NUMBER NOT NULL PRIMARY KEY,
    "CustomerName" VARCHAR2(255),
    "CreditLimit" NUMBER(10, 2) NOT NULL,
    "LegacyRegion" VARCHAR2(50),
    CONSTRAINT "CHK_CreditLimit" CHECK ("CreditLimit" > 1000)
);

CREATE TABLE "Orders" (
    "CustomerId" NUMBER NOT NULL,
    "OrderId" NUMBER NOT NULL,
    "OrderValue" NUMBER(10, 2),
    "LegacyOrderSystem" VARCHAR2(50) NOT NULL,
    PRIMARY KEY ("CustomerId", "LegacyOrderSystem", "OrderId"),
    CONSTRAINT "FK_CustomerOrder" FOREIGN KEY ("CustomerId") REFERENCES "Customers"("CustomerId")
);

CREATE TABLE "AllDataTypes" (
    "id" NUMBER NOT NULL PRIMARY KEY,
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
    "float_col" FLOAT DEFAULT NULL,
    "double_col" BINARY_DOUBLE DEFAULT NULL,
    "decimal_col" NUMBER DEFAULT NULL,
    "datetime_col" TIMESTAMP DEFAULT NULL,
    "time_col" VARCHAR2(50) DEFAULT NULL,
    "year_col" VARCHAR2(4) DEFAULT NULL,
    "char_col" CHAR(255) DEFAULT NULL,
    "tinyblob_col" RAW(255) DEFAULT NULL,
    "tinytext_col" VARCHAR2(255) DEFAULT NULL,
    "blob_col" BLOB DEFAULT NULL,
    "mediumblob_col" BLOB DEFAULT NULL,
    "mediumtext_col" CLOB DEFAULT NULL,
    "test_json_col" CLOB DEFAULT NULL,
    "longblob_col" BLOB DEFAULT NULL,
    "longtext_col" CLOB DEFAULT NULL,
    "enum_col" VARCHAR2(50) DEFAULT NULL,
    "bool_col" NUMBER(1) DEFAULT NULL,
    "binary_col" RAW(255) DEFAULT NULL,
    "varbinary_col" RAW(1000) DEFAULT NULL,
    "bit_col" RAW(8) DEFAULT NULL,
    "bit8_col" NUMBER DEFAULT NULL,
    "bit1_col" NUMBER(1) DEFAULT NULL,
    "boolean_col" NUMBER(1) DEFAULT NULL,
    "int_col" NUMBER DEFAULT NULL,
    "integer_unsigned_col" NUMBER DEFAULT NULL,
    "timestamp_col" TIMESTAMP DEFAULT NULL,
    "set_col" VARCHAR2(255) DEFAULT NULL
);
