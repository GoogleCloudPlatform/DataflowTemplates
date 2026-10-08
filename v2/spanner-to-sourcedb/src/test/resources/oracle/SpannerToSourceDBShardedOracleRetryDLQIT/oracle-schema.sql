CREATE TABLE "Customers" (
    "CustomerId" NUMBER NOT NULL PRIMARY KEY,
    "CustomerName" VARCHAR2(255),
    "CreditLimit" NUMBER(10, 2) NOT NULL,
    "LegacyRegion" VARCHAR2(50),
    CONSTRAINT CHK_CreditLimit CHECK ("CreditLimit" > 1000)
);

CREATE TABLE "Orders" (
    "CustomerId" NUMBER NOT NULL,
    "OrderId" NUMBER NOT NULL,
    "OrderValue" NUMBER(10, 2),
    "LegacyOrderSystem" VARCHAR2(50) NOT NULL,
    PRIMARY KEY ("CustomerId", "LegacyOrderSystem", "OrderId"),
    CONSTRAINT FK_CustomerOrder FOREIGN KEY ("CustomerId") REFERENCES "Customers"("CustomerId")
);

CREATE TABLE "AllDataTypes" (
    "id" NUMBER PRIMARY KEY,
    "varchar_col" VARCHAR2(1000),
    "tinyint_col" NUMBER,
    "tinyint_unsigned_col" NUMBER,
    "text_col" CLOB,
    "date_col" DATE,
    "smallint_col" NUMBER,
    "smallint_unsigned_col" NUMBER,
    "mediumint_col" NUMBER,
    "mediumint_unsigned_col" NUMBER,
    "bigint_col" NUMBER,
    "bigint_unsigned_col" NUMBER,
    "float_col" FLOAT,
    "double_col" FLOAT,
    "decimal_col" NUMBER,
    "datetime_col" TIMESTAMP,
    "time_col" VARCHAR2(50),
    "year_col" NUMBER,
    "char_col" CHAR(255),
    "tinyblob_col" BLOB,
    "tinytext_col" CLOB,
    "blob_col" BLOB,
    "mediumblob_col" BLOB,
    "mediumtext_col" CLOB,
    "test_json_col" CLOB,
    "longblob_col" BLOB,
    "longtext_col" CLOB,
    "enum_col" VARCHAR2(255),
    "bool_col" NUMBER(1),
    "binary_col" RAW(255),
    "varbinary_col" RAW(1000),
    "bit_col" RAW(64),
    "bit8_col" NUMBER,
    "bit1_col" NUMBER(1),
    "boolean_col" NUMBER(1),
    "int_col" NUMBER,
    "integer_unsigned_col" NUMBER,
    "timestamp_col" TIMESTAMP,
    "set_col" VARCHAR2(255)
);
