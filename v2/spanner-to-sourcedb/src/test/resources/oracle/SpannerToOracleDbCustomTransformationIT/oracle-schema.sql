CREATE TABLE "Users1" (
    "id" INT NOT NULL,
    "first_name" VARCHAR2(25),
    "last_name" VARCHAR2(25),
 PRIMARY KEY("id"));

CREATE TABLE "AllDatatypeTransformation" (
    "varchar_column" VARCHAR2(20) NOT NULL,
    "source_only_pk" INT NOT NULL,
    "tinyint_column" NUMBER,
    "text_column" CLOB,
    "date_column" DATE,
    "int_column" INT,
    "bigint_column" NUMBER,
    "float_column" FLOAT,
    "double_column" DOUBLE PRECISION,
    "decimal_column" DECIMAL(10,2),
    "datetime_column" TIMESTAMP,
    "timestamp_column" TIMESTAMP,
    "time_column" VARCHAR2(20),
    "year_column" VARCHAR2(10),
    "blob_column" BLOB,
    "enum_column" VARCHAR2(20),
    "bool_column" NUMBER(1),
    "binary_column" RAW(150),
    "bit_column" RAW(10),
    PRIMARY KEY ("source_only_pk", "varchar_column")
);
