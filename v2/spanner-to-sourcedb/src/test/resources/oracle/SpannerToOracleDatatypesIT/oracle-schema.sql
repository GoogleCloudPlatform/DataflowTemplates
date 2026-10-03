CREATE TABLE "default_types_table" (
  "id" VARCHAR2(255) PRIMARY KEY,
  "varchar2_col" VARCHAR2(255),
  "varchar_col" VARCHAR(255),
  "char_col" CHAR(255),
  "character_col" CHARACTER(255),
  "nvarchar2_col" NVARCHAR2(255),
  "nchar_col" NCHAR(255),
  "nchar_varying_col" NCHAR VARYING(255),
  "national_character_col" NATIONAL CHARACTER(255),
  "national_char_col" NATIONAL CHAR(255),
  "national_character_varying_col" NATIONAL CHARACTER VARYING(255),
  "national_char_varying_col" NATIONAL CHAR VARYING(255),
  "number_col" NUMBER,
  "numeric_col" NUMERIC,
  "decimal_col" DECIMAL,
  "dec_col" DEC,
  "float_col" FLOAT,
  "double_precision_col" DOUBLE PRECISION,
  "real_col" REAL,
  "binary_float_col" BINARY_FLOAT,
  "binary_double_col" BINARY_DOUBLE,
  "integer_col" INTEGER,
  "int_col" INT,
  "smallint_col" SMALLINT,
  "date_col" DATE,
  "timestamp_col" TIMESTAMP,
  "timestamp_with_time_zone_col" TIMESTAMP WITH TIME ZONE,
  "timestamp_with_local_time_zone_col" TIMESTAMP WITH LOCAL TIME ZONE,
  "raw_col" RAW(255),
  "blob_col" BLOB,
  "clob_col" CLOB,
  "nclob_col" NCLOB,
  "boolean_col" NUMBER(1),
  "json_col" CLOB
);

CREATE TABLE "alt_types_table" (
  "id" VARCHAR2(255) PRIMARY KEY,
  "varchar2_to_string_col" VARCHAR2(255),
  "varchar_to_string_col" VARCHAR(255),
  "char_to_string_col" CHAR(255),
  "character_to_string_col" CHARACTER(255),
  "nvarchar2_to_string_col" NVARCHAR2(255),
  "nchar_to_string_col" NCHAR(255),
  "nchar_varying_to_string_col" NCHAR VARYING(255),
  "national_character_to_string_col" NATIONAL CHARACTER(255),
  "national_char_to_string_col" NATIONAL CHAR(255),
  "national_character_varying_to_string_col" NATIONAL CHARACTER VARYING(255),
  "national_char_varying_to_string_col" NATIONAL CHAR VARYING(255),
  "number_to_float64_col" NUMBER,
  "numeric_to_float64_col" NUMERIC,
  "decimal_to_float64_col" DECIMAL,
  "dec_to_float64_col" DEC,
  "float_to_float64_col" FLOAT,
  "double_precision_to_numeric_col" DOUBLE PRECISION,
  "real_to_string_col" REAL,
  "binary_float_to_float64_col" BINARY_FLOAT,
  "binary_double_to_string_col" BINARY_DOUBLE,
  "integer_to_numeric_col" INTEGER,
  "int_to_numeric_col" INT,
  "smallint_to_numeric_col" SMALLINT,
  "date_to_date_col" DATE,
  "timestamp_to_string_col" TIMESTAMP,
  "timestamp_with_time_zone_to_string_col" TIMESTAMP WITH TIME ZONE,
  "timestamp_with_local_time_zone_to_string_col" TIMESTAMP WITH LOCAL TIME ZONE,
  "raw_to_bytes_col" RAW(255),
  "blob_to_string_col" BLOB,
  "clob_to_bytes_col" CLOB,
  "nclob_to_bytes_col" NCLOB,
  "boolean_to_int64_col" NUMBER(1),
  "json_to_string_col" CLOB
);

CREATE TABLE "varchar2_pk_table" (
  "varchar2_pk_col" VARCHAR2(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "varchar_pk_table" (
  "varchar_pk_col" VARCHAR(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "char_pk_table" (
  "char_pk_col" CHAR(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "character_pk_table" (
  "character_pk_col" CHARACTER(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "nvarchar2_pk_table" (
  "nvarchar2_pk_col" NVARCHAR2(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "nchar_pk_table" (
  "nchar_pk_col" NCHAR(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "nchar_varying_pk_table" (
  "nchar_varying_pk_col" NCHAR VARYING(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "national_character_pk_table" (
  "national_character_pk_col" NATIONAL CHARACTER(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "national_char_pk_table" (
  "national_char_pk_col" NATIONAL CHAR(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "national_character_varying_pk_table" (
  "national_character_varying_pk_col" NATIONAL CHARACTER VARYING(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "national_char_varying_pk_table" (
  "national_char_varying_pk_col" NATIONAL CHAR VARYING(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "number_pk_table" (
  "number_pk_col" NUMBER PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "dec_pk_table" (
  "dec_pk_col" DEC PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "integer_pk_table" (
  "integer_pk_col" INTEGER PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "int_pk_table" (
  "int_pk_col" INT PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "smallint_pk_table" (
  "smallint_pk_col" SMALLINT PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "date_pk_table" (
  "date_pk_col" DATE PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "timestamp_pk_table" (
  "timestamp_pk_col" TIMESTAMP PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "raw_pk_table" (
  "raw_pk_col" RAW(255) PRIMARY KEY,
  "val" VARCHAR2(255)
);

CREATE TABLE "boolean_pk_table" (
  "boolean_pk_col" NUMBER(1) PRIMARY KEY,
  "val" VARCHAR2(255)
);

