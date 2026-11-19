CREATE TABLE "varchar2_table" (
  "id" bigint NOT NULL,
  "varchar2_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "varchar2_to_str4000_table" (
  "id" bigint NOT NULL,
  "varchar2_to_str4000_col" varchar(4000),
  PRIMARY KEY ("id")
);

CREATE TABLE "varchar2_to_bytes_table" (
  "id" bigint NOT NULL,
  "varchar2_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "varchar2_pk_table" (
  "varchar2_pk_col" varchar NOT NULL,
  PRIMARY KEY ("varchar2_pk_col")
);

CREATE TABLE "varchar_table" (
  "id" bigint NOT NULL,
  "varchar_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "varchar_to_str4000_table" (
  "id" bigint NOT NULL,
  "varchar_to_str4000_col" varchar(4000),
  PRIMARY KEY ("id")
);

CREATE TABLE "varchar_to_bytes_table" (
  "id" bigint NOT NULL,
  "varchar_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "varchar_pk_table" (
  "varchar_pk_col" varchar NOT NULL,
  PRIMARY KEY ("varchar_pk_col")
);

CREATE TABLE "char_table" (
  "id" bigint NOT NULL,
  "char_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "char_to_str2000_table" (
  "id" bigint NOT NULL,
  "char_to_str2000_col" varchar(2000),
  PRIMARY KEY ("id")
);

CREATE TABLE "char_to_bytes_table" (
  "id" bigint NOT NULL,
  "char_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "char_pk_table" (
  "char_pk_col" varchar NOT NULL,
  PRIMARY KEY ("char_pk_col")
);

CREATE TABLE "character_table" (
  "id" bigint NOT NULL,
  "character_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "character_to_str2000_table" (
  "id" bigint NOT NULL,
  "character_to_str2000_col" varchar(2000),
  PRIMARY KEY ("id")
);

CREATE TABLE "character_to_bytes_table" (
  "id" bigint NOT NULL,
  "character_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "character_pk_table" (
  "character_pk_col" varchar NOT NULL,
  PRIMARY KEY ("character_pk_col")
);

CREATE TABLE "nvarchar2_table" (
  "id" bigint NOT NULL,
  "nvarchar2_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "nvarchar2_to_str2000_table" (
  "id" bigint NOT NULL,
  "nvarchar2_to_str2000_col" varchar(2000),
  PRIMARY KEY ("id")
);

CREATE TABLE "nvarchar2_to_bytes_table" (
  "id" bigint NOT NULL,
  "nvarchar2_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "nvarchar2_pk_table" (
  "nvarchar2_pk_col" varchar NOT NULL,
  PRIMARY KEY ("nvarchar2_pk_col")
);

CREATE TABLE "nchar_table" (
  "id" bigint NOT NULL,
  "nchar_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "nchar_to_str1000_table" (
  "id" bigint NOT NULL,
  "nchar_to_str1000_col" varchar(1000),
  PRIMARY KEY ("id")
);

CREATE TABLE "nchar_to_bytes_table" (
  "id" bigint NOT NULL,
  "nchar_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "nchar_pk_table" (
  "nchar_pk_col" varchar NOT NULL,
  PRIMARY KEY ("nchar_pk_col")
);

CREATE TABLE "nchar_varying_table" (
  "id" bigint NOT NULL,
  "nchar_varying_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "nchar_varying_to_str2000_table" (
  "id" bigint NOT NULL,
  "nchar_varying_to_str2000_col" varchar(2000),
  PRIMARY KEY ("id")
);

CREATE TABLE "nchar_varying_to_bytes_table" (
  "id" bigint NOT NULL,
  "nchar_varying_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "nchar_varying_pk_table" (
  "nchar_varying_pk_col" varchar NOT NULL,
  PRIMARY KEY ("nchar_varying_pk_col")
);

CREATE TABLE "nat_character_table" (
  "id" bigint NOT NULL,
  "nat_character_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_character_to_str1000_table" (
  "id" bigint NOT NULL,
  "nat_character_to_str1000_col" varchar(1000),
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_character_to_bytes_table" (
  "id" bigint NOT NULL,
  "nat_character_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_character_pk_table" (
  "nat_character_pk_col" varchar NOT NULL,
  PRIMARY KEY ("nat_character_pk_col")
);

CREATE TABLE "nat_char_table" (
  "id" bigint NOT NULL,
  "nat_char_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_char_to_str1000_table" (
  "id" bigint NOT NULL,
  "nat_char_to_str1000_col" varchar(1000),
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_char_to_bytes_table" (
  "id" bigint NOT NULL,
  "nat_char_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_char_pk_table" (
  "nat_char_pk_col" varchar NOT NULL,
  PRIMARY KEY ("nat_char_pk_col")
);

CREATE TABLE "nat_characvar_table" (
  "id" bigint NOT NULL,
  "nat_characvar_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_characvar_to_str2000_table" (
  "id" bigint NOT NULL,
  "nat_characvar_to_str2000_col" varchar(2000),
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_characvar_to_bytes_table" (
  "id" bigint NOT NULL,
  "nat_characvar_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_characvar_pk_table" (
  "nat_characvar_pk_col" varchar NOT NULL,
  PRIMARY KEY ("nat_characvar_pk_col")
);

CREATE TABLE "nat_charvar_table" (
  "id" bigint NOT NULL,
  "nat_charvar_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_charvar_to_str2000_table" (
  "id" bigint NOT NULL,
  "nat_charvar_to_str2000_col" varchar(2000),
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_charvar_to_bytes_table" (
  "id" bigint NOT NULL,
  "nat_charvar_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "nat_charvar_pk_table" (
  "nat_charvar_pk_col" varchar NOT NULL,
  PRIMARY KEY ("nat_charvar_pk_col")
);

CREATE TABLE "number_table" (
  "id" bigint NOT NULL,
  "number_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "number_to_float64_table" (
  "id" bigint NOT NULL,
  "number_to_float64_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "number_to_str_table" (
  "id" bigint NOT NULL,
  "number_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "number_to_int64_table" (
  "id" bigint NOT NULL,
  "number_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "numeric_table" (
  "id" bigint NOT NULL,
  "numeric_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "numeric_to_float64_table" (
  "id" bigint NOT NULL,
  "numeric_to_float64_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "numeric_to_str_table" (
  "id" bigint NOT NULL,
  "numeric_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "numeric_to_int64_table" (
  "id" bigint NOT NULL,
  "numeric_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "decimal_table" (
  "id" bigint NOT NULL,
  "decimal_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "decimal_to_float64_table" (
  "id" bigint NOT NULL,
  "decimal_to_float64_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "decimal_to_str_table" (
  "id" bigint NOT NULL,
  "decimal_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "decimal_to_int64_table" (
  "id" bigint NOT NULL,
  "decimal_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "dec_table" (
  "id" bigint NOT NULL,
  "dec_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "dec_to_float64_table" (
  "id" bigint NOT NULL,
  "dec_to_float64_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "dec_to_str_table" (
  "id" bigint NOT NULL,
  "dec_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "dec_to_int64_table" (
  "id" bigint NOT NULL,
  "dec_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "float_table" (
  "id" bigint NOT NULL,
  "float_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "float_to_float64_table" (
  "id" bigint NOT NULL,
  "float_to_float64_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "float_to_str_table" (
  "id" bigint NOT NULL,
  "float_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "float_to_int64_table" (
  "id" bigint NOT NULL,
  "float_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "double_prec_table" (
  "id" bigint NOT NULL,
  "double_prec_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "double_prec_to_numeric_table" (
  "id" bigint NOT NULL,
  "double_prec_to_numeric_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "double_prec_to_str_table" (
  "id" bigint NOT NULL,
  "double_prec_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "double_prec_to_int64_table" (
  "id" bigint NOT NULL,
  "double_prec_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "real_table" (
  "id" bigint NOT NULL,
  "real_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "real_to_numeric_table" (
  "id" bigint NOT NULL,
  "real_to_numeric_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "real_to_str_table" (
  "id" bigint NOT NULL,
  "real_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "real_to_int64_table" (
  "id" bigint NOT NULL,
  "real_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "binary_float_table" (
  "id" bigint NOT NULL,
  "binary_float_col" float4,
  PRIMARY KEY ("id")
);

CREATE TABLE "binary_float_to_float64_table" (
  "id" bigint NOT NULL,
  "binary_float_to_float64_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "binary_float_to_str_table" (
  "id" bigint NOT NULL,
  "binary_float_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "binary_float_to_numeric_table" (
  "id" bigint NOT NULL,
  "binary_float_to_numeric_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "binary_float_to_int64_table" (
  "id" bigint NOT NULL,
  "binary_float_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "binary_double_table" (
  "id" bigint NOT NULL,
  "binary_double_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "binary_double_to_str_table" (
  "id" bigint NOT NULL,
  "binary_double_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "binary_double_to_numeric_table" (
  "id" bigint NOT NULL,
  "binary_double_to_numeric_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "binary_double_to_int64_table" (
  "id" bigint NOT NULL,
  "binary_double_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "integer_table" (
  "id" bigint NOT NULL,
  "integer_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "integer_to_numeric_table" (
  "id" bigint NOT NULL,
  "integer_to_numeric_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "integer_to_str_table" (
  "id" bigint NOT NULL,
  "integer_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "integer_to_float64_table" (
  "id" bigint NOT NULL,
  "integer_to_float64_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "integer_pk_table" (
  "integer_pk_col" bigint NOT NULL,
  PRIMARY KEY ("integer_pk_col")
);

CREATE TABLE "int_table" (
  "id" bigint NOT NULL,
  "int_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "int_to_numeric_table" (
  "id" bigint NOT NULL,
  "int_to_numeric_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "int_to_str_table" (
  "id" bigint NOT NULL,
  "int_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "int_to_float64_table" (
  "id" bigint NOT NULL,
  "int_to_float64_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "int_pk_table" (
  "int_pk_col" bigint NOT NULL,
  PRIMARY KEY ("int_pk_col")
);

CREATE TABLE "smallint_table" (
  "id" bigint NOT NULL,
  "smallint_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "smallint_to_numeric_table" (
  "id" bigint NOT NULL,
  "smallint_to_numeric_col" numeric,
  PRIMARY KEY ("id")
);

CREATE TABLE "smallint_to_str_table" (
  "id" bigint NOT NULL,
  "smallint_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "smallint_to_float64_table" (
  "id" bigint NOT NULL,
  "smallint_to_float64_col" float8,
  PRIMARY KEY ("id")
);

CREATE TABLE "smallint_pk_table" (
  "smallint_pk_col" bigint NOT NULL,
  PRIMARY KEY ("smallint_pk_col")
);

CREATE TABLE "date_table" (
  "id" bigint NOT NULL,
  "date_col" timestamptz,
  PRIMARY KEY ("id")
);

CREATE TABLE "date_to_date_table" (
  "id" bigint NOT NULL,
  "date_to_date_col" date,
  PRIMARY KEY ("id")
);

CREATE TABLE "date_to_str_table" (
  "id" bigint NOT NULL,
  "date_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "date_to_int64_table" (
  "id" bigint NOT NULL,
  "date_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "date_pk_table" (
  "date_pk_col" timestamptz NOT NULL,
  PRIMARY KEY ("date_pk_col")
);

CREATE TABLE "timestamp_table" (
  "id" bigint NOT NULL,
  "timestamp_col" timestamptz,
  PRIMARY KEY ("id")
);

CREATE TABLE "timestamp_to_str_table" (
  "id" bigint NOT NULL,
  "timestamp_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "timestamp_to_int64_table" (
  "id" bigint NOT NULL,
  "timestamp_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "timestamp_pk_table" (
  "timestamp_pk_col" timestamptz NOT NULL,
  PRIMARY KEY ("timestamp_pk_col")
);

CREATE TABLE "timestamp_tz_table" (
  "id" bigint NOT NULL,
  "timestamp_tz_col" timestamptz,
  PRIMARY KEY ("id")
);

CREATE TABLE "timestamp_tz_to_str_table" (
  "id" bigint NOT NULL,
  "timestamp_tz_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "timestamp_tz_to_int64_table" (
  "id" bigint NOT NULL,
  "timestamp_tz_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "timestamp_ltz_table" (
  "id" bigint NOT NULL,
  "timestamp_ltz_col" timestamptz,
  PRIMARY KEY ("id")
);

CREATE TABLE "timestamp_ltz_to_str_table" (
  "id" bigint NOT NULL,
  "timestamp_ltz_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "timestamp_ltz_to_int64_table" (
  "id" bigint NOT NULL,
  "timestamp_ltz_to_int64_col" bigint,
  PRIMARY KEY ("id")
);

CREATE TABLE "timestamp_ltz_pk_table" (
  "timestamp_ltz_pk_col" timestamptz NOT NULL,
  PRIMARY KEY ("timestamp_ltz_pk_col")
);

CREATE TABLE "raw_table" (
  "id" bigint NOT NULL,
  "raw_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "raw_to_bytes2000_table" (
  "id" bigint NOT NULL,
  "raw_to_bytes2000_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "raw_to_str_table" (
  "id" bigint NOT NULL,
  "raw_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "raw_pk_table" (
  "raw_pk_col" bytea NOT NULL,
  PRIMARY KEY ("raw_pk_col")
);

CREATE TABLE "blob_table" (
  "id" bigint NOT NULL,
  "blob_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "blob_to_str_table" (
  "id" bigint NOT NULL,
  "blob_to_str_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "clob_table" (
  "id" bigint NOT NULL,
  "clob_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "clob_to_bytes_table" (
  "id" bigint NOT NULL,
  "clob_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "nclob_table" (
  "id" bigint NOT NULL,
  "nclob_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "nclob_to_bytes_table" (
  "id" bigint NOT NULL,
  "nclob_to_bytes_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "interval_year_to_month_table" (
  "id" bigint NOT NULL,
  "interval_year_to_month_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "interval_day_to_second_table" (
  "id" bigint NOT NULL,
  "interval_day_to_second_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "long_raw_table" (
  "id" bigint NOT NULL,
  "long_raw_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "bfile_table" (
  "id" bigint NOT NULL,
  "bfile_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "long_table" (
  "id" bigint NOT NULL,
  "long_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "rowid_table" (
  "id" bigint NOT NULL,
  "rowid_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "urowid_table" (
  "id" bigint NOT NULL,
  "urowid_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "xmltype_table" (
  "id" bigint NOT NULL,
  "xmltype_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "anydata_table" (
  "id" bigint NOT NULL,
  "anydata_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "anytype_table" (
  "id" bigint NOT NULL,
  "anytype_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "anydataset_table" (
  "id" bigint NOT NULL,
  "anydataset_col" bytea,
  PRIMARY KEY ("id")
);

CREATE TABLE "uritype_table" (
  "id" bigint NOT NULL,
  "uritype_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "dburitype_table" (
  "id" bigint NOT NULL,
  "dburitype_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "xdburitype_table" (
  "id" bigint NOT NULL,
  "xdburitype_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "httpuritype_table" (
  "id" bigint NOT NULL,
  "httpuritype_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "mlslabel_table" (
  "id" bigint NOT NULL,
  "mlslabel_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "ref_table" (
  "id" bigint NOT NULL,
  "ref_col" varchar,
  PRIMARY KEY ("id")
);

CREATE TABLE "varray_table" (
  "id" bigint NOT NULL,
  "varray_col" varchar[],
  PRIMARY KEY ("id")
);

CREATE TABLE "nested_table_table" (
  "id" bigint NOT NULL,
  "nested_table_col" jsonb,
  PRIMARY KEY ("id")
);

CREATE TABLE "object_type_table" (
  "id" bigint NOT NULL,
  "object_type_col" jsonb,
  PRIMARY KEY ("id")
);
