CREATE TABLE IF NOT EXISTS tinyint_table (
  id bigint NOT NULL,
  tinyint_col bigint,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS tinyint_to_string_table (
  id bigint NOT NULL,
  tinyint_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS tinyint_pk_table (
  id bigint NOT NULL,
  tinyint_pk_col bigint NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS smallint_table (
  id bigint NOT NULL,
  smallint_col bigint,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS smallint_to_string_table (
  id bigint NOT NULL,
  smallint_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS smallint_pk_table (
  id bigint NOT NULL,
  smallint_pk_col bigint NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS int_table (
  id bigint NOT NULL,
  int_col bigint,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS int_to_string_table (
  id bigint NOT NULL,
  int_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS int_pk_table (
  id bigint NOT NULL,
  int_pk_col bigint NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS bigint_table (
  id bigint NOT NULL,
  bigint_col bigint,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS bigint_to_string_table (
  id bigint NOT NULL,
  bigint_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS bigint_pk_table (
  id bigint NOT NULL,
  bigint_pk_col bigint NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS bit_table (
  id bigint NOT NULL,
  bit_col boolean,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS bit_to_int64_table (
  id bigint NOT NULL,
  bit_to_int64_col bigint,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS bit_to_string_table (
  id bigint NOT NULL,
  bit_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS bit_pk_table (
  id boolean NOT NULL,
  bit_pk_col boolean NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS decimal_table (
  id bigint NOT NULL,
  decimal_col numeric,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS decimal_to_float64_table (
  id bigint NOT NULL,
  decimal_to_float64_col double precision,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS decimal_to_string_table (
  id bigint NOT NULL,
  decimal_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS numeric_table (
  id bigint NOT NULL,
  numeric_col numeric,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS numeric_to_float64_table (
  id bigint NOT NULL,
  numeric_to_float64_col double precision,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS numeric_to_string_table (
  id bigint NOT NULL,
  numeric_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS numeric_pk_table (
  id varchar NOT NULL,
  numeric_pk_col numeric NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS money_table (
  id bigint NOT NULL,
  money_col numeric,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS money_to_float64_table (
  id bigint NOT NULL,
  money_to_float64_col double precision,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS money_to_string_table (
  id bigint NOT NULL,
  money_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS smallmoney_table (
  id bigint NOT NULL,
  smallmoney_col numeric,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS smallmoney_to_float64_table (
  id bigint NOT NULL,
  smallmoney_to_float64_col double precision,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS smallmoney_to_string_table (
  id bigint NOT NULL,
  smallmoney_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS float_table (
  id bigint NOT NULL,
  float_col double precision,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS float_to_string_table (
  id bigint NOT NULL,
  float_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS real_table (
  id bigint NOT NULL,
  real_col real,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS real_to_float64_table (
  id bigint NOT NULL,
  real_to_float64_col double precision,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS real_to_string_table (
  id bigint NOT NULL,
  real_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS date_table (
  id bigint NOT NULL,
  date_col date,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS date_to_string_table (
  id bigint NOT NULL,
  date_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS date_pk_table (
  id date NOT NULL,
  date_pk_col date NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS time_table (
  id bigint NOT NULL,
  time_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS time_to_bytes_table (
  id bigint NOT NULL,
  time_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS time_pk_table (
  id varchar NOT NULL,
  time_pk_col varchar NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datetime2_table (
  id bigint NOT NULL,
  datetime2_col timestamp with time zone,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datetime2_to_string_table (
  id bigint NOT NULL,
  datetime2_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datetime2_pk_table (
  id timestamp with time zone NOT NULL,
  datetime2_pk_col timestamp with time zone NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datetimeoffset_table (
  id bigint NOT NULL,
  datetimeoffset_col timestamp with time zone,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datetimeoffset_to_string_table (
  id bigint NOT NULL,
  datetimeoffset_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datetimeoffset_pk_table (
  id timestamp with time zone NOT NULL,
  datetimeoffset_pk_col timestamp with time zone NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datetime_table (
  id bigint NOT NULL,
  datetime_col timestamp with time zone,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datetime_to_string_table (
  id bigint NOT NULL,
  datetime_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS datetime_pk_table (
  id timestamp with time zone NOT NULL,
  datetime_pk_col timestamp with time zone NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS smalldatetime_table (
  id bigint NOT NULL,
  smalldatetime_col timestamp with time zone,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS smalldatetime_to_string_table (
  id bigint NOT NULL,
  smalldatetime_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS smalldatetime_pk_table (
  id timestamp with time zone NOT NULL,
  smalldatetime_pk_col timestamp with time zone NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS char_table (
  id bigint NOT NULL,
  char_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS char_to_bytes_table (
  id bigint NOT NULL,
  char_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS char_pk_table (
  id varchar NOT NULL,
  char_pk_col varchar NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS varchar_table (
  id bigint NOT NULL,
  varchar_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS varchar_to_bytes_table (
  id bigint NOT NULL,
  varchar_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS varchar_pk_table (
  id varchar NOT NULL,
  varchar_pk_col varchar NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS text_table (
  id bigint NOT NULL,
  text_col text,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS text_to_bytes_table (
  id bigint NOT NULL,
  text_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS nchar_table (
  id bigint NOT NULL,
  nchar_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS nchar_to_bytes_table (
  id bigint NOT NULL,
  nchar_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS nchar_pk_table (
  id varchar NOT NULL,
  nchar_pk_col varchar NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS nvarchar_table (
  id bigint NOT NULL,
  nvarchar_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS nvarchar_to_bytes_table (
  id bigint NOT NULL,
  nvarchar_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS nvarchar_pk_table (
  id varchar NOT NULL,
  nvarchar_pk_col varchar NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS ntext_table (
  id bigint NOT NULL,
  ntext_col text,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS ntext_to_bytes_table (
  id bigint NOT NULL,
  ntext_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS binary_table (
  id bigint NOT NULL,
  binary_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS binary_to_string_table (
  id bigint NOT NULL,
  binary_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS binary_pk_table (
  id bytea NOT NULL,
  binary_pk_col bytea NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS varbinary_table (
  id bigint NOT NULL,
  varbinary_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS varbinary_to_string_table (
  id bigint NOT NULL,
  varbinary_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS varbinary_pk_table (
  id bytea NOT NULL,
  varbinary_pk_col bytea NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS image_table (
  id bigint NOT NULL,
  image_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS image_to_string_table (
  id bigint NOT NULL,
  image_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS uniqueidentifier_table (
  id bigint NOT NULL,
  uniqueidentifier_col uuid,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS uniqueidentifier_to_bytes_table (
  id bigint NOT NULL,
  uniqueidentifier_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS uniqueidentifier_to_string_table (
  id bigint NOT NULL,
  uniqueidentifier_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS uniqueidentifier_pk_table (
  id uuid NOT NULL,
  uniqueidentifier_pk_col uuid NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS xml_table (
  id bigint NOT NULL,
  xml_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS xml_to_bytes_table (
  id bigint NOT NULL,
  xml_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS rowversion_table (
  id bigint NOT NULL,
  rowversion_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS rowversion_to_bytes_table (
  id bigint NOT NULL,
  rowversion_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS rowversion_to_int64_table (
  id bigint NOT NULL,
  rowversion_to_int64_col bigint,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS timestamp_table (
  id bigint NOT NULL,
  timestamp_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS timestamp_to_bytes_table (
  id bigint NOT NULL,
  timestamp_to_bytes_col bytea,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS timestamp_to_int64_table (
  id bigint NOT NULL,
  timestamp_to_int64_col bigint,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS json_table (
  id bigint NOT NULL,
  json_col jsonb,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS json_to_string_table (
  id bigint NOT NULL,
  json_to_string_col varchar,
  PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS vector_table (
  id bigint NOT NULL,
  vector_col float8[],
  PRIMARY KEY(id)
);



