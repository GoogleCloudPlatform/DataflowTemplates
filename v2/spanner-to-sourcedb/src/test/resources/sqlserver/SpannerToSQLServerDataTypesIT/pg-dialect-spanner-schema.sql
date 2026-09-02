CREATE TABLE IF NOT EXISTS tinyint_table (id bigint NOT NULL PRIMARY KEY, tinyint_col bigint);
CREATE TABLE IF NOT EXISTS smallint_table (id bigint NOT NULL PRIMARY KEY, smallint_col bigint);
CREATE TABLE IF NOT EXISTS int_table (id bigint NOT NULL PRIMARY KEY, int_col bigint);
CREATE TABLE IF NOT EXISTS bigint_table (id bigint NOT NULL PRIMARY KEY, bigint_col bigint);
CREATE TABLE IF NOT EXISTS bit_table (id bigint NOT NULL PRIMARY KEY, bit_col boolean);
CREATE TABLE IF NOT EXISTS decimal_table (id bigint NOT NULL PRIMARY KEY, decimal_col numeric);
CREATE TABLE IF NOT EXISTS numeric_table (id bigint NOT NULL PRIMARY KEY, numeric_col numeric);
CREATE TABLE IF NOT EXISTS money_table (id bigint NOT NULL PRIMARY KEY, money_col numeric);
CREATE TABLE IF NOT EXISTS smallmoney_table (id bigint NOT NULL PRIMARY KEY, smallmoney_col numeric);
CREATE TABLE IF NOT EXISTS float_table (id bigint NOT NULL PRIMARY KEY, float_col double precision);
CREATE TABLE IF NOT EXISTS real_table (id bigint NOT NULL PRIMARY KEY, real_col real);
CREATE TABLE IF NOT EXISTS date_table (id bigint NOT NULL PRIMARY KEY, date_col date);
CREATE TABLE IF NOT EXISTS time_table (id bigint NOT NULL PRIMARY KEY, time_col varchar);
CREATE TABLE IF NOT EXISTS datetime_table (id bigint NOT NULL PRIMARY KEY, datetime_col timestamp with time zone);
CREATE TABLE IF NOT EXISTS datetime2_table (id bigint NOT NULL PRIMARY KEY, datetime2_col timestamp with time zone);
CREATE TABLE IF NOT EXISTS smalldatetime_table (id bigint NOT NULL PRIMARY KEY, smalldatetime_col timestamp with time zone);
CREATE TABLE IF NOT EXISTS datetimeoffset_table (id bigint NOT NULL PRIMARY KEY, datetimeoffset_col timestamp with time zone);
CREATE TABLE IF NOT EXISTS char_table (id bigint NOT NULL PRIMARY KEY, char_col varchar(20));
CREATE TABLE IF NOT EXISTS varchar_table (id bigint NOT NULL PRIMARY KEY, varchar_col varchar(200));
CREATE TABLE IF NOT EXISTS text_table (id bigint NOT NULL PRIMARY KEY, text_col text);
CREATE TABLE IF NOT EXISTS nchar_table (id bigint NOT NULL PRIMARY KEY, nchar_col varchar(20));
CREATE TABLE IF NOT EXISTS nvarchar_table (id bigint NOT NULL PRIMARY KEY, nvarchar_col varchar(200));
CREATE TABLE IF NOT EXISTS ntext_table (id bigint NOT NULL PRIMARY KEY, ntext_col text);
CREATE TABLE IF NOT EXISTS binary_table (id bigint NOT NULL PRIMARY KEY, binary_col bytea);
CREATE TABLE IF NOT EXISTS varbinary_table (id bigint NOT NULL PRIMARY KEY, varbinary_col bytea);
CREATE TABLE IF NOT EXISTS image_table (id bigint NOT NULL PRIMARY KEY, image_col bytea);
CREATE TABLE IF NOT EXISTS uniqueidentifier_table (id bigint NOT NULL PRIMARY KEY, uniqueidentifier_col varchar(36));
CREATE TABLE IF NOT EXISTS xml_table (id bigint NOT NULL PRIMARY KEY, xml_col text);

CREATE TABLE IF NOT EXISTS tinyint_to_string_table (id bigint NOT NULL PRIMARY KEY, tinyint_to_string_col varchar);
CREATE TABLE IF NOT EXISTS smallint_to_string_table (id bigint NOT NULL PRIMARY KEY, smallint_to_string_col varchar);
CREATE TABLE IF NOT EXISTS int_to_string_table (id bigint NOT NULL PRIMARY KEY, int_to_string_col varchar);
CREATE TABLE IF NOT EXISTS bigint_to_string_table (id bigint NOT NULL PRIMARY KEY, bigint_to_string_col varchar);
CREATE TABLE IF NOT EXISTS bit_to_int64_table (id bigint NOT NULL PRIMARY KEY, bit_to_int64_col bigint);
CREATE TABLE IF NOT EXISTS bit_to_string_table (id bigint NOT NULL PRIMARY KEY, bit_to_string_col varchar);
CREATE TABLE IF NOT EXISTS decimal_to_float64_table (id bigint NOT NULL PRIMARY KEY, decimal_to_float64_col double precision);
CREATE TABLE IF NOT EXISTS decimal_to_string_table (id bigint NOT NULL PRIMARY KEY, decimal_to_string_col varchar);
CREATE TABLE IF NOT EXISTS numeric_to_float64_table (id bigint NOT NULL PRIMARY KEY, numeric_to_float64_col double precision);
CREATE TABLE IF NOT EXISTS numeric_to_string_table (id bigint NOT NULL PRIMARY KEY, numeric_to_string_col varchar);
CREATE TABLE IF NOT EXISTS money_to_float64_table (id bigint NOT NULL PRIMARY KEY, money_to_float64_col double precision);
CREATE TABLE IF NOT EXISTS money_to_string_table (id bigint NOT NULL PRIMARY KEY, money_to_string_col varchar);
CREATE TABLE IF NOT EXISTS smallmoney_to_float64_table (id bigint NOT NULL PRIMARY KEY, smallmoney_to_float64_col double precision);
CREATE TABLE IF NOT EXISTS smallmoney_to_string_table (id bigint NOT NULL PRIMARY KEY, smallmoney_to_string_col varchar);
CREATE TABLE IF NOT EXISTS float_to_string_table (id bigint NOT NULL PRIMARY KEY, float_to_string_col varchar);
CREATE TABLE IF NOT EXISTS real_to_float64_table (id bigint NOT NULL PRIMARY KEY, real_to_float64_col double precision);
CREATE TABLE IF NOT EXISTS real_to_string_table (id bigint NOT NULL PRIMARY KEY, real_to_string_col varchar);
CREATE TABLE IF NOT EXISTS date_to_string_table (id bigint NOT NULL PRIMARY KEY, date_to_string_col varchar);
CREATE TABLE IF NOT EXISTS datetime_to_string_table (id bigint NOT NULL PRIMARY KEY, datetime_to_string_col varchar);
CREATE TABLE IF NOT EXISTS datetime2_to_string_table (id bigint NOT NULL PRIMARY KEY, datetime2_to_string_col varchar);
CREATE TABLE IF NOT EXISTS smalldatetime_to_string_table (id bigint NOT NULL PRIMARY KEY, smalldatetime_to_string_col varchar);
CREATE TABLE IF NOT EXISTS char_to_bytes_table (id bigint NOT NULL PRIMARY KEY, char_to_bytes_col bytea);
CREATE TABLE IF NOT EXISTS varchar_to_bytes_table (id bigint NOT NULL PRIMARY KEY, varchar_to_bytes_col bytea);
CREATE TABLE IF NOT EXISTS nchar_to_bytes_table (id bigint NOT NULL PRIMARY KEY, nchar_to_bytes_col bytea);
CREATE TABLE IF NOT EXISTS nvarchar_to_bytes_table (id bigint NOT NULL PRIMARY KEY, nvarchar_to_bytes_col bytea);
CREATE TABLE IF NOT EXISTS binary_to_string_table (id bigint NOT NULL PRIMARY KEY, binary_to_string_col varchar);
CREATE TABLE IF NOT EXISTS varbinary_to_string_table (id bigint NOT NULL PRIMARY KEY, varbinary_to_string_col varchar);
CREATE TABLE IF NOT EXISTS image_to_string_table (id bigint NOT NULL PRIMARY KEY, image_to_string_col varchar);
CREATE TABLE IF NOT EXISTS uniqueidentifier_to_bytes_table (id bigint NOT NULL PRIMARY KEY, uniqueidentifier_to_bytes_col bytea);
CREATE TABLE IF NOT EXISTS xml_to_bytes_table (id bigint NOT NULL PRIMARY KEY, xml_to_bytes_col bytea);
CREATE TABLE IF NOT EXISTS json_to_varchar_table (id bigint NOT NULL PRIMARY KEY, json_to_varchar_col jsonb);
CREATE TABLE IF NOT EXISTS array_to_varchar_table (id bigint NOT NULL PRIMARY KEY, array_to_varchar_col float8[]);

CREATE CHANGE STREAM allstream
  FOR ALL WITH (
  value_capture_type = 'NEW_ROW',
  retention_period = '7d'
);
