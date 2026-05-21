CREATE TABLE bool_table (id BIGINT PRIMARY KEY, bool_col boolean);
CREATE TABLE int64_table (id BIGINT PRIMARY KEY, int64_col bigint);
CREATE TABLE float64_table (id BIGINT PRIMARY KEY, float64_col double precision);
CREATE TABLE string_table (id BIGINT PRIMARY KEY, string_col text);
CREATE TABLE bytes_table (id BIGINT PRIMARY KEY, bytes_col bytea);
CREATE TABLE date_table (id BIGINT PRIMARY KEY, date_col date);
CREATE TABLE numeric_table (id BIGINT PRIMARY KEY, numeric_col numeric);
CREATE TABLE timestamp_table (id BIGINT PRIMARY KEY, timestamp_col timestamp with time zone);
CREATE TABLE json_table (id BIGINT PRIMARY KEY, json_col jsonb);