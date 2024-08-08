CREATE TABLE bigint_table (
    id SERIAL PRIMARY KEY,
    bigint_col BIGINT
);

CREATE TABLE bigserial_table (
    id SERIAL PRIMARY KEY,
    bigserial_col BIGSERIAL
);

CREATE TABLE bit_table (
    id SERIAL PRIMARY KEY,
    bit_col BIT
);

CREATE TABLE bit_varying_table (
    id SERIAL PRIMARY KEY,
    bit_varying_col BIT VARYING
);

CREATE TABLE bool_table (
    id SERIAL PRIMARY KEY,
    bool_col BOOL
);

CREATE TABLE boolean_table (
    id SERIAL PRIMARY KEY,
    boolean_col BOOLEAN
);

CREATE TABLE box_table (
    id SERIAL PRIMARY KEY,
    box_col BOX
);

CREATE TABLE bytea_table (
    id SERIAL PRIMARY KEY,
    bytea_col BYTEA
);

CREATE TABLE char_table (
    id SERIAL PRIMARY KEY,
    char_col CHAR
);

CREATE TABLE character_table (
    id SERIAL PRIMARY KEY,
    character_col CHARACTER
);

CREATE TABLE character_varying_table (
    id SERIAL PRIMARY KEY,
    character_varying_col CHARACTER VARYING
);

CREATE TABLE cidr_table (
    id SERIAL PRIMARY KEY,
    cidr_col CIDR
);

CREATE TABLE circle_table (
    id SERIAL PRIMARY KEY,
    circle_col CIRCLE
);

CREATE TABLE date_table (
    id SERIAL PRIMARY KEY,
    date_col DATE
);

CREATE TABLE decimal_table (
    id SERIAL PRIMARY KEY,
    decimal_col DECIMAL
);

CREATE TABLE double_precision_table (
    id SERIAL PRIMARY KEY,
    double_precision_col DOUBLE PRECISION
);

CREATE TABLE float4_table (
    id SERIAL PRIMARY KEY,
    float4_col FLOAT4
);

CREATE TABLE float8_table (
    id SERIAL PRIMARY KEY,
    float8_col FLOAT8
);

CREATE TABLE inet_table (
    id SERIAL PRIMARY KEY,
    inet_col INET
);

CREATE TABLE integer_table (
    id SERIAL PRIMARY KEY,
    integer_col INTEGER
);

CREATE TABLE int2_table (
    id SERIAL PRIMARY KEY,
    int2_col INT2
);

CREATE TABLE int4_table (
    id SERIAL PRIMARY KEY,
    int4_col INT4
);

CREATE TABLE int8_table (
    id SERIAL PRIMARY KEY,
    int8_col INT8
);

CREATE TABLE interval_table (
    id SERIAL PRIMARY KEY,
    interval_col INTERVAL
);

CREATE TABLE json_table (
    id SERIAL PRIMARY KEY,
    json_col JSON
);

CREATE TABLE jsonb_table (
    id SERIAL PRIMARY KEY,
    jsonb_col JSONB
);

CREATE TABLE line_table (
    id SERIAL PRIMARY KEY,
    line_col LINE
);

CREATE TABLE lseg_table (
    id SERIAL PRIMARY KEY,
    lseg_col LSEG
);

CREATE TABLE macaddr_table (
    id SERIAL PRIMARY KEY,
    macaddr_col MACADDR
);

CREATE TABLE macaddr8_table (
    id SERIAL PRIMARY KEY,
    macaddr8_col MACADDR8
);

CREATE TABLE money_table (
    id SERIAL PRIMARY KEY,
    money_col MONEY
);

CREATE TABLE numeric_table (
    id SERIAL PRIMARY KEY,
    numeric_col NUMERIC
);

CREATE TABLE path_table (
    id SERIAL PRIMARY KEY,
    path_col PATH
);

CREATE TABLE pg_lsn_table (
    id SERIAL PRIMARY KEY,
    pg_lsn_col PG_LSN
);

CREATE TABLE pg_snapshot_table (
    id SERIAL PRIMARY KEY,
    pg_snapshot_col PG_SNAPSHOT
);

CREATE TABLE point_table (
    id SERIAL PRIMARY KEY,
    point_col POINT
);

CREATE TABLE polygon_table (
    id SERIAL PRIMARY KEY,
    polygon_col POLYGON
);

CREATE TABLE real_table (
    id SERIAL PRIMARY KEY,
    real_col REAL
);

CREATE TABLE serial2_table (
    id SERIAL PRIMARY KEY,
    serial2_col SERIAL2
);

CREATE TABLE serial4_table (
    id SERIAL PRIMARY KEY,
    serial4_col SERIAL4
);

CREATE TABLE serial8_table (
    id SERIAL PRIMARY KEY,
    serial8_col SERIAL8
);

CREATE TABLE smallint_table (
    id SERIAL PRIMARY KEY,
    smallint_col SMALLINT
);

CREATE TABLE smallserial_table (
    id SERIAL PRIMARY KEY,
    smallserial_col SMALLSERIAL
);

CREATE TABLE text_table (
    id SERIAL PRIMARY KEY,
    text_col TEXT
);

CREATE TABLE time_table (
    id SERIAL PRIMARY KEY,
    time_col TIME
);

CREATE TABLE time_without_time_zone_table (
    id SERIAL PRIMARY KEY,
    time_without_time_zone_col TIME WITHOUT TIME ZONE
);

CREATE TABLE timestamp_table (
    id SERIAL PRIMARY KEY,
    timestamp_col TIMESTAMP
);

CREATE TABLE timestamp_without_time_zone_table (
    id SERIAL PRIMARY KEY,
    timestamp_without_time_zone_col TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE timestamptz_table (
    id SERIAL PRIMARY KEY,
    timestamptz_col TIMESTAMPTZ
);

CREATE TABLE timestamp_with_time_zone_table (
    id SERIAL PRIMARY KEY,
    timestamp_with_time_zone_col TIMESTAMP WITH TIME ZONE
);

CREATE TABLE timetz_table (
    id SERIAL PRIMARY KEY,
    timetz_col TIMETZ
);

CREATE TABLE time_with_time_zone_table (
    id SERIAL PRIMARY KEY,
    time_with_time_zone_col TIME WITH TIME ZONE
);

CREATE TABLE tsquery_table (
    id SERIAL PRIMARY KEY,
    tsquery_col TSQUERY
);

CREATE TABLE tsvector_table (
    id SERIAL PRIMARY KEY,
    tsvector_col TSVECTOR
);

CREATE TABLE txid_snapshot_table (
    id SERIAL PRIMARY KEY,
    txid_snapshot_col TXID_SNAPSHOT
);

CREATE TABLE uuid_table (
    id SERIAL PRIMARY KEY,
    uuid_col UUID
);

CREATE TABLE varbit_table (
    id SERIAL PRIMARY KEY,
    varbit_col VARBIT
);

CREATE TABLE varchar_table (
    id SERIAL PRIMARY KEY,
    varchar_col VARCHAR
);

CREATE TABLE xml_table (
    id SERIAL PRIMARY KEY,
    xml_col XML
);

INSERT INTO bigint_table (bigint_col) VALUES(9223372036854775807);
INSERT INTO bigint_table (bigint_col) VALUES(-9223372036854775808);
INSERT INTO bigint_table (bigint_col) VALUES(NULL);

INSERT INTO bigserial_table (bigserial_col) VALUES(9223372036854775807);
INSERT INTO bigserial_table (bigserial_col) VALUES(-9223372036854775808);

INSERT INTO bit_table (bit_col) VALUES(0::bit);
INSERT INTO bit_table (bit_col) VALUES(1::bit);
INSERT INTO bit_table (bit_col) VALUES(NULL);

INSERT INTO bit_varying_table (bit_varying_col) VALUES(B'101');
INSERT INTO bit_varying_table (bit_varying_col) VALUES(NULL);

INSERT INTO bool_table (bool_col) VALUES(false);
INSERT INTO bool_table (bool_col) VALUES(true);
INSERT INTO bool_table (bool_col) VALUES(NULL);

INSERT INTO boolean_table (boolean_col) VALUES(false);
INSERT INTO boolean_table (boolean_col) VALUES(true);
INSERT INTO boolean_table (boolean_col) VALUES(NULL);

INSERT INTO box_table (box_col) VALUES('((999999999999999, 1000000000000001), (100000000000000123, 123))');
INSERT INTO box_table (box_col) VALUES(NULL);

INSERT INTO bytea_table (bytea_col) VALUES('abc'::bytea);
INSERT INTO bytea_table (bytea_col) VALUES(NULL);

INSERT INTO char_table (char_col) VALUES('Ã');
INSERT INTO char_table (char_col) VALUES('¶');
INSERT INTO char_table (char_col) VALUES(NULL);

INSERT INTO character_table (character_col) VALUES('Ã');
INSERT INTO character_table (character_col) VALUES('¶');
INSERT INTO character_table (character_col) VALUES(NULL);

INSERT INTO character_varying_table (character_varying_col) VALUES('character varying');
INSERT INTO character_varying_table (character_varying_col) VALUES(NULL);

INSERT INTO cidr_table (cidr_col) VALUES('192.168.100.128/25');
INSERT INTO cidr_table (cidr_col) VALUES('192.168.1');
INSERT INTO cidr_table (cidr_col) VALUES('192');
INSERT INTO cidr_table (cidr_col) VALUES('::ffff:1.2.3.0/128');
INSERT INTO cidr_table (cidr_col) VALUES(NULL);

INSERT INTO circle_table (circle_col) VALUES('((999999999999999, 1000000000000001), 100000000000000123)');
INSERT INTO circle_table (circle_col) VALUES(NULL);

INSERT INTO date_table (date_col) VALUES('4713-01-01 BC'::date);
INSERT INTO date_table (date_col) VALUES('5874897-12-31'::date);
INSERT INTO date_table (date_col) VALUES('0001-01-01 BC'::date);
INSERT INTO date_table (date_col) VALUES('10000-01-01'::date);
INSERT INTO date_table (date_col) VALUES('0001-01-01'::date);
INSERT INTO date_table (date_col) VALUES('9999-12-31'::date);
INSERT INTO date_table (date_col) VALUES(NULL);

INSERT INTO decimal_table (decimal_col) VALUES('Infinity');
INSERT INTO decimal_table (decimal_col) VALUES('-Infinity');
INSERT INTO decimal_table (decimal_col) VALUES('NaN');
INSERT INTO decimal_table (decimal_col) VALUES('1.23');
INSERT INTO decimal_table (decimal_col) VALUES(NULL);

INSERT INTO double_precision_table (double_precision_col) VALUES('1.9876542e307');
INSERT INTO double_precision_table (double_precision_col) VALUES('-1.9876542e307');
INSERT INTO double_precision_table (double_precision_col) VALUES('NaN');
INSERT INTO double_precision_table (double_precision_col) VALUES('Infinity');
INSERT INTO double_precision_table (double_precision_col) VALUES('-Infinity');
INSERT INTO double_precision_table (double_precision_col) VALUES(NULL);

INSERT INTO float4_table (float4_col) VALUES('1.9876542e38');
INSERT INTO float4_table (float4_col) VALUES('-1.9876542e38');
INSERT INTO float4_table (float4_col) VALUES('NaN');
INSERT INTO float4_table (float4_col) VALUES('Infinity');
INSERT INTO float4_table (float4_col) VALUES('-Infinity');
INSERT INTO float4_table (float4_col) VALUES(NULL);

INSERT INTO float8_table (float8_col) VALUES('1.9876542e307');
INSERT INTO float8_table (float8_col) VALUES('-1.9876542e307');
INSERT INTO float8_table (float8_col) VALUES('NaN');
INSERT INTO float8_table (float8_col) VALUES('Infinity');
INSERT INTO float8_table (float8_col) VALUES('-Infinity');
INSERT INTO float8_table (float8_col) VALUES(NULL);

INSERT INTO inet_table (inet_col) VALUES('192.168.1.0/24');
INSERT INTO inet_table (inet_col) VALUES(NULL);

INSERT INTO int2_table (int2_col) VALUES(32767);
INSERT INTO int2_table (int2_col) VALUES(-32768);
INSERT INTO int2_table (int2_col) VALUES(NULL);

INSERT INTO int4_table (int4_col) VALUES(2147483647);
INSERT INTO int4_table (int4_col) VALUES(-2147483648);
INSERT INTO int4_table (int4_col) VALUES(NULL);

INSERT INTO int8_table (int8_col) VALUES(9223372036854775807);
INSERT INTO int8_table (int8_col) VALUES(-9223372036854775808);
INSERT INTO int8_table (int8_col) VALUES(NULL);

INSERT INTO interval_table (interval_col) VALUES('1 day');
INSERT INTO interval_table (interval_col) VALUES('1 month');
INSERT INTO interval_table (interval_col) VALUES('1 year');
INSERT INTO interval_table (interval_col) VALUES(NULL);

INSERT INTO json_table (json_col) VALUES('{"duplicate_key": 1, "duplicate_key": 2}');
INSERT INTO json_table (json_col) VALUES('{"big_number": 1e9999999999}');
INSERT INTO json_table (json_col) VALUES('{"null_key": null}');
INSERT INTO json_table (json_col) VALUES('{"number": 1e4931}');
INSERT INTO json_table (json_col) VALUES(NULL);

INSERT INTO jsonb_table (jsonb_col) VALUES('{"duplicate_key": 1, "duplicate_key": 2}');
INSERT INTO jsonb_table (jsonb_col) VALUES('{"big_number": 1e99999}');
INSERT INTO jsonb_table (jsonb_col) VALUES('{"null_key": null}');
INSERT INTO jsonb_table (jsonb_col) VALUES('{"number": 1e4931}');
INSERT INTO jsonb_table (jsonb_col) VALUES(NULL);

INSERT INTO line_table (line_col) VALUES('{ 1, 2, 3 }');
INSERT INTO line_table (line_col) VALUES(NULL);

INSERT INTO lseg_table (lseg_col) VALUES('[ (1, 2), (3, 4) ]');
INSERT INTO lseg_table (lseg_col) VALUES(NULL);

INSERT INTO macaddr_table (macaddr_col) VALUES('08:00:2b:01:02:03');
INSERT INTO macaddr_table (macaddr_col) VALUES(NULL);

INSERT INTO macaddr8_table (macaddr8_col) VALUES('08:00:2b:01:02:03:04:05');
INSERT INTO macaddr8_table (macaddr8_col) VALUES(NULL);

INSERT INTO money_table (money_col) VALUES('123.45'::money);
INSERT INTO money_table (money_col) VALUES(NULL);

INSERT INTO numeric_table (numeric_col) VALUES('Infinity');
INSERT INTO numeric_table (numeric_col) VALUES('-Infinity');
INSERT INTO numeric_table (numeric_col) VALUES('NaN');
INSERT INTO numeric_table (numeric_col) VALUES('1.23');
INSERT INTO numeric_table (numeric_col) VALUES(NULL);

INSERT INTO path_table (path_col) VALUES('[ (1, 2), (3, 4), (5, 6) ]');
INSERT INTO path_table (path_col) VALUES(NULL);

INSERT INTO pg_lsn_table (pg_lsn_col) VALUES('123/0'::pg_lsn);
INSERT INTO pg_lsn_table (pg_lsn_col) VALUES(NULL);

INSERT INTO pg_snapshot_table (pg_snapshot_col) VALUES('10:20:10,14,15'::pg_snapshot);
INSERT INTO pg_snapshot_table (pg_snapshot_col) VALUES(NULL);

INSERT INTO point_table (point_col) VALUES('(1, 2)');
INSERT INTO point_table (point_col) VALUES(NULL);

INSERT INTO polygon_table (polygon_col) VALUES('( (1, 2), (3, 4) )');
INSERT INTO polygon_table (polygon_col) VALUES(NULL);

INSERT INTO serial2_table (serial2_col) VALUES(32767);
INSERT INTO serial2_table (serial2_col) VALUES(-32768);

INSERT INTO serial4_table (serial4_col) VALUES(2147483647);
INSERT INTO serial4_table (serial4_col) VALUES(-2147483648);

INSERT INTO serial8_table (serial8_col) VALUES(9223372036854775807);
INSERT INTO serial8_table (serial8_col) VALUES(-9223372036854775808);

INSERT INTO smallint_table (smallint_col) VALUES(32767);
INSERT INTO smallint_table (smallint_col) VALUES(-32768);
INSERT INTO smallint_table (smallint_col) VALUES(NULL);

INSERT INTO smallserial_table (smallserial_col) VALUES(32767);
INSERT INTO smallserial_table (smallserial_col) VALUES(-32768);

INSERT INTO text_table (text_col) VALUES('text');
INSERT INTO text_table (text_col) VALUES(NULL);

INSERT INTO time_table (time_col) VALUES('00:00:00'::time);
INSERT INTO time_table (time_col) VALUES('23:59:59'::time);
INSERT INTO time_table (time_col) VALUES('24:00:00'::time);
INSERT INTO time_table (time_col) VALUES(NULL);

INSERT INTO time_without_time_zone_table (time_without_time_zone_col) VALUES('00:00:00'::time);
INSERT INTO time_without_time_zone_table (time_without_time_zone_col) VALUES('23:59:59'::time);
INSERT INTO time_without_time_zone_table (time_without_time_zone_col) VALUES('24:00:00'::time);
INSERT INTO time_without_time_zone_table (time_without_time_zone_col) VALUES(NULL);

INSERT INTO timestamp_table (timestamp_col) VALUES('4713-01-01 00:00:00 BC'::timestamp);
INSERT INTO timestamp_table (timestamp_col) VALUES('294276-12-31 23:59:59'::timestamp);
INSERT INTO timestamp_table (timestamp_col) VALUES(NULL);

INSERT INTO timestamp_without_time_zone_table (timestamp_without_time_zone_col) VALUES('4713-01-01 00:00:00 BC'::timestamp);
INSERT INTO timestamp_without_time_zone_table (timestamp_without_time_zone_col) VALUES('294276-12-31 23:59:59'::timestamp);
INSERT INTO timestamp_without_time_zone_table (timestamp_without_time_zone_col) VALUES(NULL);

INSERT INTO timestamptz_table (timestamptz_col) VALUES('4713-01-01 00:00:00+1559 BC'::timestamptz);
INSERT INTO timestamptz_table (timestamptz_col) VALUES('294276-12-31 23:59:59'::timestamptz);
INSERT INTO timestamptz_table (timestamptz_col) VALUES(NULL);

INSERT INTO timestamp_with_time_zone_table (timestamp_with_time_zone_col) VALUES('4713-01-01 00:00:00+1559 BC'::timestamptz);
INSERT INTO timestamp_with_time_zone_table (timestamp_with_time_zone_col) VALUES('294276-12-31 23:59:59'::timestamptz);
INSERT INTO timestamp_with_time_zone_table (timestamp_with_time_zone_col) VALUES(NULL);

INSERT INTO timetz_table (timetz_col) VALUES('00:00:00+1559'::timetz);
INSERT INTO timetz_table (timetz_col) VALUES('24:00:00-1559'::timetz);
INSERT INTO timetz_table (timetz_col) VALUES(NULL);

INSERT INTO time_with_time_zone_table (time_with_time_zone_col) VALUES('00:00:00+1559'::timetz);
INSERT INTO time_with_time_zone_table (time_with_time_zone_col) VALUES('24:00:00-1559'::timetz);
INSERT INTO time_with_time_zone_table (time_with_time_zone_col) VALUES(NULL);

INSERT INTO tsquery_table (tsquery_col) VALUES('fat & rat'::tsquery);
INSERT INTO tsquery_table (tsquery_col) VALUES(NULL);

INSERT INTO tsvector_table (tsvector_col) VALUES('a fat cat sat on a mat and ate a fat rat'::tsvector);
INSERT INTO tsvector_table (tsvector_col) VALUES(NULL);

INSERT INTO txid_snapshot_table (txid_snapshot_col) VALUES('10:20:10,14,15'::txid_snapshot);
INSERT INTO txid_snapshot_table (txid_snapshot_col) VALUES(NULL);

INSERT INTO uuid_table (uuid_col) VALUES('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid);
INSERT INTO uuid_table (uuid_col) VALUES(NULL);

INSERT INTO varbit_table (varbit_col) VALUES(B'101');
INSERT INTO varbit_table (varbit_col) VALUES(NULL);

INSERT INTO varchar_table (varchar_col) VALUES('varchar');
INSERT INTO varchar_table (varchar_col) VALUES(NULL);

INSERT INTO xml_table (xml_col) VALUES('<test>123</test>'::xml);
INSERT INTO xml_table (xml_col) VALUES(NULL);
