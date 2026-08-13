/*
  NOTE: Boundary values injected in this schema strictly respect the target Spanner datatype limits.
  If a PostgreSQL datatype's max/min bounds exceed the mapped Spanner datatype's capacity,
  the injected value is clamped to the Spanner limit to avoid pipeline migration crashes.
*/

CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS ltree;

DROP TABLE IF EXISTS aclitem_to_string;
CREATE TABLE aclitem_to_string (
    id serial primary key,
    col aclitem
);
INSERT INTO aclitem_to_string (col) VALUES
    ('postgres=arwdDxt/postgres'), ('postgres=arwdDxt/postgres'), ('postgres=arwdDxt/postgres'), (NULL);

DROP TABLE IF EXISTS bigint_to_int64;
CREATE TABLE bigint_to_int64 (
    id serial primary key,
    col bigint
);
INSERT INTO bigint_to_int64 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS bigint_to_string;
CREATE TABLE bigint_to_string (
    id serial primary key,
    col bigint
);
INSERT INTO bigint_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS bigserial_to_int64;
CREATE TABLE bigserial_to_int64 (
    id serial primary key,
    col bigserial
);
INSERT INTO bigserial_to_int64 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (42);

DROP TABLE IF EXISTS bigserial_to_string;
CREATE TABLE bigserial_to_string (
    id serial primary key,
    col bigserial
);
INSERT INTO bigserial_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (42);

DROP TABLE IF EXISTS bit_to_bytes;
CREATE TABLE bit_to_bytes (
    id serial primary key,
    col bit
);
INSERT INTO bit_to_bytes (col) VALUES
    (B'1'), (B'1'), (B'1'), (NULL);

DROP TABLE IF EXISTS bool_to_bool;
CREATE TABLE bool_to_bool (
    id serial primary key,
    col bool
);
INSERT INTO bool_to_bool (col) VALUES
    (TRUE), (FALSE), (NULL), (NULL);

DROP TABLE IF EXISTS bool_to_string;
CREATE TABLE bool_to_string (
    id serial primary key,
    col bool
);
INSERT INTO bool_to_string (col) VALUES
    (TRUE), (FALSE), (NULL), (NULL);

DROP TABLE IF EXISTS boolean_to_bool;
CREATE TABLE boolean_to_bool (
    id serial primary key,
    col boolean
);
INSERT INTO boolean_to_bool (col) VALUES
    (TRUE), (FALSE), (NULL), (NULL);

DROP TABLE IF EXISTS boolean_to_string;
CREATE TABLE boolean_to_string (
    id serial primary key,
    col boolean
);
INSERT INTO boolean_to_string (col) VALUES
    (TRUE), (FALSE), (NULL), (NULL);

DROP TABLE IF EXISTS box_to_string;
CREATE TABLE box_to_string (
    id serial primary key,
    col box
);
INSERT INTO box_to_string (col) VALUES
    ('((0,0),(1,1))'), ('((0,0),(1,1))'), ('((0,0),(1,1))'), (NULL);

DROP TABLE IF EXISTS bytea_to_bytes;
CREATE TABLE bytea_to_bytes (
    id serial primary key,
    col bytea
);
INSERT INTO bytea_to_bytes (col) VALUES
    (NULL), (NULL), (NULL), (NULL);

DROP TABLE IF EXISTS char_to_string;
CREATE TABLE char_to_string (
    id serial primary key,
    col char
);
INSERT INTO char_to_string (col) VALUES
    (''), ('M'), (NULL /* Spanner does not support NULL byte */), (NULL);

DROP TABLE IF EXISTS character_to_string;
CREATE TABLE character_to_string (
    id serial primary key,
    col character
);
INSERT INTO character_to_string (col) VALUES
    (''), ('M'), (NULL /* Spanner does not support NULL byte */), (NULL);

DROP TABLE IF EXISTS character_varying_to_string;
CREATE TABLE character_varying_to_string (
    id serial primary key,
    col character varying
);
INSERT INTO character_varying_to_string (col) VALUES
    (''), ('MAX_LEN'), (NULL /* Spanner does not support NULL byte */), (NULL);

DROP TABLE IF EXISTS cid_to_string;
CREATE TABLE cid_to_string (
    id serial primary key,
    col cid
);
INSERT INTO cid_to_string (col) VALUES
    ('1'), ('1'), ('1'), (NULL);

DROP TABLE IF EXISTS cidr_to_string;
CREATE TABLE cidr_to_string (
    id serial primary key,
    col cidr
);
INSERT INTO cidr_to_string (col) VALUES
    ('192.168.1.1'), ('10.0.0.1'), ('10.0.0.1'), (NULL);

DROP TABLE IF EXISTS circle_to_string;
CREATE TABLE circle_to_string (
    id serial primary key,
    col circle
);
INSERT INTO circle_to_string (col) VALUES
    ('<(0,0),1>'), ('<(0,0),1>'), ('<(0,0),1>'), (NULL);

DROP TABLE IF EXISTS citext_to_string;
CREATE TABLE citext_to_string (
    id serial primary key,
    col citext
);
INSERT INTO citext_to_string (col) VALUES
    (''), ('MAX_LEN'), (NULL /* Spanner does not support NULL byte */), (NULL);

DROP TABLE IF EXISTS date_to_date;
CREATE TABLE date_to_date (
    id serial primary key,
    col date
);
INSERT INTO date_to_date (col) VALUES
    ('1970-01-01'), ('9999-12-31'), ('1970-01-01'), (NULL);

DROP TABLE IF EXISTS date_to_string;
CREATE TABLE date_to_string (
    id serial primary key,
    col date
);
INSERT INTO date_to_string (col) VALUES
    ('1970-01-01'), ('9999-12-31'), ('1970-01-01'), (NULL);

DROP TABLE IF EXISTS datemultirange_to_string;
CREATE TABLE datemultirange_to_string (
    id serial primary key,
    col datemultirange
);
INSERT INTO datemultirange_to_string (col) VALUES
    ('{[2020-01-01, 2020-01-05]}'), ('{[2020-01-01, 2020-01-05]}'), ('{[2020-01-01, 2020-01-05]}'), (NULL);

DROP TABLE IF EXISTS daterange_to_string;
CREATE TABLE daterange_to_string (
    id serial primary key,
    col daterange
);
INSERT INTO daterange_to_string (col) VALUES
    ('[2020-01-01, 2020-01-05]'), ('[2020-01-01, 2020-01-05]'), ('[2020-01-01, 2020-01-05]'), (NULL);

DROP TABLE IF EXISTS decimal_to_string;
CREATE TABLE decimal_to_string (
    id serial primary key,
    col decimal
);
INSERT INTO decimal_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS double_precision_to_float64;
CREATE TABLE double_precision_to_float64 (
    id serial primary key,
    col double precision
);
INSERT INTO double_precision_to_float64 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS double_precision_to_string;
CREATE TABLE double_precision_to_string (
    id serial primary key,
    col double precision
);
INSERT INTO double_precision_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS float_to_float64;
CREATE TABLE float_to_float64 (
    id serial primary key,
    col float
);
INSERT INTO float_to_float64 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS float_to_string;
CREATE TABLE float_to_string (
    id serial primary key,
    col float
);
INSERT INTO float_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS float4_to_float32;
CREATE TABLE float4_to_float32 (
    id serial primary key,
    col float4
);
INSERT INTO float4_to_float32 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS float4_to_float64;
CREATE TABLE float4_to_float64 (
    id serial primary key,
    col float4
);
INSERT INTO float4_to_float64 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS float4_to_string;
CREATE TABLE float4_to_string (
    id serial primary key,
    col float4
);
INSERT INTO float4_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS float8_to_float64;
CREATE TABLE float8_to_float64 (
    id serial primary key,
    col float8
);
INSERT INTO float8_to_float64 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS float8_to_string;
CREATE TABLE float8_to_string (
    id serial primary key,
    col float8
);
INSERT INTO float8_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS hstore_to_string;
CREATE TABLE hstore_to_string (
    id serial primary key,
    col hstore
);
INSERT INTO hstore_to_string (col) VALUES
    ('"a"=>"1"'), ('"a"=>"1"'), ('"a"=>"1"'), (NULL);

DROP TABLE IF EXISTS inet_to_string;
CREATE TABLE inet_to_string (
    id serial primary key,
    col inet
);
INSERT INTO inet_to_string (col) VALUES
    ('192.168.1.1'), ('10.0.0.1'), ('10.0.0.1'), (NULL);

DROP TABLE IF EXISTS int_to_int64;
CREATE TABLE int_to_int64 (
    id serial primary key,
    col int
);
INSERT INTO int_to_int64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (NULL);

DROP TABLE IF EXISTS int_to_float64;
CREATE TABLE int_to_float64 (
    id serial primary key,
    col int
);
INSERT INTO int_to_float64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (NULL);

DROP TABLE IF EXISTS int_to_string;
CREATE TABLE int_to_string (
    id serial primary key,
    col int
);
INSERT INTO int_to_string (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (NULL);

DROP TABLE IF EXISTS int2_to_int64;
CREATE TABLE int2_to_int64 (
    id serial primary key,
    col int2
);
INSERT INTO int2_to_int64 (col) VALUES
    ('-32768'), ('32767'), ('0'), (NULL);

DROP TABLE IF EXISTS int2_to_float32;
CREATE TABLE int2_to_float32 (
    id serial primary key,
    col int2
);
INSERT INTO int2_to_float32 (col) VALUES
    ('-32768'), ('32767'), ('0'), (NULL);

DROP TABLE IF EXISTS int2_to_float64;
CREATE TABLE int2_to_float64 (
    id serial primary key,
    col int2
);
INSERT INTO int2_to_float64 (col) VALUES
    ('-32768'), ('32767'), ('0'), (NULL);

DROP TABLE IF EXISTS int2_to_string;
CREATE TABLE int2_to_string (
    id serial primary key,
    col int2
);
INSERT INTO int2_to_string (col) VALUES
    ('-32768'), ('32767'), ('0'), (NULL);

DROP TABLE IF EXISTS int4_to_int64;
CREATE TABLE int4_to_int64 (
    id serial primary key,
    col int4
);
INSERT INTO int4_to_int64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (NULL);

DROP TABLE IF EXISTS int4_to_float64;
CREATE TABLE int4_to_float64 (
    id serial primary key,
    col int4
);
INSERT INTO int4_to_float64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (NULL);

DROP TABLE IF EXISTS int4_to_string;
CREATE TABLE int4_to_string (
    id serial primary key,
    col int4
);
INSERT INTO int4_to_string (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (NULL);

DROP TABLE IF EXISTS int4multirange_to_string;
CREATE TABLE int4multirange_to_string (
    id serial primary key,
    col int4multirange
);
INSERT INTO int4multirange_to_string (col) VALUES
    ('{[1,10]}'), ('{[1,10]}'), ('{[1,10]}'), (NULL);

DROP TABLE IF EXISTS int4range_to_string;
CREATE TABLE int4range_to_string (
    id serial primary key,
    col int4range
);
INSERT INTO int4range_to_string (col) VALUES
    ('[1,10]'), ('[1,10]'), ('[1,10]'), (NULL);

DROP TABLE IF EXISTS int8_to_int64;
CREATE TABLE int8_to_int64 (
    id serial primary key,
    col int8
);
INSERT INTO int8_to_int64 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS int8_to_string;
CREATE TABLE int8_to_string (
    id serial primary key,
    col int8
);
INSERT INTO int8_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS int8multirange_to_string;
CREATE TABLE int8multirange_to_string (
    id serial primary key,
    col int8multirange
);
INSERT INTO int8multirange_to_string (col) VALUES
    ('{[1,10]}'), ('{[1,10]}'), ('{[1,10]}'), (NULL);

DROP TABLE IF EXISTS int8range_to_string;
CREATE TABLE int8range_to_string (
    id serial primary key,
    col int8range
);
INSERT INTO int8range_to_string (col) VALUES
    ('[1,10]'), ('[1,10]'), ('[1,10]'), (NULL);

DROP TABLE IF EXISTS integer_to_int64;
CREATE TABLE integer_to_int64 (
    id serial primary key,
    col integer
);
INSERT INTO integer_to_int64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (NULL);

DROP TABLE IF EXISTS integer_to_float64;
CREATE TABLE integer_to_float64 (
    id serial primary key,
    col integer
);
INSERT INTO integer_to_float64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (NULL);

DROP TABLE IF EXISTS integer_to_string;
CREATE TABLE integer_to_string (
    id serial primary key,
    col integer
);
INSERT INTO integer_to_string (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (NULL);

DROP TABLE IF EXISTS interval_to_string;
CREATE TABLE interval_to_string (
    id serial primary key,
    col interval
);
INSERT INTO interval_to_string (col) VALUES
    ('1 day'), ('1 day'), ('1 day'), (NULL);

DROP TABLE IF EXISTS json_to_string;
CREATE TABLE json_to_string (
    id serial primary key,
    col json
);
INSERT INTO json_to_string (col) VALUES
    ('{}'), ('{"k": "v"}'), ('[]'), (NULL);

DROP TABLE IF EXISTS jsonb_to_string;
CREATE TABLE jsonb_to_string (
    id serial primary key,
    col jsonb
);
INSERT INTO jsonb_to_string (col) VALUES
    ('{}'), ('{"k": "v"}'), ('[]'), (NULL);

DROP TABLE IF EXISTS line_to_string;
CREATE TABLE line_to_string (
    id serial primary key,
    col line
);
INSERT INTO line_to_string (col) VALUES
    ('{1,-1,0}'), ('{1,-1,0}'), ('{1,-1,0}'), (NULL);

DROP TABLE IF EXISTS lseg_to_string;
CREATE TABLE lseg_to_string (
    id serial primary key,
    col lseg
);
INSERT INTO lseg_to_string (col) VALUES
    ('[(0,0),(1,1)]'), ('[(0,0),(1,1)]'), ('[(0,0),(1,1)]'), (NULL);

DROP TABLE IF EXISTS ltree_to_string;
CREATE TABLE ltree_to_string (
    id serial primary key,
    col ltree
);
INSERT INTO ltree_to_string (col) VALUES
    ('A.B.C'), ('A.B.C'), ('A.B.C'), (NULL);

DROP TABLE IF EXISTS macaddr_to_string;
CREATE TABLE macaddr_to_string (
    id serial primary key,
    col macaddr
);
INSERT INTO macaddr_to_string (col) VALUES
    ('08:00:2b:01:02:03'), ('08:00:2b:01:02:03'), ('08:00:2b:01:02:03'), (NULL);

DROP TABLE IF EXISTS macaddr8_to_string;
CREATE TABLE macaddr8_to_string (
    id serial primary key,
    col macaddr8
);
INSERT INTO macaddr8_to_string (col) VALUES
    ('08:00:2b:01:02:03'), ('08:00:2b:01:02:03'), ('08:00:2b:01:02:03'), (NULL);

DROP TABLE IF EXISTS money_to_string;
CREATE TABLE money_to_string (
    id serial primary key,
    col money
);
INSERT INTO money_to_string (col) VALUES
    ('-92233720368547758.08'), ('92233720368547758.07'), ('0'), (NULL);

DROP TABLE IF EXISTS numeric_to_string;
CREATE TABLE numeric_to_string (
    id serial primary key,
    col numeric
);
INSERT INTO numeric_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS nummultirange_to_string;
CREATE TABLE nummultirange_to_string (
    id serial primary key,
    col nummultirange
);
INSERT INTO nummultirange_to_string (col) VALUES
    ('{[1.5,5.5]}'), ('{[1.5,5.5]}'), ('{[1.5,5.5]}'), (NULL);

DROP TABLE IF EXISTS numrange_to_string;
CREATE TABLE numrange_to_string (
    id serial primary key,
    col numrange
);
INSERT INTO numrange_to_string (col) VALUES
    ('[1.5,5.5]'), ('[1.5,5.5]'), ('[1.5,5.5]'), (NULL);

DROP TABLE IF EXISTS oid_to_int64;
CREATE TABLE oid_to_int64 (
    id serial primary key,
    col oid
);
INSERT INTO oid_to_int64 (col) VALUES
    ('0'), ('4294967295'), ('0'), (NULL);

DROP TABLE IF EXISTS oid_to_float64;
CREATE TABLE oid_to_float64 (
    id serial primary key,
    col oid
);
INSERT INTO oid_to_float64 (col) VALUES
    ('0'), ('4294967295'), ('0'), (NULL);

DROP TABLE IF EXISTS oid_to_string;
CREATE TABLE oid_to_string (
    id serial primary key,
    col oid
);
INSERT INTO oid_to_string (col) VALUES
    ('0'), ('4294967295'), ('0'), (NULL);

DROP TABLE IF EXISTS path_to_string;
CREATE TABLE path_to_string (
    id serial primary key,
    col path
);
INSERT INTO path_to_string (col) VALUES
    ('[(0,0),(1,1)]'), ('[(0,0),(1,1)]'), ('[(0,0),(1,1)]'), (NULL);

DROP TABLE IF EXISTS pg_lsn_to_string;
CREATE TABLE pg_lsn_to_string (
    id serial primary key,
    col pg_lsn
);
INSERT INTO pg_lsn_to_string (col) VALUES
    ('16/B374D848'), ('16/B374D848'), ('16/B374D848'), (NULL);

DROP TABLE IF EXISTS pg_snapshot_to_string;
CREATE TABLE pg_snapshot_to_string (
    id serial primary key,
    col pg_snapshot
);
INSERT INTO pg_snapshot_to_string (col) VALUES
    ('10:20:10'), ('10:20:10'), ('10:20:10'), (NULL);

DROP TABLE IF EXISTS point_to_string;
CREATE TABLE point_to_string (
    id serial primary key,
    col point
);
INSERT INTO point_to_string (col) VALUES
    ('(0,0)'), ('(0,0)'), ('(0,0)'), (NULL);

DROP TABLE IF EXISTS polygon_to_string;
CREATE TABLE polygon_to_string (
    id serial primary key,
    col polygon
);
INSERT INTO polygon_to_string (col) VALUES
    ('((0,0),(1,1),(2,0))'), ('((0,0),(1,1),(2,0))'), ('((0,0),(1,1),(2,0))'), (NULL);

DROP TABLE IF EXISTS real_to_float32;
CREATE TABLE real_to_float32 (
    id serial primary key,
    col real
);
INSERT INTO real_to_float32 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS real_to_float64;
CREATE TABLE real_to_float64 (
    id serial primary key,
    col real
);
INSERT INTO real_to_float64 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS real_to_string;
CREATE TABLE real_to_string (
    id serial primary key,
    col real
);
INSERT INTO real_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (NULL);

DROP TABLE IF EXISTS regclass_to_string;
CREATE TABLE regclass_to_string (
    id serial primary key,
    col regclass
);
INSERT INTO regclass_to_string (col) VALUES
    ('pg_class'), ('pg_class'), ('pg_class'), (NULL);

DROP TABLE IF EXISTS regconfig_to_string;
CREATE TABLE regconfig_to_string (
    id serial primary key,
    col regconfig
);
INSERT INTO regconfig_to_string (col) VALUES
    ('english'), ('english'), ('english'), (NULL);

DROP TABLE IF EXISTS regdictionary_to_string;
CREATE TABLE regdictionary_to_string (
    id serial primary key,
    col regdictionary
);
INSERT INTO regdictionary_to_string (col) VALUES
    ('simple'), ('simple'), ('simple'), (NULL);

DROP TABLE IF EXISTS regnamespace_to_string;
CREATE TABLE regnamespace_to_string (
    id serial primary key,
    col regnamespace
);
INSERT INTO regnamespace_to_string (col) VALUES
    ('public'), ('public'), ('public'), (NULL);

DROP TABLE IF EXISTS regproc_to_string;
CREATE TABLE regproc_to_string (
    id serial primary key,
    col regproc
);
INSERT INTO regproc_to_string (col) VALUES
    ('pg_backend_pid'), ('pg_backend_pid'), ('pg_backend_pid'), (NULL);

DROP TABLE IF EXISTS regrole_to_string;
CREATE TABLE regrole_to_string (
    id serial primary key,
    col regrole
);
INSERT INTO regrole_to_string (col) VALUES
    ('postgres'), ('postgres'), ('postgres'), (NULL);

DROP TABLE IF EXISTS regtype_to_string;
CREATE TABLE regtype_to_string (
    id serial primary key,
    col regtype
);
INSERT INTO regtype_to_string (col) VALUES
    ('int4'), ('int4'), ('int4'), (NULL);

DROP TABLE IF EXISTS serial_to_int64;
CREATE TABLE serial_to_int64 (
    id serial primary key,
    col serial
);
INSERT INTO serial_to_int64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (42);

DROP TABLE IF EXISTS serial_to_float64;
CREATE TABLE serial_to_float64 (
    id serial primary key,
    col serial
);
INSERT INTO serial_to_float64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (42);

DROP TABLE IF EXISTS serial_to_string;
CREATE TABLE serial_to_string (
    id serial primary key,
    col serial
);
INSERT INTO serial_to_string (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (42);

DROP TABLE IF EXISTS serial2_to_int64;
CREATE TABLE serial2_to_int64 (
    id serial primary key,
    col serial2
);
INSERT INTO serial2_to_int64 (col) VALUES
    ('-32768'), ('32767'), ('0'), (42);

DROP TABLE IF EXISTS serial2_to_float32;
CREATE TABLE serial2_to_float32 (
    id serial primary key,
    col serial2
);
INSERT INTO serial2_to_float32 (col) VALUES
    ('-32768'), ('32767'), ('0'), (42);

DROP TABLE IF EXISTS serial2_to_float64;
CREATE TABLE serial2_to_float64 (
    id serial primary key,
    col serial2
);
INSERT INTO serial2_to_float64 (col) VALUES
    ('-32768'), ('32767'), ('0'), (42);

DROP TABLE IF EXISTS serial2_to_string;
CREATE TABLE serial2_to_string (
    id serial primary key,
    col serial2
);
INSERT INTO serial2_to_string (col) VALUES
    ('-32768'), ('32767'), ('0'), (42);

DROP TABLE IF EXISTS serial4_to_int64;
CREATE TABLE serial4_to_int64 (
    id serial primary key,
    col serial4
);
INSERT INTO serial4_to_int64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (42);

DROP TABLE IF EXISTS serial4_to_float64;
CREATE TABLE serial4_to_float64 (
    id serial primary key,
    col serial4
);
INSERT INTO serial4_to_float64 (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (42);

DROP TABLE IF EXISTS serial4_to_string;
CREATE TABLE serial4_to_string (
    id serial primary key,
    col serial4
);
INSERT INTO serial4_to_string (col) VALUES
    ('-2147483648'), ('2147483647'), ('0'), (42);

DROP TABLE IF EXISTS serial8_to_int64;
CREATE TABLE serial8_to_int64 (
    id serial primary key,
    col serial8
);
INSERT INTO serial8_to_int64 (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (42);

DROP TABLE IF EXISTS serial8_to_string;
CREATE TABLE serial8_to_string (
    id serial primary key,
    col serial8
);
INSERT INTO serial8_to_string (col) VALUES
    ('-9223372036854775808'), ('9223372036854775807'), ('0'), (42);

DROP TABLE IF EXISTS smallint_to_int64;
CREATE TABLE smallint_to_int64 (
    id serial primary key,
    col smallint
);
INSERT INTO smallint_to_int64 (col) VALUES
    ('-32768'), ('32767'), ('0'), (NULL);

DROP TABLE IF EXISTS smallint_to_float32;
CREATE TABLE smallint_to_float32 (
    id serial primary key,
    col smallint
);
INSERT INTO smallint_to_float32 (col) VALUES
    ('-32768'), ('32767'), ('0'), (NULL);

DROP TABLE IF EXISTS smallint_to_float64;
CREATE TABLE smallint_to_float64 (
    id serial primary key,
    col smallint
);
INSERT INTO smallint_to_float64 (col) VALUES
    ('-32768'), ('32767'), ('0'), (NULL);

DROP TABLE IF EXISTS smallint_to_string;
CREATE TABLE smallint_to_string (
    id serial primary key,
    col smallint
);
INSERT INTO smallint_to_string (col) VALUES
    ('-32768'), ('32767'), ('0'), (NULL);

DROP TABLE IF EXISTS smallserial_to_int64;
CREATE TABLE smallserial_to_int64 (
    id serial primary key,
    col smallserial
);
INSERT INTO smallserial_to_int64 (col) VALUES
    ('-32768'), ('32767'), ('0'), (42);

DROP TABLE IF EXISTS smallserial_to_float32;
CREATE TABLE smallserial_to_float32 (
    id serial primary key,
    col smallserial
);
INSERT INTO smallserial_to_float32 (col) VALUES
    ('-32768'), ('32767'), ('0'), (42);

DROP TABLE IF EXISTS smallserial_to_float64;
CREATE TABLE smallserial_to_float64 (
    id serial primary key,
    col smallserial
);
INSERT INTO smallserial_to_float64 (col) VALUES
    ('-32768'), ('32767'), ('0'), (42);

DROP TABLE IF EXISTS smallserial_to_string;
CREATE TABLE smallserial_to_string (
    id serial primary key,
    col smallserial
);
INSERT INTO smallserial_to_string (col) VALUES
    ('-32768'), ('32767'), ('0'), (42);

DROP TABLE IF EXISTS text_to_string;
CREATE TABLE text_to_string (
    id serial primary key,
    col text
);
INSERT INTO text_to_string (col) VALUES
    (''), ('MAX_LEN'), (NULL /* Spanner does not support NULL byte */), (NULL);

DROP TABLE IF EXISTS tid_to_string;
CREATE TABLE tid_to_string (
    id serial primary key,
    col tid
);
INSERT INTO tid_to_string (col) VALUES
    ('(0,1)'), ('(0,1)'), ('(0,1)'), (NULL);

DROP TABLE IF EXISTS time__with_time_zone_to_string;
CREATE TABLE time__with_time_zone_to_string (
    id serial primary key,
    col time  with time zone
);
INSERT INTO time__with_time_zone_to_string (col) VALUES
    ('00:00:00'), ('00:00:00'), ('00:00:00'), (NULL);

DROP TABLE IF EXISTS time__without_time_zone_to_string;
CREATE TABLE time__without_time_zone_to_string (
    id serial primary key,
    col time  without time zone
);
INSERT INTO time__without_time_zone_to_string (col) VALUES
    ('00:00:00'), ('00:00:00'), ('00:00:00'), (NULL);

DROP TABLE IF EXISTS timestamp__with_time_zone_to_timestamp;
CREATE TABLE timestamp__with_time_zone_to_timestamp (
    id serial primary key,
    col timestamp  with time zone
);
INSERT INTO timestamp__with_time_zone_to_timestamp (col) VALUES
    ('1970-01-01 00:00:00'), ('9999-12-31 00:00:00'), ('1970-01-01 00:00:00'), (NULL);

DROP TABLE IF EXISTS timestamp__with_time_zone_to_string;
CREATE TABLE timestamp__with_time_zone_to_string (
    id serial primary key,
    col timestamp  with time zone
);
INSERT INTO timestamp__with_time_zone_to_string (col) VALUES
    ('1970-01-01 00:00:00'), ('9999-12-31 00:00:00'), ('1970-01-01 00:00:00'), (NULL);

DROP TABLE IF EXISTS timestamp__without_time_zone_to_timestamp;
CREATE TABLE timestamp__without_time_zone_to_timestamp (
    id serial primary key,
    col timestamp  without time zone
);
INSERT INTO timestamp__without_time_zone_to_timestamp (col) VALUES
    ('1970-01-01 00:00:00'), ('9999-12-31 00:00:00'), ('1970-01-01 00:00:00'), (NULL);

DROP TABLE IF EXISTS timestamp__without_time_zone_to_string;
CREATE TABLE timestamp__without_time_zone_to_string (
    id serial primary key,
    col timestamp  without time zone
);
INSERT INTO timestamp__without_time_zone_to_string (col) VALUES
    ('1970-01-01 00:00:00'), ('9999-12-31 00:00:00'), ('1970-01-01 00:00:00'), (NULL);

DROP TABLE IF EXISTS timestamptz_to_timestamp;
CREATE TABLE timestamptz_to_timestamp (
    id serial primary key,
    col timestamptz
);
INSERT INTO timestamptz_to_timestamp (col) VALUES
    ('1970-01-01 00:00:00'), ('9999-12-31 00:00:00'), ('1970-01-01 00:00:00'), (NULL);

DROP TABLE IF EXISTS timestamptz_to_string;
CREATE TABLE timestamptz_to_string (
    id serial primary key,
    col timestamptz
);
INSERT INTO timestamptz_to_string (col) VALUES
    ('1970-01-01 00:00:00'), ('9999-12-31 00:00:00'), ('1970-01-01 00:00:00'), (NULL);

DROP TABLE IF EXISTS timetz_to_string;
CREATE TABLE timetz_to_string (
    id serial primary key,
    col timetz
);
INSERT INTO timetz_to_string (col) VALUES
    ('00:00:00'), ('00:00:00'), ('00:00:00'), (NULL);

DROP TABLE IF EXISTS tsmultirange_to_string;
CREATE TABLE tsmultirange_to_string (
    id serial primary key,
    col tsmultirange
);
INSERT INTO tsmultirange_to_string (col) VALUES
    ('{[2020-01-01 00:00:00, 2020-01-05 00:00:00]}'), ('{[2020-01-01 00:00:00, 2020-01-05 00:00:00]}'), ('{[2020-01-01 00:00:00, 2020-01-05 00:00:00]}'), (NULL);

DROP TABLE IF EXISTS tsquery_to_string;
CREATE TABLE tsquery_to_string (
    id serial primary key,
    col tsquery
);
INSERT INTO tsquery_to_string (col) VALUES
    ('fat & rat'), ('fat & rat'), ('fat & rat'), (NULL);

DROP TABLE IF EXISTS tsrange_to_string;
CREATE TABLE tsrange_to_string (
    id serial primary key,
    col tsrange
);
INSERT INTO tsrange_to_string (col) VALUES
    ('[2020-01-01 00:00:00, 2020-01-05 00:00:00]'), ('[2020-01-01 00:00:00, 2020-01-05 00:00:00]'), ('[2020-01-01 00:00:00, 2020-01-05 00:00:00]'), (NULL);

DROP TABLE IF EXISTS tstzmultirange_to_string;
CREATE TABLE tstzmultirange_to_string (
    id serial primary key,
    col tstzmultirange
);
INSERT INTO tstzmultirange_to_string (col) VALUES
    ('{[2020-01-01 00:00:00+00, 2020-01-05 00:00:00+00]}'), ('{[2020-01-01 00:00:00+00, 2020-01-05 00:00:00+00]}'), ('{[2020-01-01 00:00:00+00, 2020-01-05 00:00:00+00]}'), (NULL);

DROP TABLE IF EXISTS tstzrange_to_string;
CREATE TABLE tstzrange_to_string (
    id serial primary key,
    col tstzrange
);
INSERT INTO tstzrange_to_string (col) VALUES
    ('[2020-01-01 00:00:00+00, 2020-01-05 00:00:00+00]'), ('[2020-01-01 00:00:00+00, 2020-01-05 00:00:00+00]'), ('[2020-01-01 00:00:00+00, 2020-01-05 00:00:00+00]'), (NULL);

DROP TABLE IF EXISTS tsvector_to_string;
CREATE TABLE tsvector_to_string (
    id serial primary key,
    col tsvector
);
INSERT INTO tsvector_to_string (col) VALUES
    ('a fat cat sat on a mat'), ('a fat cat sat on a mat'), ('a fat cat sat on a mat'), (NULL);

DROP TABLE IF EXISTS txid_snapshot_to_string;
CREATE TABLE txid_snapshot_to_string (
    id serial primary key,
    col txid_snapshot
);
INSERT INTO txid_snapshot_to_string (col) VALUES
    ('10:20:10'), ('10:20:10'), ('10:20:10'), (NULL);

DROP TABLE IF EXISTS uuid_to_uuid;
CREATE TABLE uuid_to_uuid (
    id serial primary key,
    col uuid
);
INSERT INTO uuid_to_uuid (col) VALUES
    ('00000000-0000-0000-0000-000000000000'), ('ffffffff-ffff-ffff-ffff-ffffffffffff'), ('00000000-0000-0000-0000-000000000000'), (NULL);

DROP TABLE IF EXISTS uuid_to_string;
CREATE TABLE uuid_to_string (
    id serial primary key,
    col uuid
);
INSERT INTO uuid_to_string (col) VALUES
    ('00000000-0000-0000-0000-000000000000'), ('ffffffff-ffff-ffff-ffff-ffffffffffff'), ('00000000-0000-0000-0000-000000000000'), (NULL);

DROP TABLE IF EXISTS varbit_to_bytes;
CREATE TABLE varbit_to_bytes (
    id serial primary key,
    col varbit
);
INSERT INTO varbit_to_bytes (col) VALUES
    (B'1010'), (B'1010'), (B'1010'), (NULL);

DROP TABLE IF EXISTS varchar_to_string;
CREATE TABLE varchar_to_string (
    id serial primary key,
    col varchar
);
INSERT INTO varchar_to_string (col) VALUES
    (''), ('MAX_LEN'), (NULL /* Spanner does not support NULL byte */), (NULL);

DROP TABLE IF EXISTS xid_to_string;
CREATE TABLE xid_to_string (
    id serial primary key,
    col xid
);
INSERT INTO xid_to_string (col) VALUES
    ('1'), ('1'), ('1'), (NULL);

DROP TABLE IF EXISTS xid8_to_string;
CREATE TABLE xid8_to_string (
    id serial primary key,
    col xid8
);
INSERT INTO xid8_to_string (col) VALUES
    ('1'), ('1'), ('1'), (NULL);

DROP TABLE IF EXISTS xml_to_string;
CREATE TABLE xml_to_string (
    id serial primary key,
    col xml
);
INSERT INTO xml_to_string (col) VALUES
    ('<foo>bar</foo>'), ('<foo>bar</foo>'), ('<foo>bar</foo>'), (NULL);

