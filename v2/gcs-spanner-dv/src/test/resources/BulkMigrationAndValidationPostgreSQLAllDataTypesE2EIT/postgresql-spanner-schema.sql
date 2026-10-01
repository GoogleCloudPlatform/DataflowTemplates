CREATE TABLE IF NOT EXISTS aclitem_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bigint_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bigint_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bigserial_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bigserial_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bit_to_bytes (
    id INT64,
    col BYTES(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bool_to_bool (
    id INT64,
    col BOOL
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bool_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS boolean_to_bool (
    id INT64,
    col BOOL
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS boolean_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS box_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS bytea_to_bytes (
    id INT64,
    col BYTES(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS char_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS character_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS character_varying_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS cid_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS cidr_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS circle_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS citext_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS date_to_date (
    id INT64,
    col DATE
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS date_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS datemultirange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS daterange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS decimal_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS double_precision_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS double_precision_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS float_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS float_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS float4_to_float32 (
    id INT64,
    col FLOAT32
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS float4_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS float4_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS float8_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS float8_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS hstore_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS inet_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int2_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int2_to_float32 (
    id INT64,
    col FLOAT32
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int2_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int2_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int4_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int4_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int4_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int4multirange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int4range_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int8_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int8_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int8multirange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS int8range_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS integer_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS integer_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS integer_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS interval_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS json_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS jsonb_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS line_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS lseg_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS ltree_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS macaddr_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS macaddr8_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS money_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS numeric_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS nummultirange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS numrange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS oid_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS oid_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS oid_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS path_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS pg_lsn_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS pg_snapshot_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS point_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS polygon_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS real_to_float32 (
    id INT64,
    col FLOAT32
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS real_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS real_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS regclass_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS regconfig_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS regdictionary_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS regnamespace_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS regproc_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS regrole_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS regtype_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial2_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial2_to_float32 (
    id INT64,
    col FLOAT32
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial2_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial2_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial4_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial4_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial4_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial8_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS serial8_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS smallint_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS smallint_to_float32 (
    id INT64,
    col FLOAT32
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS smallint_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS smallint_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS smallserial_to_int64 (
    id INT64,
    col INT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS smallserial_to_float32 (
    id INT64,
    col FLOAT32
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS smallserial_to_float64 (
    id INT64,
    col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS smallserial_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS text_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS tid_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS time__with_time_zone_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS time__without_time_zone_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS timestamp__with_time_zone_to_timestamp (
    id INT64,
    col TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS timestamp__with_time_zone_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS timestamp__without_time_zone_to_timestamp (
    id INT64,
    col TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS timestamp__without_time_zone_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS timestamptz_to_timestamp (
    id INT64,
    col TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS timestamptz_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS timetz_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS tsmultirange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS tsquery_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS tsrange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS tstzmultirange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS tstzrange_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS tsvector_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS txid_snapshot_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS uuid_to_uuid (
    id INT64,
    col UUID
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS uuid_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS varbit_to_bytes (
    id INT64,
    col BYTES(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS varchar_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS xid_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS xid8_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS xml_to_string (
    id INT64,
    col STRING(MAX)
) PRIMARY KEY (id);

