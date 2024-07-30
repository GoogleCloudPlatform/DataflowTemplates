CREATE TABLE bigint_table (
    id INT64 NOT NULL,
    bigint_col INT64
) PRIMARY KEY (id);

CREATE TABLE bigserial_table (
    id INT64 NOT NULL,
    bigserial_col INT64
) PRIMARY KEY (id);

CREATE TABLE bit_table (
    id INT64 NOT NULL,
    bit_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE bit_varying_table (
    id INT64 NOT NULL,
    bit_varying_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE bool_table (
    id INT64 NOT NULL,
    bool_col BOOL
) PRIMARY KEY (id);

CREATE TABLE boolean_table (
    id INT64 NOT NULL,
    boolean_col BOOL
) PRIMARY KEY (id);

CREATE TABLE box_table (
    id INT64 NOT NULL,
    box_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE bytea_table (
    id INT64 NOT NULL,
    bytea_col BYTES(MAX)
) PRIMARY KEY (id);

CREATE TABLE char_table (
    id INT64 NOT NULL,
    char_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE character_table (
    id INT64 NOT NULL,
    character_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE character_varying_table (
    id INT64 NOT NULL,
    character_varying_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE cidr_table (
    id INT64 NOT NULL,
    cidr_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE circle_table (
    id INT64 NOT NULL,
    circle_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE date_table (
    id INT64 NOT NULL,
    date_col DATE
) PRIMARY KEY (id);

CREATE TABLE decimal_table (
    id INT64 NOT NULL,
    decimal_col NUMERIC
) PRIMARY KEY (id);

CREATE TABLE double_precision_table (
    id INT64 NOT NULL,
    double_precision_col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE float4_table (
    id INT64 NOT NULL,
    float4_col FLOAT32
) PRIMARY KEY (id);

CREATE TABLE float8_table (
    id INT64 NOT NULL,
    float8_col FLOAT64
) PRIMARY KEY (id);

CREATE TABLE inet_table (
    id INT64 NOT NULL,
    inet_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE integer_table (
    id INT64 NOT NULL,
    integer_col INT64
) PRIMARY KEY (id);

CREATE TABLE int2_table (
    id INT64 NOT NULL,
    int2_col INT64
) PRIMARY KEY (id);

CREATE TABLE int4_table (
    id INT64 NOT NULL,
    int4_col INT64
) PRIMARY KEY (id);

CREATE TABLE int8_table (
    id INT64 NOT NULL,
    int8_col INT64
) PRIMARY KEY (id);

CREATE TABLE interval_table (
    id INT64 NOT NULL,
    interval_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE json_table (
    id INT64 NOT NULL,
    json_col JSON
) PRIMARY KEY (id);

CREATE TABLE jsonb_table (
    id INT64 NOT NULL,
    jsonb_col JSON
) PRIMARY KEY (id);

CREATE TABLE line_table (
    id INT64 NOT NULL,
    line_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE lseg_table (
    id INT64 NOT NULL,
    lseg_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE macaddr_table (
    id INT64 NOT NULL,
    macaddr_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE macaddr8_table (
    id INT64 NOT NULL,
    macaddr8_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE money_table (
    id INT64 NOT NULL,
    money_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE numeric_table (
    id INT64 NOT NULL,
    numeric_col NUMERIC
) PRIMARY KEY (id);

CREATE TABLE path_table (
    id INT64 NOT NULL,
    path_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE pg_lsn_table (
    id INT64 NOT NULL,
    pg_lsn_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE pg_snapshot_table (
    id INT64 NOT NULL,
    pg_snapshot_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE point_table (
    id INT64 NOT NULL,
    point_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE polygon_table (
    id INT64 NOT NULL,
    polygon_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE real_table (
    id INT64 NOT NULL,
    real_col FLOAT32
) PRIMARY KEY (id);

CREATE TABLE serial2_table (
    id INT64 NOT NULL,
    serial2_col INT64
) PRIMARY KEY (id);

CREATE TABLE serial4_table (
    id INT64 NOT NULL,
    serial4_col INT64
) PRIMARY KEY (id);

CREATE TABLE serial8_table (
    id INT64 NOT NULL,
    serial8_col INT64
) PRIMARY KEY (id);

CREATE TABLE smallint_table (
    id INT64 NOT NULL,
    smallint_col INT64
) PRIMARY KEY (id);

CREATE TABLE smallserial_table (
    id INT64 NOT NULL,
    smallserial_col INT64
) PRIMARY KEY (id);

CREATE TABLE text_table (
    id INT64 NOT NULL,
    text_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE time_table (
    id INT64 NOT NULL,
    time_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE time_without_time_zone_table (
    id INT64 NOT NULL,
    time_without_time_zone_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE timestamp_table (
    id INT64 NOT NULL,
    timestamp_col TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE timestamp_without_time_zone_table (
    id INT64 NOT NULL,
    timestamp_without_time_zone_col TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE timestamptz_table (
    id INT64 NOT NULL,
    timestamptz_col TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE timestamp_with_time_zone_table (
    id INT64 NOT NULL,
    timestamp_with_time_zone_col TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE timetz_table (
    id INT64 NOT NULL,
    timetz_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE time_with_time_zone_table (
    id INT64 NOT NULL,
    time_with_time_zone_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE tsquery_table (
    id INT64 NOT NULL,
    tsquery_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE tsvector_table (
    id INT64 NOT NULL,
    tsvector_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE txid_snapshot_table (
    id INT64 NOT NULL,
    txid_snapshot_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE uuid_table (
    id INT64 NOT NULL,
    uuid_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE varbit_table (
    id INT64 NOT NULL,
    varbit_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE varchar_table (
    id INT64 NOT NULL,
    varchar_col STRING(MAX)
) PRIMARY KEY (id);

CREATE TABLE xml_table (
    id INT64 NOT NULL,
    xml_col STRING(MAX)
) PRIMARY KEY (id);
