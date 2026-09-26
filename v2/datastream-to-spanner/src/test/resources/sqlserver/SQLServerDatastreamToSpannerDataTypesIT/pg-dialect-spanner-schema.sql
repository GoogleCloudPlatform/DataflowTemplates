CREATE TABLE varchar_table (
    id bigint PRIMARY KEY,
    varchar_col character varying(2621440)
);

CREATE TABLE tinyint_table (
    id bigint PRIMARY KEY,
    tinyint_col bigint
);

CREATE TABLE smallint_table (
    id bigint PRIMARY KEY,
    smallint_col bigint
);

CREATE TABLE int_table (
    id bigint PRIMARY KEY,
    int_col bigint
);

CREATE TABLE bigint_table (
    id bigint PRIMARY KEY,
    bigint_col bigint
);

CREATE TABLE bit_table (
    id bigint PRIMARY KEY,
    bit_col boolean
);

CREATE TABLE decimal_table (
    id bigint PRIMARY KEY,
    decimal_col numeric
);

CREATE TABLE numeric_table (
    id bigint PRIMARY KEY,
    numeric_col numeric
);

CREATE TABLE money_table (
    id bigint PRIMARY KEY,
    money_col numeric
);

CREATE TABLE smallmoney_table (
    id bigint PRIMARY KEY,
    smallmoney_col numeric
);

CREATE TABLE float_table (
    id bigint PRIMARY KEY,
    float_col double precision
);

CREATE TABLE real_table (
    id bigint PRIMARY KEY,
    real_col real
);

CREATE TABLE date_table (
    id bigint PRIMARY KEY,
    date_col date
);

CREATE TABLE time_table (
    id bigint PRIMARY KEY,
    time_col character varying(2621440)
);

CREATE TABLE datetime2_table (
    id bigint PRIMARY KEY,
    datetime2_col timestamp with time zone
);

CREATE TABLE datetimeoffset_table (
    id bigint PRIMARY KEY,
    datetimeoffset_col timestamp with time zone
);

CREATE TABLE datetime_table (
    id bigint PRIMARY KEY,
    datetime_col timestamp with time zone
);

CREATE TABLE smalldatetime_table (
    id bigint PRIMARY KEY,
    smalldatetime_col timestamp with time zone
);

CREATE TABLE char_table (
    id bigint PRIMARY KEY,
    char_col character varying(2621440)
);

CREATE TABLE text_table (
    id bigint PRIMARY KEY,
    text_col character varying(2621440)
);

CREATE TABLE nchar_table (
    id bigint PRIMARY KEY,
    nchar_col character varying(2621440)
);

CREATE TABLE nvarchar_table (
    id bigint PRIMARY KEY,
    nvarchar_col character varying(2621440)
);

CREATE TABLE ntext_table (
    id bigint PRIMARY KEY,
    ntext_col character varying(2621440)
);

CREATE TABLE binary_table (
    id bigint PRIMARY KEY,
    binary_col bytea
);

CREATE TABLE varbinary_table (
    id bigint PRIMARY KEY,
    varbinary_col bytea
);

CREATE TABLE image_table (
    id bigint PRIMARY KEY,
    image_col bytea
);

CREATE TABLE uniqueidentifier_table (
    id bigint PRIMARY KEY,
    uniqueidentifier_col character varying(2621440)
);

CREATE TABLE xml_table (
    id bigint PRIMARY KEY,
    xml_col character varying(2621440)
);

CREATE TABLE tinyint_to_string_table (
    id bigint PRIMARY KEY,
    tinyint_to_string_col character varying(2621440)
);

CREATE TABLE smallint_to_string_table (
    id bigint PRIMARY KEY,
    smallint_to_string_col character varying(2621440)
);

CREATE TABLE int_to_string_table (
    id bigint PRIMARY KEY,
    int_to_string_col character varying(2621440)
);

CREATE TABLE bigint_to_string_table (
    id bigint PRIMARY KEY,
    bigint_to_string_col character varying(2621440)
);

CREATE TABLE bit_to_int64_table (
    id bigint PRIMARY KEY,
    bit_to_int64_col bigint
);

CREATE TABLE bit_to_string_table (
    id bigint PRIMARY KEY,
    bit_to_string_col character varying(2621440)
);

CREATE TABLE decimal_to_string_table (
    id bigint PRIMARY KEY,
    decimal_to_string_col character varying(2621440)
);

CREATE TABLE decimal_to_float64_table (
    id bigint PRIMARY KEY,
    decimal_to_float64_col double precision
);

CREATE TABLE numeric_to_string_table (
    id bigint PRIMARY KEY,
    numeric_to_string_col character varying(2621440)
);

CREATE TABLE money_to_string_table (
    id bigint PRIMARY KEY,
    money_to_string_col character varying(2621440)
);

CREATE TABLE smallmoney_to_string_table (
    id bigint PRIMARY KEY,
    smallmoney_to_string_col character varying(2621440)
);

CREATE TABLE float_to_string_table (
    id bigint PRIMARY KEY,
    float_to_string_col character varying(2621440)
);

CREATE TABLE real_to_float64_table (
    id bigint PRIMARY KEY,
    real_to_float64_col double precision
);

CREATE TABLE real_to_string_table (
    id bigint PRIMARY KEY,
    real_to_string_col character varying(2621440)
);

CREATE TABLE date_to_string_table (
    id bigint PRIMARY KEY,
    date_to_string_col character varying(2621440)
);

CREATE TABLE datetime2_to_string_table (
    id bigint PRIMARY KEY,
    datetime2_to_string_col character varying(2621440)
);

CREATE TABLE datetimeoffset_to_string_table (
    id bigint PRIMARY KEY,
    datetimeoffset_to_string_col character varying(2621440)
);

CREATE TABLE datetime_to_string_table (
    id bigint PRIMARY KEY,
    datetime_to_string_col character varying(2621440)
);

CREATE TABLE smalldatetime_to_string_table (
    id bigint PRIMARY KEY,
    smalldatetime_to_string_col character varying(2621440)
);

CREATE TABLE char_to_bytes_table (
    id bigint PRIMARY KEY,
    char_to_bytes_col bytea
);

CREATE TABLE varchar_to_bytes_table (
    id bigint PRIMARY KEY,
    varchar_to_bytes_col bytea
);

CREATE TABLE nchar_to_bytes_table (
    id bigint PRIMARY KEY,
    nchar_to_bytes_col bytea
);

CREATE TABLE nvarchar_to_bytes_table (
    id bigint PRIMARY KEY,
    nvarchar_to_bytes_col bytea
);

CREATE TABLE binary_to_string_table (
    id bigint PRIMARY KEY,
    binary_to_string_col character varying(2621440)
);

CREATE TABLE varbinary_to_string_table (
    id bigint PRIMARY KEY,
    varbinary_to_string_col character varying(2621440)
);

CREATE TABLE image_to_string_table (
    id bigint PRIMARY KEY,
    image_to_string_col character varying(2621440)
);

CREATE TABLE tinyint_pk_table (
    id bigint PRIMARY KEY,
    tinyint_pk_col bigint
);

CREATE TABLE smallint_pk_table (
    id bigint PRIMARY KEY,
    smallint_pk_col bigint
);

CREATE TABLE int_pk_table (
    id bigint PRIMARY KEY,
    int_pk_col bigint
);

CREATE TABLE bigint_pk_table (
    id bigint PRIMARY KEY,
    bigint_pk_col bigint
);

CREATE TABLE bit_pk_table (
    id boolean PRIMARY KEY,
    bit_pk_col boolean
);

CREATE TABLE date_pk_table (
    id date PRIMARY KEY,
    date_pk_col date
);

CREATE TABLE time_pk_table (
    id character varying(2621440) PRIMARY KEY,
    time_pk_col character varying(2621440)
);

CREATE TABLE datetime2_pk_table (
    id timestamp with time zone PRIMARY KEY,
    datetime2_pk_col timestamp with time zone
);

CREATE TABLE datetimeoffset_pk_table (
    id timestamp with time zone PRIMARY KEY,
    datetimeoffset_pk_col timestamp with time zone
);

CREATE TABLE datetime_pk_table (
    id timestamp with time zone PRIMARY KEY,
    datetime_pk_col timestamp with time zone
);

CREATE TABLE smalldatetime_pk_table (
    id timestamp with time zone PRIMARY KEY,
    smalldatetime_pk_col timestamp with time zone
);

CREATE TABLE char_pk_table (
    id character varying(100) PRIMARY KEY,
    char_pk_col character varying(100)
);

CREATE TABLE varchar_pk_table (
    id character varying(100) PRIMARY KEY,
    varchar_pk_col character varying(100)
);

CREATE TABLE nchar_pk_table (
    id character varying(100) PRIMARY KEY,
    nchar_pk_col character varying(100)
);

CREATE TABLE nvarchar_pk_table (
    id character varying(100) PRIMARY KEY,
    nvarchar_pk_col character varying(100)
);

CREATE TABLE binary_pk_table (
    id bytea PRIMARY KEY,
    binary_pk_col bytea
);

CREATE TABLE varbinary_pk_table (
    id bytea PRIMARY KEY,
    varbinary_pk_col bytea
);

CREATE TABLE uniqueidentifier_pk_table (
    id character varying(2621440) PRIMARY KEY,
    uniqueidentifier_pk_col character varying(2621440)
);

CREATE TABLE generated_pk_column (
    first_name_col character varying(2621440),
    last_name_col character varying(2621440),
    generated_column_col character varying(2621440) PRIMARY KEY
);

CREATE TABLE generated_non_pk_column (
    id bigint PRIMARY KEY,
    first_name_col character varying(2621440),
    last_name_col character varying(2621440),
    generated_column_col character varying(2621440)
);

CREATE TABLE non_generated_to_generated_column (
    first_name_col character varying(2621440),
    last_name_col character varying(2621440),
    generated_column_col character varying(2621440),
    generated_column_pk_col character varying(2621440) PRIMARY KEY
);

CREATE TABLE generated_to_non_generated_column (
    first_name_col character varying(2621440),
    last_name_col character varying(2621440),
    generated_column_col character varying(2621440),
    generated_column_pk_col character varying(2621440) PRIMARY KEY
);
