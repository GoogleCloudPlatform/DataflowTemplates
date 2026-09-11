CREATE TABLE Users1 (
    id INT NOT NULL,
    first_name VARCHAR(25),
    last_name VARCHAR(25),
    PRIMARY KEY(id)
);

CREATE TABLE AllDatatypeTransformation (
    varchar_column VARCHAR(20) NOT NULL,
    source_only_pk INT NOT NULL,
    tinyint_column TINYINT,
    text_column TEXT,
    date_column DATE,
    int_column INT,
    bigint_column BIGINT,
    float_column FLOAT,
    double_column FLOAT,
    decimal_column DECIMAL(10,2),
    datetime_column DATETIME,
    timestamp_column DATETIME,
    time_column TIME,
    year_column INT,
    blob_column VARBINARY(MAX),
    enum_column VARCHAR(10),
    bool_column BIT,
    binary_column VARBINARY(150),
    bit_column BIT,
    PRIMARY KEY (source_only_pk, varchar_column)
);
