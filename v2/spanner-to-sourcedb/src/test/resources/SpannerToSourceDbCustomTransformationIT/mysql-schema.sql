CREATE TABLE Users1 (
    id INT NOT NULL,
    first_name VARCHAR(25),
    last_name VARCHAR(25),
 PRIMARY KEY(id));

CREATE TABLE AllDatatypeTransformation (
    varchar_column VARCHAR(20) NOT NULL,
    tinyint_column TINYINT,
    text_column TEXT,
    date_column DATE,
    int_column INT,
    bigint_column BIGINT,
    float_column FLOAT(10,2),
    double_column DOUBLE,
    decimal_column DECIMAL(10,2),
    datetime_column DATETIME,
    timestamp_column TIMESTAMP,
    time_column TIME,
    year_column YEAR,
    blob_column BLOB,
    enum_column ENUM('1','2','3'),
    bool_column TINYINT(1),
    binary_column VARBINARY(150),
    bit_column BIT(8),
    PRIMARY KEY (varchar_column)
);