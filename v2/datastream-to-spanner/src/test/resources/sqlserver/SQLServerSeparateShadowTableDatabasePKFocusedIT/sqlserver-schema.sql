CREATE TABLE my_table (
    id INT NOT NULL PRIMARY KEY,
    val INT
);

CREATE TABLE alltypes (
    bool_field BIT NOT NULL,
    int64_field BIGINT NOT NULL,
    float64_field FLOAT(53) NOT NULL,
    string_field NVARCHAR(255) NOT NULL,
    bytes_field VARBINARY(100),
    timestamp_field DATETIMEOFFSET NOT NULL,
    date_field DATE NOT NULL,
    numeric_field NUMERIC(38,9) NOT NULL,
    val INT,
    CONSTRAINT pk_alltypes PRIMARY KEY (
        bool_field,
        int64_field,
        float64_field,
        string_field,
        timestamp_field,
        date_field,
        numeric_field
    )
);
