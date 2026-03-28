CREATE TABLE Customers (
    CustomerId INT NOT NULL PRIMARY KEY,
    CustomerName VARCHAR(255),
    CreditLimit DECIMAL(10, 2) NOT NULL,
    LegacyRegion VARCHAR(50),
    CONSTRAINT CHK_CreditLimit CHECK (CreditLimit > 1000)
);

CREATE TABLE Orders (
    CustomerId INT NOT NULL,
    OrderId INT NOT NULL,
    OrderValue DECIMAL(10, 2),
    LegacyOrderSystem VARCHAR(50) NOT NULL,
    PRIMARY KEY (CustomerId, LegacyOrderSystem, OrderId),
    CONSTRAINT FK_CustomerOrder FOREIGN KEY (CustomerId) REFERENCES Customers(CustomerId)
);

CREATE TABLE `AllDataTypes` (
                                `id` INT PRIMARY KEY,
                                `varchar_col` VARCHAR(1000) CHARACTER SET utf8 DEFAULT NULL,
                                `tinyint_col` TINYINT DEFAULT NULL,
                                `tinyint_unsigned_col` TINYINT UNSIGNED DEFAULT NULL,
                                `text_col` TEXT CHARACTER SET utf8 DEFAULT NULL,
                                `date_col` DATE DEFAULT NULL,
                                `smallint_col` SMALLINT DEFAULT NULL,
                                `smallint_unsigned_col` SMALLINT UNSIGNED DEFAULT NULL,
                                `mediumint_col` MEDIUMINT DEFAULT NULL,
                                `mediumint_unsigned_col` MEDIUMINT UNSIGNED DEFAULT NULL,
                                `bigint_col` BIGINT DEFAULT NULL,
                                `bigint_unsigned_col` BIGINT UNSIGNED DEFAULT NULL,
                                `float_col` FLOAT DEFAULT NULL,
                                `double_col` DOUBLE DEFAULT NULL,
                                `decimal_col` DECIMAL(65,30) DEFAULT NULL,
                                `datetime_col` DATETIME DEFAULT NULL,
                                `time_col` TIME DEFAULT NULL,
                                `year_col` YEAR DEFAULT NULL,
                                `char_col` CHAR(255) CHARACTER SET utf8 DEFAULT NULL,
                                `tinyblob_col` TINYBLOB DEFAULT NULL,
                                `tinytext_col` TINYTEXT CHARACTER SET utf8 DEFAULT NULL,
                                `blob_col` BLOB DEFAULT NULL,
                                `mediumblob_col` MEDIUMBLOB DEFAULT NULL,
                                `mediumtext_col` MEDIUMTEXT CHARACTER SET utf8 DEFAULT NULL,
                                `test_json_col` JSON DEFAULT NULL,
                                `longblob_col` LONGBLOB DEFAULT NULL,
                                `longtext_col` LONGTEXT CHARACTER SET utf8 DEFAULT NULL,
                                `enum_col` ENUM('1','2','3') CHARACTER SET utf8 DEFAULT NULL,
                                `bool_col` TINYINT(1) DEFAULT NULL,
                                `binary_col` BINARY(255) DEFAULT NULL,
                                `varbinary_col` VARBINARY(1000) DEFAULT NULL,
                                `bit_col` BIT(64) DEFAULT NULL,
                                `bit8_col` BIT(8) DEFAULT NULL,
                                `bit1_col` BIT(1) DEFAULT NULL,
                                `boolean_col` TINYINT(1) DEFAULT NULL,
                                `int_col` INT DEFAULT NULL,
                                `integer_unsigned_col` INTEGER UNSIGNED DEFAULT NULL,
                                `timestamp_col` TIMESTAMP DEFAULT NULL,
                                `set_col` SET('v1', 'v2', 'v3') DEFAULT NULL
);