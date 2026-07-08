CREATE TABLE Users (
    id INT NOT NULL,
    name VARCHAR(25),
    `from` VARCHAR(25),
 PRIMARY KEY(id));

 CREATE TABLE TableWithVirtualGeneratedColumn (
    id INT NOT NULL,
    column1 INT,
    virtual_generated_column INT AS (column1 + id) VIRTUAL,
    PRIMARY KEY(id)
 );

 CREATE TABLE TableWithStoredGeneratedColumn (
     id INT NOT NULL,
     column1 INT,
     stored_generated_column INT AS (column1 + id) STORED,
     PRIMARY KEY(id)
 );

 CREATE TABLE testtable_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvYZPAeGeqiO(
 id INT NOT NULL,
 col_qcbF69RmXTRe3B_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvY VARCHAR(25),
 PRIMARY KEY(id));

 CREATE TABLE TableWithIdentityColumn (
     id BIGINT NOT NULL AUTO_INCREMENT,
     column1 VARCHAR(25),
     PRIMARY KEY(id)
 );

CREATE TABLE IF NOT EXISTS `generated_pk_column_table` (
    `first_name_col` varchar(50) DEFAULT NULL,
    `last_name_col` varchar(50) DEFAULT NULL,
    `generated_column_col` varchar(100) GENERATED ALWAYS AS (concat(`first_name_col`,' ')) STORED NOT NULL,
    PRIMARY KEY (`generated_column_col`)
);

CREATE TABLE IF NOT EXISTS `generated_non_pk_column_table` (
    `first_name_col` varchar(50) DEFAULT NULL,
    `last_name_col` varchar(50) DEFAULT NULL,
    `generated_column_col` varchar(100) GENERATED ALWAYS AS (concat(`first_name_col`,' ')) STORED NOT NULL,
    `id` int not null,
    PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `non_generated_to_generated_column_table` (
    `first_name_col` varchar(50) DEFAULT NULL,
    `last_name_col` varchar(50) DEFAULT NULL,
    `generated_column_col` varchar(100) NOT NULL,
    `generated_column_pk_col` varchar(100) NOT NULL,
    PRIMARY KEY (`generated_column_pk_col`)
);

CREATE TABLE IF NOT EXISTS `generated_to_non_generated_column_table` (
    `first_name_col` varchar(50) DEFAULT NULL,
    `last_name_col` varchar(50) DEFAULT NULL,
    `generated_column_col` varchar(100) GENERATED ALWAYS AS (concat(`first_name_col`,' ')) STORED NOT NULL,
    `generated_column_pk_col` varchar(100) GENERATED ALWAYS AS (concat(`first_name_col`,' ')) STORED NOT NULL,
    PRIMARY KEY (`generated_column_pk_col`)
);
