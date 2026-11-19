CREATE TABLE Users (
    id INT NOT NULL,
    name VARCHAR(25),
    [from] VARCHAR(25),
    PRIMARY KEY(id)
);

CREATE TABLE Users2 (
    id INT NOT NULL,
    name VARCHAR(25),
    PRIMARY KEY(id)
);

CREATE TABLE TableWithVirtualGeneratedColumn (
    id INT NOT NULL,
    column1 INT,
    virtual_generated_column AS (column1 + id),
    PRIMARY KEY(id)
);

CREATE TABLE TableWithStoredGeneratedColumn (
    id INT NOT NULL,
    column1 INT,
    stored_generated_column AS (column1 + id) PERSISTED,
    PRIMARY KEY(id)
);

CREATE TABLE testtable_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvYZPAeGeqiO (
    id INT NOT NULL,
    col_qcbF69RmXTRe3B_03TpCoVF16ED0KLxM3v808cH3bTGQ0uK_FEXuZHbttvY VARCHAR(25),
    PRIMARY KEY(id)
);

CREATE TABLE TableWithIdentityColumn (
    id BIGINT NOT NULL,
    column1 VARCHAR(25),
    PRIMARY KEY(id)
);

CREATE TABLE generated_pk_column_table (
    first_name_col VARCHAR(50) NULL,
    last_name_col VARCHAR(50) NULL,
    generated_column_col AS (concat(first_name_col, ' ')) PERSISTED NOT NULL,
    PRIMARY KEY (generated_column_col)
);

CREATE TABLE generated_non_pk_column_table (
    first_name_col VARCHAR(50) NULL,
    last_name_col VARCHAR(50) NULL,
    generated_column_col AS (concat(first_name_col, ' ')) PERSISTED,
    id INT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE non_generated_to_generated_column_table (
    first_name_col VARCHAR(50) NULL,
    last_name_col VARCHAR(50) NULL,
    generated_column_col VARCHAR(100) NOT NULL,
    generated_column_pk_col VARCHAR(100) NOT NULL,
    PRIMARY KEY (generated_column_pk_col)
);

CREATE TABLE generated_to_non_generated_column_table (
    first_name_col VARCHAR(50) NULL,
    last_name_col VARCHAR(50) NULL,
    generated_column_col AS (concat(first_name_col, ' ')) PERSISTED NOT NULL,
    generated_column_pk_col AS (concat(first_name_col, ' ')) PERSISTED NOT NULL,
    PRIMARY KEY (generated_column_pk_col)
);
