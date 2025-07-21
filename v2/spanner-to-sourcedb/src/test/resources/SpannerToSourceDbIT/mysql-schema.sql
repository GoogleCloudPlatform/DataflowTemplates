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
