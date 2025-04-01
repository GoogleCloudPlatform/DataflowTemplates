-- MySQL schema for a table with 17 primary key columns (exceeding Spanner's maximum of 16)
DROP TABLE IF EXISTS ExcessivePrimaryKeyTable;

CREATE TABLE ExcessivePrimaryKeyTable (
  pk_col1 VARCHAR(36) NOT NULL,
  pk_col2 VARCHAR(36) NOT NULL,
  pk_col3 VARCHAR(36) NOT NULL,
  pk_col4 VARCHAR(36) NOT NULL,
  pk_col5 VARCHAR(36) NOT NULL,
  pk_col6 VARCHAR(36) NOT NULL,
  pk_col7 VARCHAR(36) NOT NULL,
  pk_col8 VARCHAR(36) NOT NULL,
  pk_col9 VARCHAR(36) NOT NULL,
  pk_col10 VARCHAR(36) NOT NULL,
  pk_col11 VARCHAR(36) NOT NULL,
  pk_col12 VARCHAR(36) NOT NULL,
  pk_col13 VARCHAR(36) NOT NULL,
  pk_col14 VARCHAR(36) NOT NULL,
  pk_col15 VARCHAR(36) NOT NULL,
  pk_col16 VARCHAR(36) NOT NULL,
  pk_col17 VARCHAR(36) NOT NULL,
  data_column1 VARCHAR(36),
  data_column2 INT,
  PRIMARY KEY (pk_col1, pk_col2, pk_col3, pk_col4, pk_col5, pk_col6, pk_col7, pk_col8, pk_col9, pk_col10, 
               pk_col11, pk_col12, pk_col13, pk_col14, pk_col15, pk_col16, pk_col17)
);

-- Insert a sample record
INSERT INTO ExcessivePrimaryKeyTable (
  pk_col1, pk_col2, pk_col3, pk_col4, pk_col5, pk_col6, pk_col7, pk_col8, pk_col9, pk_col10, 
  pk_col11, pk_col12, pk_col13, pk_col14, pk_col15, pk_col16, pk_col17, data_column1, data_column2
) VALUES (
  'val1', 'val2', 'val3', 'val4', 'val5', 'val6', 'val7', 'val8', 'val9', 'val10',
  'val11', 'val12', 'val13', 'val14', 'val15', 'val16', 'val17', 'data1', 123
);
