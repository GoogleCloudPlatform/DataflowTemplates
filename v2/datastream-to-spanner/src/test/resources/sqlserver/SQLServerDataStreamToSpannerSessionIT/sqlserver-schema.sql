CREATE TABLE Category (
  category_id TINYINT NOT NULL PRIMARY KEY,
  name VARCHAR(25),
  last_update DATETIMEOFFSET
);

CREATE TABLE Books (
  id INT NOT NULL,
  title VARCHAR(200),
  author_id INT
);

CREATE UNIQUE INDEX ux_Books_id ON Books (id);

CREATE TABLE Genre (
  genre_id NUMERIC(10, 0) NOT NULL PRIMARY KEY,
  name VARCHAR(200)
);

EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Books', @role_name = NULL, @supports_net_changes = 1, @index_name = N'ux_Books_id';
