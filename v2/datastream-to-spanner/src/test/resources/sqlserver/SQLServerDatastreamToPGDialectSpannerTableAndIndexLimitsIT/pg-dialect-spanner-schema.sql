CREATE TABLE "LargeKey" (
  pk_col1 character varying(255) NOT NULL,
  pk_col2 character varying(255) NOT NULL,
  pk_col3 character varying(255) NOT NULL,
  col1 character varying(255),
  col2 character varying(255),
  col3 character varying(255),
  value_col character varying(2621440),
  PRIMARY KEY (pk_col1, pk_col2, pk_col3)
);

CREATE INDEX large_index ON "LargeKey" (col1, col2, col3);

CREATE TABLE "LargeCell" (
  id bigint NOT NULL,
  max_string_col_to_bytes bytea,
  max_string_col_to_str character varying(2621440),
  PRIMARY KEY (id)
);

CREATE TABLE "WideRow" (
  id bigint NOT NULL,
  col1 character varying(2621440),
  col2 character varying(2621440),
  col3 character varying(2621440),
  PRIMARY KEY (id)
);
