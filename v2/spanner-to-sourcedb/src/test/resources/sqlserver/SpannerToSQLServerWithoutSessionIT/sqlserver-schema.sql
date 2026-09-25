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
