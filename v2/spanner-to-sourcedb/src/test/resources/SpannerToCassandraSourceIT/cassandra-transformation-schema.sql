DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    id int PRIMARY KEY,
    full_name text,
    first_name text,
    last_name text,
    empty_string text,
    null_key text
);