cqlsh -u cassandra -p cassandra
CREATE ROLE <super_user> WITH PASSWORD = '<password>' AND SUPERUSER = true AND LOGIN = true;

CREATE KEYSPACE ecommerce
WITH replication = {
    'class': 'SimpleStrategy',      
    'replication_factor': 1       
} 
AND durable_writes = true;

CREATE TABLE ecommerce.customers (
    customer_id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    address TEXT,
    phone TEXT
);

CREATE TABLE ecommerce.products (
    product_id UUID PRIMARY KEY,
    name TEXT,
    description TEXT,
    price DECIMAL,
    stock INT
);

CREATE TABLE ecommerce.orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID,
    order_date TIMESTAMP,
    status TEXT,
    total DECIMAL
);