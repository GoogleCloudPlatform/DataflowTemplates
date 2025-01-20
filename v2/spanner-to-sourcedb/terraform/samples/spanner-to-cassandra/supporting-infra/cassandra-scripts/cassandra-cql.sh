#!/bin/bash

# Check if credentials file exists
if [ ! -f "cred.txt" ]; then
    echo "Error: cred.txt file not found!"
    echo "Please create cred.txt with username on line 1 and password on line 2"
    exit 1
fi

# Read credentials from file
SUPER_USER=$(sed -n '1p' cred.txt)
PASSWORD=$(sed -n '2p' cred.txt)

# Validate that credentials were read
if [ -z "$SUPER_USER" ] || [ -z "$PASSWORD" ]; then
    echo "Error: Could not read credentials from cred.txt"
    echo "Please ensure cred.txt contains username on line 1 and password on line 2"
    exit 1
fi

# Create a temporary file for CQL commands
TMP_FILE=$(mktemp)

# Write CQL commands to temporary file
cat << EOF > "$TMP_FILE"
CREATE ROLE $SUPER_USER WITH PASSWORD = '$PASSWORD' AND SUPERUSER = true AND LOGIN = true;

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
EOF

# Execute the commands using cqlsh
cqlsh -u cassandra -p cassandra -f "$TMP_FILE"

# Check if the commands were executed successfully
if [ $? -eq 0 ]; then
    echo "Successfully created superuser, keyspace, and tables."
else
    echo "An error occurred while executing the commands."
fi

# Clean up temporary file
rm "$TMP_FILE"