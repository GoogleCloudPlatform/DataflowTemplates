#!/bin/bash
. ~db2inst1/sqllib/db2profile
db2 "connect to TESTDB"
db2 "CREATE SCHEMA TESTS"
db2 -tvmf ~/queries/create_table.sql
echo "TABLES CREATED"
whoami
db2 "list tables for SCHEMA TESTS"