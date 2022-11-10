#!/bin/bash
. ~db2inst1/sqllib/db2profile
db2 "connect to TESTDB"
db2 "CREATE VARIABLE UTIL.ID_VALUE INTEGER"
db2 "SET UTIL.ID_VALUE = $1"
db2 -tvmf ~/queries/insert_table.sql