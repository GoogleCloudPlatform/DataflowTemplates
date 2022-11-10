#!/bin/bash
. ~db2inst1/sqllib/db2profile
db2 "connect to TESTDB"
db2 -tvmf ~/queries/select_table.sql
db2 "select id from tests.h_obj_base;"