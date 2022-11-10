#!/bin/bash
. ~db2inst1/sqllib/db2profile
db2 "connect to TESTDB"
db2 "select trigname from syscat.triggers where tabschema='TESTS';"