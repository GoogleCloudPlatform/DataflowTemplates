#!/bin/bash
. ~db2inst1/sqllib/db2profile
db2 "connect to TESTDB"
db2 "UPDATE TESTS.OBJ_BASE SET STATUS = 'updatedSTATUS' WHERE ID = 1;"