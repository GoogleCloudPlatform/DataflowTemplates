#!/bin/bash

su - db2inst1
. ~db2inst1/sqllib/db2profile
db2 "create database testdb"
db2 list tables
db2 list db directory
echo "we done _____"