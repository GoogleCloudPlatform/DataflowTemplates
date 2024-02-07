#!/usr/bin/env bash
set -x
. ./env.sh
cbt -project $PROJECT -instance $INSTANCE lookup $1 $2 cells-per-column=100
