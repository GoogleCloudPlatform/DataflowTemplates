#!/usr/bin/env bash
set -x
. ./env.sh

#  cbt deletecolumn <table> <row> <family> <column> [app-profile=<app profile id>]
cbt -project $PROJECT -instance $INSTANCE deletecolumn $TABLE $@
