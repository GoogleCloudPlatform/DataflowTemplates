#!/usr/bin/env bash
set -x
. ./env.sh
cbt -project $PROJECT -instance $INSTANCE deleterow $TABLE $1
