#!/usr/bin/env bash
set -x
. ./env.sh
#cbt -project $PROJECT -instance $INSTANCE set $TABLE foobar2 cf1:foo="$(date +%s)" cf1:bar="$(date +%s)" cf2:foo=bar
~/go/bin/cbt -project $PROJECT -instance $INSTANCE set $TABLE $1 ${@: 2}
