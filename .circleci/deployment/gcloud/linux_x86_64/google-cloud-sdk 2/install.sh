#!/bin/sh
#
# Copyright 2013 Google Inc. All Rights Reserved.
#

echo Welcome to the Google Cloud SDK!

# <cloud-sdk-sh-preamble>
#
#  CLOUDSDK_ROOT_DIR            (a)  installation root dir
#  CLOUDSDK_PYTHON              (u)  python interpreter path
#  CLOUDSDK_GSUTIL_PYTHON       (u)  python interpreter path for gsutil
#  CLOUDSDK_PYTHON_ARGS         (u)  python interpreter arguments
#  CLOUDSDK_PYTHON_SITEPACKAGES (u)  use python site packages
#  CLOUDSDK_BQ_PYTHON           (u)  python interpreter for bq
#
# (a) always defined by the preamble
# (u) user definition overrides preamble

# Wrapper around 'which' and 'command -v', tries which first, then falls back
# to command -v
_cloudsdk_which() {
  which "$1" 2>/dev/null || command -v "$1" 2>/dev/null
}

# Check whether passed in python command reports major version 3.
_is_python3() {
  echo "$("$1" -V 2>&1)" | grep -E "Python 3" > /dev/null
}

# For Python 3, gsutil requires Python 3.5+.
_py3_interpreter_compat_with_gsutil () {
  # Some environments (e.g. macOS) don't support grep -P, so we use grep -E.
  echo "$("$1" -V 2>&1)" | grep -E "Python 3[.]([5-9]|[1-9][0-9])" > /dev/null
}

order_python() {
  selected_version=""
  for python_version in "$@"
  do
    if [ -z "$selected_version" ]; then
      if _cloudsdk_which $python_version > /dev/null && "$python_version" -c "import sys; print(sys.version)" > /dev/null; then
        selected_version=$python_version
      fi
    fi
  done
  if [ -z "$selected_version" ]; then
    selected_version=python
  fi
  echo $selected_version
}

# Determines the real cloud sdk root dir given the script path.
# Would be easier with a portable "readlink -f".
_cloudsdk_root_dir() {
  case $1 in
  /*)   _cloudsdk_path=$1
        ;;
  */*)  _cloudsdk_path=$PWD/$1
        ;;
  *)    _cloudsdk_path=$(_cloudsdk_which $1)
        case $_cloudsdk_path in
        /*) ;;
        *)  _cloudsdk_path=$PWD/$_cloudsdk_path ;;
        esac
        ;;
  esac
  _cloudsdk_dir=0
  while :
  do
    while _cloudsdk_link=$(readlink "$_cloudsdk_path")
    do
      case $_cloudsdk_link in
      /*) _cloudsdk_path=$_cloudsdk_link ;;
      *)  _cloudsdk_path=$(dirname "$_cloudsdk_path")/$_cloudsdk_link ;;
      esac
    done
    case $_cloudsdk_dir in
    1)  break ;;
    esac
    if [ -d "${_cloudsdk_path}" ]; then
      break
    fi
    _cloudsdk_dir=1
    _cloudsdk_path=$(dirname "$_cloudsdk_path")
  done
  while :
  do  case $_cloudsdk_path in
      */)     _cloudsdk_path=$(dirname "$_cloudsdk_path/.")
              ;;
      */.)    _cloudsdk_path=$(dirname "$_cloudsdk_path")
              ;;
      */bin)  dirname "$_cloudsdk_path"
              break
              ;;
      *)      echo "$_cloudsdk_path"
              break
              ;;
      esac
  done
}
CLOUDSDK_ROOT_DIR=$(_cloudsdk_root_dir "$0")

setup_cloudsdk_python() {
  # if $CLOUDSDK_PYTHON is not set, prefer python3 over python2
  if [ -z "$CLOUDSDK_PYTHON" ]; then
    CLOUDSDK_PYTHON=$(order_python python3 python2 python2.7 python)
  fi
}

setup_cloudsdk_python

# $PYTHONHOME can interfere with gcloud. Users should use
# CLOUDSDK_PYTHON to configure which python gcloud uses.
unset PYTHONHOME

# if CLOUDSDK_PYTHON_SITEPACKAGES and VIRTUAL_ENV are empty
case :$CLOUDSDK_PYTHON_SITEPACKAGES:$VIRTUAL_ENV: in
:::)  # add -S to CLOUDSDK_PYTHON_ARGS if not already there
      case " $CLOUDSDK_PYTHON_ARGS " in
      *" -S "*) ;;
      "  ")     CLOUDSDK_PYTHON_ARGS="-S"
                ;;
      *)        CLOUDSDK_PYTHON_ARGS="$CLOUDSDK_PYTHON_ARGS -S"
                ;;
      esac
      unset CLOUDSDK_PYTHON_SITEPACKAGES
      ;;
*)    # remove -S from CLOUDSDK_PYTHON_ARGS if already there
      while :; do
        case " $CLOUDSDK_PYTHON_ARGS " in
        *" -S "*) CLOUDSDK_PYTHON_ARGS=${CLOUDSDK_PYTHON_ARGS%%-S*}' '${CLOUDSDK_PYTHON_ARGS#*-S} ;;
        *) break ;;
        esac
      done
      # if CLOUDSDK_PYTHON_SITEPACKAGES is empty
      [ -z "$CLOUDSDK_PYTHON_SITEPACKAGES" ] &&
        CLOUDSDK_PYTHON_SITEPACKAGES=1
      export CLOUDSDK_PYTHON_SITEPACKAGES
      ;;
esac

# Allow users to set the Python interpreter used to launch gsutil, falling
# back to the CLOUDSDK_PYTHON interpreter otherwise.
if [ -z "$CLOUDSDK_GSUTIL_PYTHON" ]; then
  CLOUDSDK_GSUTIL_PYTHON="$CLOUDSDK_PYTHON"
fi

if [ -z "$CLOUDSDK_BQ_PYTHON" ]; then
  CLOUDSDK_BQ_PYTHON="$CLOUDSDK_PYTHON"
fi

export CLOUDSDK_ROOT_DIR
export CLOUDSDK_PYTHON_ARGS
export CLOUDSDK_GSUTIL_PYTHON
export CLOUDSDK_BQ_PYTHON

case $HOSTNAME in
  *.corp.google.com|*.c.googlers.com) export CLOUDSDK_GOOGLE_AUTH_IS_GOOGLE_DOMAIN=true;;
esac

# </cloud-sdk-sh-preamble>

if [ -z "$CLOUDSDK_PYTHON" ]; then
  if [ -z "$( _cloudsdk_which python)" ]; then
    echo
    echo "To use the Google Cloud SDK, you must have Python installed and on your PATH."
    echo "As an alternative, you may also set the CLOUDSDK_PYTHON environment variable"
    echo "to the location of your Python executable."
    exit 1
  fi
fi

# Warns user if they are running as root.
if [ $(id -u) = 0 ]; then
  echo "WARNING: You appear to be running this script as root. This may cause "
  echo "the installation to be inaccessible to users other than the root user."
fi

"$CLOUDSDK_PYTHON" $CLOUDSDK_PYTHON_ARGS "${CLOUDSDK_ROOT_DIR}/bin/bootstrapping/install.py" "$@"
