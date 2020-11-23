#!/usr/bin/env python
# -*- coding: utf-8 -*- #
#
# Copyright 2013 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""gcloud command line tool."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os
import sys

_GCLOUD_PY_DIR = os.path.dirname(__file__)
_THIRD_PARTY_DIR = os.path.join(_GCLOUD_PY_DIR, 'third_party')

if os.path.isdir(_THIRD_PARTY_DIR):
  sys.path.insert(0, _THIRD_PARTY_DIR)


def _fix_google_module():
  """Reloads the google module to prefer our vendored copy.

  When python is not invoked with the -S option, it can preload google module
  via .pth file setting its __path__. After this happens, our vendored google
  package may not in the __path__. After our vendored dependency directory is
  put at the first place in the sys.path, google module should be reloaded,
  so that our vendored copy can be preferred.
  """
  if 'google' not in sys.modules:
    return
  import google  # pylint: disable=g-import-not-at-top
  try:
    reload(google)
  except NameError:
    import importlib  # pylint: disable=g-import-not-at-top
    importlib.reload(google)


def _import_gcloud_main():
  """Returns reference to gcloud_main module."""
  # pylint:disable=g-import-not-at-top
  import googlecloudsdk.gcloud_main
  return googlecloudsdk.gcloud_main


def main():
  # pylint:disable=g-import-not-at-top
  from googlecloudsdk.core.util import encoding

  if encoding.GetEncodedValue(os.environ, '_ARGCOMPLETE'):
    try:
      # pylint:disable=g-import-not-at-top
      import googlecloudsdk.command_lib.static_completion.lookup as lookup
      lookup.Complete()
      return
    except Exception:  # pylint:disable=broad-except, hide completion errors
      if encoding.GetEncodedValue(os.environ, '_ARGCOMPLETE_TRACE') == 'static':
        raise

  try:
    _fix_google_module()
    gcloud_main = _import_gcloud_main()
  except Exception as err:  # pylint: disable=broad-except
    # We want to catch *everything* here to display a nice message to the user
    # pylint:disable=g-import-not-at-top
    import traceback
    # We DON'T want to suggest `gcloud components reinstall` here (ex. as
    # opposed to the similar message in gcloud_main.py), as we know that no
    # commands will work.
    sys.stderr.write(
        ('ERROR: gcloud failed to load: {0}\n{1}\n\n'
         'This usually indicates corruption in your gcloud installation or '
         'problems with your Python interpreter.\n\n'
         'Please verify that the following is the path to a working Python 2.7 '
         'or 3.5+ executable:\n'
         '    {2}\n\n'
         'If it is not, please set the CLOUDSDK_PYTHON environment variable to '
         'point to a working Python 2.7 or 3.5+ executable.\n\n'
         'If you are still experiencing problems, please reinstall the Cloud '
         'SDK using the instructions here:\n'
         '    https://cloud.google.com/sdk/\n').format(
             err,
             '\n'.join(traceback.format_exc().splitlines()[2::2]),
             sys.executable))
    sys.exit(1)
  sys.exit(gcloud_main.main())


if __name__ == '__main__':
  main()
