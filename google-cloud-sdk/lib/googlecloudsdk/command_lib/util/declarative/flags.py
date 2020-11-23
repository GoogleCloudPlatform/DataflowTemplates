# -*- coding: utf-8 -*- #
# Copyright 2020 Google LLC. All Rights Reserved.
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
"""Library for retrieving declarative parsers."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import os

from googlecloudsdk.command_lib.util.declarative.clients import client_base
from googlecloudsdk.core.util import files


def AddPathFlag(parser):
  parser.add_argument(
      '--path',
      type=files.ExpandHomeAndVars,
      help='Path of the directory or file to output configuration.')


def AddAllFlag(parser, collection='collection'):
  parser.add_argument(
      '--all',
      action='store_true',
      help=(
          'Retrieve all resources within the {}. If `--path` is '
          'specified and is a valid directory, resources will be output as '
          'individual files based on resource name and scope. If `--path` is not '
          'specified, resources will be streamed to stdout.'.format(collection)
      ))


def ValidateAllPathArgs(args):
  if args.IsSpecified('all'):
    if args.IsSpecified('path') and not os.path.isdir(args.path):
      raise client_base.ClientException(
          'Error executing export: "{}" must be a directory when --all is specified.'
          .format(args.path))
