# -*- coding: utf-8 -*- #
# Copyright 2016 Google LLC. All Rights Reserved.
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

"""The main command group for the gcloud debug command group."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.debug import transforms
from googlecloudsdk.calliope import base


@base.ReleaseTracks(base.ReleaseTrack.GA, base.ReleaseTrack.BETA)
class Debug(base.Group):
  """Commands for interacting with the Cloud Debugger.

  The {command} command group provides interaction with Cloud Debugger, allowing
  you to list and manipulate debugging targets, snapshots and logpoints.

  Cloud Debugger is a feature of Google Cloud that lets you inspect the state of
  an application at any code location without using logging statements and
  without stopping or slowing down your applications.

  More information on Cloud Debugger can be found here:
  https://cloud.google.com/debugger and detailed documentation can be found
  here: https://cloud.google.com/debugger/docs/

  ## EXAMPLES

  To view all available debug targets, run:

    $ {command} targets list

    NAME           ID             DESCRIPTION
    default-test   gcp:1234:5678  myproject-test-9876543
    default-test2  gcp:9012:3456  myproject-test2-1234567

  To create a snapshot for a particular target:

    $ {command} snapshots create --target=default-test foo.py:12

  Note that if there is not a target with the exact name or ID specified, the
  target is treated as a regular expression to match against the name or
  description:

    $ {command} snapshots create --target=test foo.py:12

    ERROR: (gcloud.beta.debug.snapshots.create) Multiple possible targets found.
    Use the --target option to select one of the following targets:
        default-test
        default-test2

    In the above case, "test" matches both targets' names. Specifying 'test$'
    would match only "default-test" (by name), while "9876" would match
    "default-test" by description.
  """

  category = base.MANAGEMENT_TOOLS_CATEGORY

  @staticmethod
  def Args(parser):
    parser.display_info.AddTransforms(transforms.GetTransforms())

  def Filter(self, context, args):
    del context, args
    base.DisableUserProjectQuota()
