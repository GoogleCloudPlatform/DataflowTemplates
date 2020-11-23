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
"""Common flags for policies API commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base


def GetAttachmentPointFlag():
  return base.Argument(
      '--attachment-point',
      required=True,
      help='The resource to which the policy is attached.')


def GetKindFlag():
  return base.Argument(
      '--kind',
      required=True,
      help='The kind of the policy.')


def GetPolicyIDFlag():
  return base.Argument(
      'policy_id',
      help='The id of the policy.')


def GetEtagFlag():
  return base.Argument('--etag', help='The etag of the existing policy.')


def GetPolicyFileFlag():
  return base.Argument(
      '--policy-file', required=True, help='The contents of the policy.')
