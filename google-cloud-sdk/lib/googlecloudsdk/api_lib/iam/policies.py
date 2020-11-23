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
"""Utilities for Policies API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import binascii

from apitools.base.protorpclite import messages as apitools_messages
from apitools.base.py import encoding
from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.calliope import exceptions as gcloud_exceptions
from googlecloudsdk.command_lib.iam import iam_util
from googlecloudsdk.core import yaml
import six


def GetClientInstance(no_http=False):
  return apis.GetClientInstance('iam', 'v2alpha1', no_http=no_http)


def GetMessagesModule(client=None):
  client = client or GetClientInstance()
  return client.MESSAGES_MODULE


def ParseYamlOrJsonPolicyFile(policy_file_path, policy_message_type):
  """Create an IAM V2 Policy protorpc.Message from YAML or JSON formatted file.

  Returns the parsed policy object.
  Args:
    policy_file_path: Path to the YAML or JSON IAM policy file.
    policy_message_type: Policy message type to convert YAML to.
  Returns:
    policy that is a protorpc.Message of type policy_message_type filled in
    from the JSON or YAML policy file
  Raises:
    BadFileException if the YAML or JSON file is malformed.
    IamEtagReadError if the etag is badly formatted.
  """
  policy_to_parse = yaml.load_path(policy_file_path)
  try:
    policy = encoding.PyValueToMessage(policy_message_type, policy_to_parse)
  except (AttributeError) as e:
    # Raised when the input file is not properly formatted YAML policy file.
    raise gcloud_exceptions.BadFileException(
        'Policy file [{0}] is not a properly formatted YAML or JSON '
        'policy file. {1}'
        .format(policy_file_path, six.text_type(e)))
  except (apitools_messages.DecodeError, binascii.Error) as e:
    # DecodeError is raised when etag is badly formatted (not proper Base64)
    raise iam_util.IamEtagReadError(
        'The etag of policy file [{0}] is not properly formatted. {1}'
        .format(policy_file_path, six.text_type(e)))
  return policy

