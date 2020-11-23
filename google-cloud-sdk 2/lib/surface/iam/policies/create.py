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
"""Command to create a policy on the given attachment point."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import textwrap

from googlecloudsdk.api_lib.iam import policies as apis
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.iam import policies_flags as flags


@base.Hidden
@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Create(base.CreateCommand):
  """Create a policy on the given attachment point with the given name."""

  detailed_help = {
      'EXAMPLES':
          textwrap.dedent("""\
          The following command creates the IAM policy defined at the resource
          project "123" of kind "denypolicies" and id "my-deny-policy" from the
          file "policy.json" :


            $ {command} my-deny-policy --resource=cloudresourcemanager.googleapis.com/projects/123 --kind=denypolicies --policy_file=policy.json
          """),
  }

  @staticmethod
  def Args(parser):
    flags.GetAttachmentPointFlag().AddToParser(parser)
    flags.GetKindFlag().AddToParser(parser)
    flags.GetPolicyIDFlag().AddToParser(parser)
    flags.GetPolicyFileFlag().AddToParser(parser)

  def Run(self, args):
    client = apis.GetClientInstance()
    messages = apis.GetMessagesModule()

    attachment_point = args.attachment_point.replace('/', '%2F')

    result = client.policies.CreatePolicy(
        messages.IamPoliciesCreatePolicyRequest(
            parent='policies/{}/{}'.format(attachment_point, args.kind),
            policyId=args.policy_id,
            googleIamV2alpha1Policy=apis.ParseYamlOrJsonPolicyFile(
                args.policy_file, messages.GoogleIamV2alpha1Policy)))
    return result
