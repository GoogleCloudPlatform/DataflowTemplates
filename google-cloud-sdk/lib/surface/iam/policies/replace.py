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
"""Command to replace a policy on the given attachment point."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import textwrap

from googlecloudsdk.api_lib.iam import policies as apis
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.iam import policies_flags as flags


@base.Hidden
@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Replace(base.UpdateCommand):
  """Replaces the policy on the given attachment point with the given name."""

  detailed_help = {
      'EXAMPLES':
          textwrap.dedent("""\
          The following command replaces the IAM policy defined at the resource
          project "123" of kind "denypolicies" and id "my-deny-policy", with
          etag "abc":

            $ {command} my-deny-policy --resource=cloudresourcemanager.googleapis.com/projects/123 --kind=denypolicies --policy_file=policy.json --etag=abc

          If etag isn't provided, the command will try to get the etag using the
          calling user permissions.
          """),
  }

  @staticmethod
  def Args(parser):
    flags.GetAttachmentPointFlag().AddToParser(parser)
    flags.GetKindFlag().AddToParser(parser)
    flags.GetPolicyIDFlag().AddToParser(parser)
    flags.GetEtagFlag().AddToParser(parser)
    flags.GetPolicyFileFlag().AddToParser(parser)

  def Run(self, args):
    client = apis.GetClientInstance()
    messages = apis.GetMessagesModule()

    attachment_point = args.attachment_point.replace('/', '%2F')

    # If etag is not present, get using GetPolicy API.
    etag = args.etag
    if not etag:
      get_result = client.policies.Get(
          messages.IamPoliciesGetRequest(name='policies/{}/{}/{}'.format(
              attachment_point, args.kind, args.policy_id)))
      if isinstance(get_result, messages.GoogleIamV2alpha1Policy):
        etag = get_result.etag
      else:
        raise Exception('Unexpected response from policies client.')

    policy = apis.ParseYamlOrJsonPolicyFile(args.policy_file,
                                            messages.GoogleIamV2alpha1Policy)
    policy.name = 'policies/{}/{}/{}'.format(attachment_point, args.kind,
                                             args.policy_id)
    policy.etag = etag

    result = client.policies.ReplacePolicy(policy)
    return result
