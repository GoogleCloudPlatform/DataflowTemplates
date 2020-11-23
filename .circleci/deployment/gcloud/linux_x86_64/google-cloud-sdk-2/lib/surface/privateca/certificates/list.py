# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
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
"""List certificates within a project."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import list_pager

from googlecloudsdk.api_lib.privateca import base as privateca_base
from googlecloudsdk.api_lib.util import common_args
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope.concepts import deps
from googlecloudsdk.command_lib.privateca import filter_rewrite
from googlecloudsdk.command_lib.privateca import resource_args
from googlecloudsdk.command_lib.privateca import response_utils
from googlecloudsdk.command_lib.privateca import text_utils
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.command_lib.util.concepts import presentation_specs
from googlecloudsdk.core import log

_DETAILED_HELP = {
    'EXAMPLES':
        """\
        To list all Certificates issued by a given Certificate Authority, run:

          $ {command} --issuer=my-ca --location=us-west1

        To list all Certificates issued by all Certificate Authorities in a
        location, run:

          $ {command} --location=us-west1

        You can omit the --location flag in both of the above examples if you've
        already set the privateca/location property. For example:

          $ {top_command} config set privateca/location us-west1

          # The following is equivalent to the first example above.
          $ {command} --issuer=my-ca

          # The following is equivalent to the second example above.
          $ {command}
        """
}


class List(base.ListCommand):
  """List certificates within a project."""

  detailed_help = _DETAILED_HELP

  @staticmethod
  def Args(parser):
    concept_parsers.ConceptParser([
        presentation_specs.ResourcePresentationSpec(
            '--issuer',
            resource_args.CreateCertificateAuthorityResourceSpec(
                'CERTIFICATE_AUTHORITY',
                ca_id_fallthroughs=[
                    deps.Fallthrough(
                        function=lambda: '-',
                        hint=('defaults to all Certificate Authorities in the '
                              'given location'),
                        active=False,
                        plural=False)
                ]), 'The issuing Certificate Authority. If this is omitted, '
            'Certificates issued by all Certificate Authorities in the given '
            'location will be listed.',
            required=True),
    ]).AddToParser(parser)
    base.PAGE_SIZE_FLAG.SetDefault(parser, 100)

    parser.display_info.AddFormat("""
        table(
          name.basename(),
          name.scope().segment(-3):label=ISSUER,
          name.scope().segment(-5):label=LOCATION,
          revocation_details.yesno(yes="REVOKED", no="ACTIVE"):label=REVOCATION_STATUS,
          certificate_description.subject_description.not_before_time():label=NOT_BEFORE,
          certificate_description.subject_description.not_after_time():label=NOT_AFTER)
        """)
    parser.display_info.AddTransforms({
        'not_before_time': text_utils.TransformNotBeforeTime,
        'not_after_time': text_utils.TransformNotAfterTime
    })

  def Run(self, args):
    client = privateca_base.GetClientInstance()
    messages = privateca_base.GetMessagesModule()

    client_filter, server_filter = filter_rewrite.BackendFilterRewrite(
    ).Rewrite(args.filter)
    log.info('original_filter=%r, client_filter=%r, server_filter=%r',
             args.filter, client_filter, server_filter)
    # Overwrite client filter used by gcloud.
    args.filter = client_filter
    parent = args.CONCEPTS.issuer.Parse()
    request = messages.PrivatecaProjectsLocationsCertificateAuthoritiesCertificatesListRequest(
        parent=parent.RelativeName(),
        orderBy=common_args.ParseSortByArg(args.sort_by),
        filter=server_filter)

    return list_pager.YieldFromList(
        client.projects_locations_certificateAuthorities_certificates,
        request,
        field='certificates',
        limit=args.limit,
        batch_size_attribute='pageSize',
        batch_size=args.page_size,
        get_field_func=response_utils.GetFieldAndLogUnreachable)
