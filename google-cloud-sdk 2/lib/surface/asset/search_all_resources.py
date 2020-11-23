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
"""Command to SearchAllResources."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.asset import client_util
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base


# pylint: disable=line-too-long
DETAILED_HELP = {
    'DESCRIPTION':
        """\
      Searches all Cloud resources within the specified scope, such as a
      project, folder or organization. The caller must be granted the
      ``cloudasset.assets.searchAllResources'' permission on the desired
      scope.
      """,
    'EXAMPLES':
        """\
      To search all the Cloud resources whose full resource names contain the
      ``xyz'' substring, within ``organizations/123456'', ensure the caller has
      been granted the ``cloudasset.assets.searchAllResources'' permission on
      the organization and run:

        $ {command} --scope='organizations/123456' --query='name:*xyz*'
      """
}


def AddScopeArgument(parser):
  parser.add_argument(
      '--scope',
      metavar='SCOPE',
      required=False,
      help=("""\
        A scope can be a project, a folder, or an organization. The search is
        limited to the Cloud resources within this scope. The caller must be
        granted the ``cloudasset.assets.searchAllResources'' permission on
        the desired scope. If not specified, the [configured project property](https://cloud.google.com//sdk/docs/configurations#setting_configuration_properties)
        will be used. To find the configured project, run:
        ```gcloud config get-value project```. To change the setting, run:
        ```gcloud config set project PROJECT_ID```.

        The allowed values are:

          * ```projects/{PROJECT_ID}``` (e.g., ``projects/foo-bar'')
          * ```projects/{PROJECT_NUMBER}``` (e.g., ``projects/12345678'')
          * ```folders/{FOLDER_NUMBER}``` (e.g., ``folders/1234567'')
          * ```organizations/{ORGANIZATION_NUMBER}``` (e.g. ``organizations/123456'')
        """))


def AddQueryArgument(parser):
  parser.add_argument(
      '--query',
      metavar='QUERY',
      required=False,
      help=("""\
        The query statement. See [how to construct a
        query](https://cloud.google.com/asset-inventory/docs/searching-resources#how_to_construct_a_query)
        for more details. If not specified or empty, it will search all the
        resources within the specified ```scope```.

        Examples:

        * ```name:Important``` to find Cloud resources whose name contains
          ``Important'' as a word.
        * ```displayName:Impor*``` to find Cloud resources whose display
           name contains ``Impor'' as a prefix.
        * ```description:*por*``` to find Cloud resources whose description
          contains ``por'' as a substring.
        * ```location:us-west*``` to find Cloud resources whose location is
          prefixed with ``us-west''.
        * ```labels:prod``` to find Cloud resources whose labels contain
          ``prod'' as a key or value.
        * ```labels.env:prod``` to find Cloud resources that have a label
          ``env'' and its value is ``prod''.
        * ```labels.env:*``` to find Cloud resources that have a label
          ``env''.
        * ```Important``` to find Cloud resources that contain ``Important''
          as a word in any of the searchable fields.
        * ```Impor*``` to find Cloud resources that contain ``Impor'' as a
          prefix in any of the searchable fields.
        * ```*por*``` to find Cloud resources that contain ``por'' as a
          substring in any of the searchable fields.
        * ```Important location:(us-west1 OR global)``` to find
          Cloud resources that contain ``Important'' as a word in any of the
          searchable fields and are also located in the ``us-west1'' region or
          the ``global'' location.
        """))


def AddAssetTypesArgument(parser):
  parser.add_argument(
      '--asset-types',
      metavar='ASSET_TYPES',
      type=arg_parsers.ArgList(),
      default=[],
      help=("""\
        A list of asset types to search. If not specified or empty, it will
        search all the [searchable asset types](https://cloud.google.com/asset-inventory/docs/supported-asset-types#searchable_asset_types).
        Example: ``cloudresourcemanager.googleapis.com/Project,compute.googleapis.com/Instance''
        to search project and VM instance resources.
        """))


def AddOrderByArgument(parser):
  parser.add_argument(
      '--order-by',
      metavar='ORDER_BY',
      required=False,
      help=("""\
        A comma-separated list of fields specifying the sorting order of the
        results. The default order is ascending. Add `` DESC'' after the field
        name to indicate descending order. Redundant space characters are
        ignored. Example: ``location DESC, name''. Only string fields in the
        response are sortable, including `name`, `displayName`, `description`
        and `location`.

        Both ```--order-by``` and ```--sort-by``` flags can be used to sort the
        output, with the following differences:

        * The ```--order-by``` flag performs server-side sorting (better
          performance), while the ```--sort-by``` flag performs client-side
          sorting.
        * The ```--sort-by``` flag supports all the fields in the output, while
          the ```--order-by``` flag only supports limited fields as shown above.
        """))


# pylint: enable=line-too-long


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class SearchAllResourcesBeta(base.ListCommand):
  """Searches all Cloud resources within the specified accessible scope, such as a project, folder or organization."""

  detailed_help = DETAILED_HELP

  @staticmethod
  def Args(parser):
    AddScopeArgument(parser)
    AddQueryArgument(parser)
    AddAssetTypesArgument(parser)
    AddOrderByArgument(parser)
    base.URI_FLAG.RemoveFromParser(parser)

  def Run(self, args):
    client = client_util.AssetSearchClient(client_util.V1P1BETA1_API_VERSION)
    return client.SearchAllResources(args)


@base.ReleaseTracks(base.ReleaseTrack.GA)
class SearchAllResources(SearchAllResourcesBeta):
  """Searches all Cloud resources within the specified accessible scope, such as a project, folder or organization."""

  def Run(self, args):
    client = client_util.AssetSearchClient(client_util.DEFAULT_API_VERSION)
    return client.SearchAllResources(args)
