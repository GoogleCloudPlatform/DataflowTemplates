# -*- coding: utf-8 -*- #
# Copyright 2018 Google LLC. All Rights Reserved.
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
"""Shared utilities for access the CloudAsset API client."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding
from apitools.base.py import exceptions as api_exceptions
from apitools.base.py import list_pager

from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.api_lib.util import exceptions
from googlecloudsdk.calliope import exceptions as gcloud_exceptions
from googlecloudsdk.command_lib.asset import utils as asset_utils
from googlecloudsdk.command_lib.util.args import repeated
from googlecloudsdk.core import exceptions as core_exceptions
from googlecloudsdk.core import log
from googlecloudsdk.core.credentials import http
from googlecloudsdk.core.util import encoding as core_encoding
from googlecloudsdk.core.util import times

import six
from six.moves import http_client as httplib

API_NAME = 'cloudasset'
DEFAULT_API_VERSION = 'v1'
V1P1BETA1_API_VERSION = 'v1p1beta1'
V1P4ALPHA1_API_VERSION = 'v1p4alpha1'
V1P4BETA1_API_VERSION = 'v1p4beta1'
V1P5BETA1_API_VERSION = 'v1p5beta1'
_HEADERS = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'X-HTTP-Method-Override': 'GET'
}
_HTTP_ERROR_FORMAT = ('HTTP request failed with status code {}. '
                      'Response content: {}')
# A dictionary that captures version differences for IAM Policy Analyzer.
_IAM_POLICY_ANALYZER_VERSION_DICT = {
    V1P4ALPHA1_API_VERSION: {
        'resource_selector': 'resourceSelector',
        'identity_selector': 'identitySelector',
        'access_selector': 'accessSelector',
        'options': 'options',
    },
    V1P4BETA1_API_VERSION: {
        'resource_selector': 'analysisQuery.resourceSelector',
        'identity_selector': 'analysisQuery.identitySelector',
        'access_selector': 'analysisQuery.accessSelector',
        'options': 'options',
        'execution_timeout': 'options.executionTimeout',
    },
    DEFAULT_API_VERSION: {
        'resource_selector': 'analysisQuery.resourceSelector',
        'identity_selector': 'analysisQuery.identitySelector',
        'access_selector': 'analysisQuery.accessSelector',
        'options': 'analysisQuery.options',
        'execution_timeout': 'executionTimeout',
    },
}


class MessageDecodeError(core_exceptions.Error):
  """Error raised when a failure to decode a message occurs."""


def GetMessages(version=DEFAULT_API_VERSION):
  """Import and return the cloudasset messages module.

  Args:
    version: the API version

  Returns:
    cloudasset message module.
  """
  return apis.GetMessagesModule(API_NAME, version)


def GetClient(version=DEFAULT_API_VERSION):
  """Import and return the cloudasset client module.

  Args:
    version: the API version

  Returns:
    cloudasset API client module.
  """
  return apis.GetClientInstance(API_NAME, version)


def ContentTypeTranslation(content_type):
  """Translate content type from gcloud format to API format.

  Args:
    content_type: the gcloud format of content_type

  Returns:
    cloudasset API format of content_type.
  """
  if content_type == 'resource':
    return 'RESOURCE'
  if content_type == 'iam-policy':
    return 'IAM_POLICY'
  if content_type == 'org-policy':
    return 'ORG_POLICY'
  if content_type == 'access-policy':
    return 'ACCESS_POLICY'
  if content_type == 'os-inventory':
    return 'OS_INVENTORY'
  return 'CONTENT_TYPE_UNSPECIFIED'


def PartitionKeyTranslation(partition_key):
  if partition_key == 'read-time':
    return 'READ_TIME'
  if partition_key == 'request-time':
    return 'REQUEST_TIME'
  return 'PARTITION_KEY_UNSPECIFIED'


def MakeGetAssetsHistoryHttpRequests(args, api_version=DEFAULT_API_VERSION):
  """Manually make the get assets history request."""
  http_client = http.Http()
  query_params = [
      ('assetNames', asset_name) for asset_name in args.asset_names or []
  ]
  query_params.extend([
      ('contentType', ContentTypeTranslation(args.content_type)),
      ('readTimeWindow.startTime', times.FormatDateTime(args.start_time))
  ])
  if args.IsSpecified('end_time'):
    query_params.extend([('readTimeWindow.endTime',
                          times.FormatDateTime(args.end_time))])
  parent = asset_utils.GetParentNameForGetHistory(args.organization,
                                                  args.project)
  endpoint = apis.GetEffectiveApiEndpoint(API_NAME, api_version)
  url = '{0}{1}/{2}:{3}'.format(endpoint, api_version, parent,
                                'batchGetAssetsHistory')
  encoded_query_params = six.moves.urllib.parse.urlencode(query_params)
  response, raw_content = http_client.request(
      uri=url, headers=_HEADERS, method='POST', body=encoded_query_params)

  content = core_encoding.Decode(raw_content)

  if int(response['status']) != httplib.OK:
    http_error = api_exceptions.HttpError(response, content, url)
    raise exceptions.HttpException(http_error)

  response_message_class = GetMessages(
      api_version).BatchGetAssetsHistoryResponse
  try:
    history_response = encoding.JsonToMessage(response_message_class, content)
  except ValueError as e:
    err_msg = ('Failed receiving proper response from server, cannot'
               'parse received assets. Error details: ' + six.text_type(e))
    raise MessageDecodeError(err_msg)

  for asset in history_response.assets:
    yield asset


def _RenderAnalysisforAnalyzeIamPolicy(analysis):
  """Renders the analysis query and results of the AnalyzeIamPolicy request."""

  for analysis_result in analysis.analysisResults:
    entry = {}

    policy = {
        'attachedResource': analysis_result.attachedResourceFullName,
        'binding': analysis_result.iamBinding,
    }
    entry['policy'] = policy

    entry['ACLs'] = []
    for acl in analysis_result.accessControlLists:
      acls = {}
      acls['identities'] = analysis_result.identityList.identities
      acls['accesses'] = acl.accesses
      acls['resources'] = acl.resources
      entry['ACLs'].append(acls)

    yield entry


def _RenderResponseforAnalyzeIamPolicy(response,
                                       analyze_service_account_impersonation):
  """Renders the response of the AnalyzeIamPolicy request."""

  if response.fullyExplored:
    msg = 'Your analysis request is fully explored. '
  else:
    msg = ('Your analysis request is NOT fully explored. You can use the '
           '--show-response option to see the unexplored part. ')

  has_results = False
  if response.mainAnalysis.analysisResults:
    has_results = True
  if (not has_results) and analyze_service_account_impersonation:
    for sa_impersonation_analysis in response.serviceAccountImpersonationAnalysis:
      if sa_impersonation_analysis.analysisResults:
        has_results = True
        break

  if not has_results:
    msg += 'No matching ACL is found.'
  else:
    msg += ('The ACLs matching your requests are listed per IAM policy binding'
            ', so there could be duplications.')

  for entry in _RenderAnalysisforAnalyzeIamPolicy(response.mainAnalysis):
    yield entry

  if analyze_service_account_impersonation:
    for analysis in response.serviceAccountImpersonationAnalysis:
      title = {
          'Service Account Impersonation Analysis Query': analysis.analysisQuery
      }
      yield title
      for entry in _RenderAnalysisforAnalyzeIamPolicy(analysis):
        yield entry

  log.status.Print(msg)


def MakeAnalyzeIamPolicyHttpRequests(args, api_version=V1P4ALPHA1_API_VERSION):
  """Manually make the analyze IAM policy request."""
  http_client = http.Http()

  if api_version == V1P4ALPHA1_API_VERSION:
    folder = None
    project = None
  else:
    folder = args.folder
    project = args.project

  parent = asset_utils.GetParentNameForAnalyzeIamPolicy(args.organization,
                                                        project, folder)
  endpoint = apis.GetEffectiveApiEndpoint(API_NAME, api_version)
  url = '{0}{1}/{2}:{3}'.format(endpoint, api_version, parent,
                                'analyzeIamPolicy')

  params = []
  if args.IsSpecified('full_resource_name'):
    params.extend([
        (_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['resource_selector'] +
         '.fullResourceName', args.full_resource_name)
    ])

  if args.IsSpecified('identity'):
    params.extend([
        (_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['identity_selector'] +
         '.identity', args.identity)
    ])

  if args.IsSpecified('roles'):
    params.extend([
        (_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['access_selector'] +
         '.roles', r) for r in args.roles
    ])
  if args.IsSpecified('permissions'):
    params.extend([
        (_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['access_selector'] +
         '.permissions', p) for p in args.permissions
    ])

  if args.expand_groups:
    params.extend([(_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['options'] +
                    '.expandGroups', args.expand_groups)])
  if args.expand_resources:
    params.extend([(_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['options'] +
                    '.expandResources', args.expand_resources)])
  if args.expand_roles:
    params.extend([(_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['options'] +
                    '.expandRoles', args.expand_roles)])

  if args.output_resource_edges:
    if (api_version == V1P4BETA1_API_VERSION or
        api_version == DEFAULT_API_VERSION) and (not args.show_response):
      raise gcloud_exceptions.InvalidArgumentException(
          '--output-resource-edges',
          'Must be set together with --show-response to take effect.')
    params.extend([(_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['options'] +
                    '.outputResourceEdges', args.output_resource_edges)])
  if args.output_group_edges:
    if (api_version == V1P4BETA1_API_VERSION or
        api_version == DEFAULT_API_VERSION) and (not args.show_response):
      raise gcloud_exceptions.InvalidArgumentException(
          '--output-group-edges',
          'Must be set together with --show-response to take effect.')
    params.extend([(_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['options'] +
                    '.outputGroupEdges', args.output_group_edges)])
  if api_version == V1P4ALPHA1_API_VERSION and args.IsSpecified(
      'output_partial_result_before_timeout'):
    params.extend([('options.outputPartialResultBeforeTimeout',
                    args.output_partial_result_before_timeout)])
  if (api_version == V1P4BETA1_API_VERSION or api_version == DEFAULT_API_VERSION
     ) and args.IsSpecified('execution_timeout'):
    params.extend([
        (_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['execution_timeout'],
         six.text_type(args.execution_timeout) + 's')
    ])

  if (api_version == V1P4BETA1_API_VERSION or api_version == DEFAULT_API_VERSION
     ) and args.analyze_service_account_impersonation:
    params.extend([(_IAM_POLICY_ANALYZER_VERSION_DICT[api_version]['options'] +
                    '.analyzeServiceAccountImpersonation',
                    args.analyze_service_account_impersonation)])

  encoded_params = six.moves.urllib.parse.urlencode(params)
  response, raw_content = http_client.request(
      uri=url, headers=_HEADERS, method='POST', body=encoded_params)

  content = core_encoding.Decode(raw_content)

  if int(response['status']) != httplib.OK:
    http_error = api_exceptions.HttpError(response, content, url)
    raise exceptions.HttpException(http_error)

  response_message_class = GetMessages(api_version).AnalyzeIamPolicyResponse
  try:
    response = encoding.JsonToMessage(response_message_class, content)
    if (api_version == V1P4BETA1_API_VERSION or
        api_version == DEFAULT_API_VERSION) and (not args.show_response):
      return _RenderResponseforAnalyzeIamPolicy(
          response, args.analyze_service_account_impersonation)
    else:
      return response
  except ValueError as e:
    err_msg = ('Failed receiving proper response from server, cannot'
               'parse received assets. Error details: ' + six.text_type(e))
    raise MessageDecodeError(err_msg)


class AssetExportClient(object):
  """Client for export asset."""

  def __init__(self, parent, api_version=DEFAULT_API_VERSION):
    self.parent = parent
    self.message_module = GetMessages(api_version)
    self.service = GetClient(api_version).v1

  def Export(self, args):
    """Export assets with the asset export method."""
    content_type = ContentTypeTranslation(args.content_type)
    content_type = getattr(
        self.message_module.ExportAssetsRequest.ContentTypeValueValuesEnum,
        content_type)
    partition_key = PartitionKeyTranslation(args.partition_key)
    partition_key = getattr(
        self.message_module.PartitionSpec.PartitionKeyValueValuesEnum,
        partition_key)
    if args.output_path or args.output_path_prefix:
      output_config = self.message_module.OutputConfig(
          gcsDestination=self.message_module.GcsDestination(
              uri=args.output_path, uriPrefix=args.output_path_prefix))
    else:
      source_ref = args.CONCEPTS.bigquery_table.Parse()
      output_config = self.message_module.OutputConfig(
          bigqueryDestination=self.message_module.BigQueryDestination(
              dataset='projects/' + source_ref.projectId + '/datasets/' +
              source_ref.datasetId,
              table=source_ref.tableId,
              force=args.force_,
              partitionSpec=self.message_module.PartitionSpec(
                  partitionKey=partition_key),
              separateTablesPerAssetType=args.per_type_))
    snapshot_time = None
    if args.snapshot_time:
      snapshot_time = times.FormatDateTime(args.snapshot_time)
    export_assets_request = self.message_module.ExportAssetsRequest(
        assetTypes=args.asset_types,
        contentType=content_type,
        outputConfig=output_config,
        readTime=snapshot_time)
    request_message = self.message_module.CloudassetExportAssetsRequest(
        parent=self.parent, exportAssetsRequest=export_assets_request)
    operation = self.service.ExportAssets(request_message)
    return operation


class AssetFeedClient(object):
  """Client for asset feed."""

  def __init__(self, parent, api_version=DEFAULT_API_VERSION):
    self.parent = parent
    self.message_module = GetMessages(api_version)
    self.service = GetClient(api_version).feeds

  def Create(self, args):
    """Create a feed."""
    content_type = ContentTypeTranslation(args.content_type)
    content_type = getattr(self.message_module.Feed.ContentTypeValueValuesEnum,
                           content_type)
    feed_output_config = self.message_module.FeedOutputConfig(
        pubsubDestination=self.message_module.PubsubDestination(
            topic=args.pubsub_topic))
    feed_condition = self.message_module.Expr(
        expression=args.condition_expression,
        title=args.condition_title,
        description=args.condition_description)
    feed = self.message_module.Feed(
        assetNames=args.asset_names,
        assetTypes=args.asset_types,
        contentType=content_type,
        feedOutputConfig=feed_output_config,
        condition=feed_condition)
    create_feed_request = self.message_module.CreateFeedRequest(
        feed=feed, feedId=args.feed)
    request_message = self.message_module.CloudassetFeedsCreateRequest(
        parent=self.parent, createFeedRequest=create_feed_request)
    return self.service.Create(request_message)

  def Describe(self, args):
    """Describe a feed."""
    request_message = self.message_module.CloudassetFeedsGetRequest(
        name='{}/feeds/{}'.format(self.parent, args.feed))
    return self.service.Get(request_message)

  def Delete(self, args):
    """Delete a feed."""
    request_message = self.message_module.CloudassetFeedsDeleteRequest(
        name='{}/feeds/{}'.format(self.parent, args.feed))
    self.service.Delete(request_message)

  def List(self):
    """List feeds under a parent."""
    request_message = self.message_module.CloudassetFeedsListRequest(
        parent=self.parent)
    return self.service.List(request_message)

  def Update(self, args):
    """Update a feed."""
    update_masks = []
    content_type = ContentTypeTranslation(args.content_type)
    content_type = getattr(self.message_module.Feed.ContentTypeValueValuesEnum,
                           content_type)
    feed_name = '{}/feeds/{}'.format(self.parent, args.feed)
    if args.content_type or args.clear_content_type:
      update_masks.append('content_type')
    if args.pubsub_topic:
      update_masks.append('feed_output_config.pubsub_destination.topic')
    if args.condition_expression or args.clear_condition_expression:
      update_masks.append('condition.expression')
    if args.condition_title or args.clear_condition_title:
      update_masks.append('condition.title')
    if args.condition_description or args.clear_condition_description:
      update_masks.append('condition.description')
    asset_names, asset_types = self.UpdateAssetNamesAndTypes(
        args, feed_name, update_masks)
    update_mask = ','.join(update_masks)
    feed_output_config = self.message_module.FeedOutputConfig(
        pubsubDestination=self.message_module.PubsubDestination(
            topic=args.pubsub_topic))
    feed_condition = self.message_module.Expr(
        expression=args.condition_expression,
        title=args.condition_title,
        description=args.condition_description)
    feed = self.message_module.Feed(
        assetNames=asset_names,
        assetTypes=asset_types,
        contentType=content_type,
        feedOutputConfig=feed_output_config,
        condition=feed_condition)
    update_feed_request = self.message_module.UpdateFeedRequest(
        feed=feed, updateMask=update_mask)
    request_message = self.message_module.CloudassetFeedsPatchRequest(
        name=feed_name, updateFeedRequest=update_feed_request)
    return self.service.Patch(request_message)

  def UpdateAssetNamesAndTypes(self, args, feed_name, update_masks):
    """Get Updated assetNames and assetTypes."""
    feed = self.service.Get(
        self.message_module.CloudassetFeedsGetRequest(name=feed_name))
    asset_names = repeated.ParsePrimitiveArgs(args, 'asset_names',
                                              lambda: feed.assetNames)
    if asset_names is not None:
      update_masks.append('asset_names')
    else:
      asset_names = []
    asset_types = repeated.ParsePrimitiveArgs(args, 'asset_types',
                                              lambda: feed.assetTypes)
    if asset_types is not None:
      update_masks.append('asset_types')
    else:
      asset_types = []
    return asset_names, asset_types


class AssetSearchClient(object):
  """Client for search assets."""

  _DEFAULT_PAGE_SIZE = 20

  def __init__(self, api_version):
    self.message_module = GetMessages(api_version)
    if api_version == V1P1BETA1_API_VERSION:
      self.resource_service = GetClient(api_version).resources
      self.search_all_resources_method = 'SearchAll'
      self.search_all_resources_request = self.message_module.CloudassetResourcesSearchAllRequest
      self.policy_service = GetClient(api_version).iamPolicies
      self.search_all_iam_policies_method = 'SearchAll'
      self.search_all_iam_policies_request = self.message_module.CloudassetIamPoliciesSearchAllRequest
    else:
      self.resource_service = GetClient(api_version).v1
      self.search_all_resources_method = 'SearchAllResources'
      self.search_all_resources_request = self.message_module.CloudassetSearchAllResourcesRequest
      self.policy_service = GetClient(api_version).v1
      self.search_all_iam_policies_method = 'SearchAllIamPolicies'
      self.search_all_iam_policies_request = self.message_module.CloudassetSearchAllIamPoliciesRequest

  def SearchAllResources(self, args):
    """Calls SearchAllResources method."""
    request = self.search_all_resources_request(
        scope=asset_utils.GetDefaultScopeIfEmpty(args),
        query=args.query,
        assetTypes=args.asset_types,
        orderBy=args.order_by)
    return list_pager.YieldFromList(
        self.resource_service,
        request,
        method=self.search_all_resources_method,
        field='results',
        batch_size=args.page_size or self._DEFAULT_PAGE_SIZE,
        batch_size_attribute='pageSize',
        current_token_attribute='pageToken',
        next_token_attribute='nextPageToken')

  def SearchAllIamPolicies(self, args):
    """Calls SearchAllIamPolicies method."""
    request = self.search_all_iam_policies_request(
        scope=asset_utils.GetDefaultScopeIfEmpty(args), query=args.query)
    return list_pager.YieldFromList(
        self.policy_service,
        request,
        method=self.search_all_iam_policies_method,
        field='results',
        batch_size=args.page_size or self._DEFAULT_PAGE_SIZE,
        batch_size_attribute='pageSize',
        current_token_attribute='pageToken',
        next_token_attribute='nextPageToken')


class AssetListClient(object):
  """Client for list assets."""

  def __init__(self, parent, api_version=V1P5BETA1_API_VERSION):
    self.parent = parent
    self.message_module = GetMessages(api_version)
    self.service = GetClient(api_version).assets

  def List(self, args):
    """List assets with the asset list method."""
    snapshot_time = None
    if args.snapshot_time:
      snapshot_time = times.FormatDateTime(args.snapshot_time)
    content_type = ContentTypeTranslation(args.content_type)
    list_assets_request = self.message_module.CloudassetAssetsListRequest(
        parent=self.parent,
        contentType=getattr(
            self.message_module.CloudassetAssetsListRequest
            .ContentTypeValueValuesEnum, content_type),
        assetTypes=args.asset_types,
        readTime=snapshot_time)
    return list_pager.YieldFromList(
        self.service,
        list_assets_request,
        field='assets',
        limit=args.limit,
        batch_size=args.page_size,
        batch_size_attribute='pageSize',
        current_token_attribute='pageToken',
        next_token_attribute='nextPageToken')


class AssetOperationClient(object):
  """Client for operations."""

  def __init__(self, api_version=DEFAULT_API_VERSION):
    self.service = GetClient(api_version).operations
    self.message = GetMessages(api_version).CloudassetOperationsGetRequest

  def Get(self, name):
    request = self.message(name=name)
    return self.service.Get(request)


class IamPolicyAnalysisLongrunningClient(object):
  """Client for analyze IAM policy asynchronously."""

  def __init__(self, api_version=DEFAULT_API_VERSION):
    self.message_module = GetMessages(api_version)
    if api_version == V1P4BETA1_API_VERSION:
      self.service = GetClient(api_version).v1p4beta1
    else:
      self.service = GetClient(api_version).v1

  def Analyze(self, scope, args, api_version=DEFAULT_API_VERSION):
    """Analyze IAM Policy asynchronously."""
    analysis_query = self.message_module.IamPolicyAnalysisQuery()
    if api_version == V1P4BETA1_API_VERSION:
      analysis_query.parent = scope
    else:
      analysis_query.scope = scope
    if args.IsSpecified('full_resource_name'):
      analysis_query.resourceSelector = self.message_module.ResourceSelector(
          fullResourceName=args.full_resource_name)
    if args.IsSpecified('identity'):
      analysis_query.identitySelector = self.message_module.IdentitySelector(
          identity=args.identity)
    if args.IsSpecified('roles') or args.IsSpecified('permissions'):
      analysis_query.accessSelector = self.message_module.AccessSelector()
      if args.IsSpecified('roles'):
        analysis_query.accessSelector.roles.extend(args.roles)
      if args.IsSpecified('permissions'):
        analysis_query.accessSelector.permissions.extend(args.permissions)

    output_config = None
    if api_version == V1P4BETA1_API_VERSION:
      output_config = self.message_module.IamPolicyAnalysisOutputConfig(
          gcsDestination=self.message_module.GcsDestination(
              uri=args.output_path))
    else:
      if args.gcs_output_path:
        output_config = self.message_module.IamPolicyAnalysisOutputConfig(
            gcsDestination=self.message_module.GoogleCloudAssetV1GcsDestination(
                uri=args.gcs_output_path))
      else:
        output_config = self.message_module.IamPolicyAnalysisOutputConfig(
            bigqueryDestination=self.message_module
            .GoogleCloudAssetV1BigQueryDestination(
                dataset=args.bigquery_dataset,
                tablePrefix=args.bigquery_table_prefix))
        if args.IsSpecified('bigquery_partition_key'):
          output_config.bigqueryDestination.partitionKey = getattr(
              self.message_module.GoogleCloudAssetV1BigQueryDestination
              .PartitionKeyValueValuesEnum, args.bigquery_partition_key)
        if args.IsSpecified('bigquery_write_disposition'):
          output_config.bigqueryDestination.writeDisposition = args.bigquery_write_disposition

    options = self.message_module.Options()
    if args.expand_groups:
      options.expandGroups = args.expand_groups
    if args.expand_resources:
      options.expandResources = args.expand_resources
    if args.expand_roles:
      options.expandRoles = args.expand_roles
    if args.output_resource_edges:
      options.outputResourceEdges = args.output_resource_edges
    if args.output_group_edges:
      options.outputGroupEdges = args.output_group_edges
    if args.analyze_service_account_impersonation:
      options.analyzeServiceAccountImpersonation = args.analyze_service_account_impersonation

    operation = None
    if api_version == V1P4BETA1_API_VERSION:
      request = self.message_module.ExportIamPolicyAnalysisRequest(
          analysisQuery=analysis_query,
          options=options,
          outputConfig=output_config)
      request_message = self.message_module.CloudassetExportIamPolicyAnalysisRequest(
          parent=scope, exportIamPolicyAnalysisRequest=request)
      operation = self.service.ExportIamPolicyAnalysis(request_message)
    else:
      analysis_query.options = options
      request = self.message_module.AnalyzeIamPolicyLongrunningRequest(
          analysisQuery=analysis_query, outputConfig=output_config)
      request_message = self.message_module.CloudassetAnalyzeIamPolicyLongrunningRequest(
          scope=scope, analyzeIamPolicyLongrunningRequest=request)
      operation = self.service.AnalyzeIamPolicyLongrunning(request_message)

    return operation
