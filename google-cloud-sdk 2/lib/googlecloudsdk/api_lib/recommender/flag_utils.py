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
"""recommender API utlities."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.recommender import service as recommender_service
import six


def GetServiceFromArgs(args, is_insight_api):
  """Returns the service from the user-specified arguments.

  Args:
    args: argparse.Namespace, An object that contains the values for the
      arguments specified in the Args method.
    is_insight_api: boolean value sepcify whether this is a insight api,
      otherwise will return a recommendation service api
  """
  if is_insight_api:
    if args.project:
      service = recommender_service.ProjectsInsightTypeInsightsService()
    elif args.billing_account:
      service = recommender_service.BillingAccountsInsightTypeInsightsService()
    elif args.folder:
      service = recommender_service.FoldersInsightTypeInsightsService()
    elif args.organization:
      service = recommender_service.OrganizationsInsightTypeInsightsService()
  else:
    if args.project:
      service = recommender_service.ProjectsRecommenderRecommendationsService()
    elif args.billing_account:
      service = recommender_service.BillingAccountsRecommenderRecommendationsService(
      )
    elif args.folder:
      service = recommender_service.FoldersRecommenderRecommendationsService()
    elif args.organization:
      service = recommender_service.OrganizationsRecommenderRecommendationsService(
      )

  return service


def GetListRequestFromArgs(args, parent_resource, is_insight_api):
  """Returns the get_request from the user-specified arguments.

  Args:
    args: argparse.Namespace, An object that contains the values for the
      arguments specified in the Args method.
    parent_resource: resource url string, the flags are already defined in
      argparse namespace, including project, billing-account, folder,
      organization, etc.
    is_insight_api: boolean value specifying whether this is a insight api,
      otherwise treat as a recommender service api and return related list
      request message.
  """

  messages = recommender_service.RecommenderMessages()
  if is_insight_api:
    if args.project:
      get_request = messages.RecommenderProjectsLocationsInsightTypesInsightsListRequest(
          parent=parent_resource)
    elif args.billing_account:
      get_request = messages.RecommenderBillingAccountsLocationsInsightTypesInsightsListRequest(
          parent=parent_resource)
    elif args.organization:
      get_request = messages.RecommenderOrganizationsLocationsInsightTypesInsightsListRequest(
          parent=parent_resource)
    elif args.folder:
      get_request = messages.RecommenderFoldersLocationsInsightTypesInsightsListRequest(
          parent=parent_resource)
  else:
    if args.project:
      get_request = messages.RecommenderProjectsLocationsRecommendersRecommendationsListRequest(
          parent=parent_resource)
    elif args.billing_account:
      get_request = messages.RecommenderBillingAccountsLocationsRecommendersRecommendationsListRequest(
          parent=parent_resource)
    elif args.organization:
      get_request = messages.RecommenderOrganizationsLocationsRecommendersRecommendationsListRequest(
          parent=parent_resource)
    elif args.folder:
      get_request = messages.RecommenderFoldersLocationsRecommendersRecommendationsListRequest(
          parent=parent_resource)

  return get_request


def GetMarkActiveRequestFromArgs(args, parent_resource, is_insight_api):
  """Returns the mark active request from the user-specified arguments.

  Args:
    args: argparse.Namespace, An object that contains the values for the
      arguments specified in the Args method.
    parent_resource: resource url string, the flags are already defined in
      argparse namespace, including project, billing-account, folder,
      organization, etc.
    is_insight_api: boolean value specifying whether this is a insight api,
      otherwise treat as a recommender service api and return related list
      request message.
  """

  messages = recommender_service.RecommenderMessages()
  if is_insight_api:
    mark_insight_active_request = messages.GoogleCloudRecommenderV1alpha2MarkInsightActiveRequest(
        etag=args.etag)
    if args.project:
      request = messages.RecommenderProjectsLocationsInsightTypesInsightsMarkActiveRequest(
          googleCloudRecommenderV1alpha2MarkInsightActiveRequest=mark_insight_active_request,
          name=parent_resource)
    elif args.billing_account:
      request = messages.RecommenderBillingAccountsLocationsInsightTypesInsightsMarkActiveRequest(
          googleCloudRecommenderV1alpha2MarkInsightActiveRequest=mark_insight_active_request,
          name=parent_resource)
    elif args.organization:
      request = messages.RecommenderOrganizationsLocationsInsightTypesInsightsMarkActiveRequest(
          googleCloudRecommenderV1alpha2MarkInsightActiveRequest=mark_insight_active_request,
          name=parent_resource)
    elif args.folder:
      request = messages.RecommenderFoldersLocationsInsightTypesInsightsMarkActiveRequest(
          googleCloudRecommenderV1alpha2MarkInsightActiveRequest=mark_insight_active_request,
          name=parent_resource)
  else:
    mark_recommendation_active_request = messages.GoogleCloudRecommenderV1alpha2MarkRecommendationActiveRequest(
        etag=args.etag)
    if args.project:
      request = messages.RecommenderProjectsLocationsRecommendersRecommendationsMarkActiveRequest(
          googleCloudRecommenderV1alpha2MarkRecommendationActiveRequest=mark_recommendation_active_request,
          name=parent_resource)
    elif args.billing_account:
      request = messages.RecommenderBillingAccountsLocationsRecommendersRecommendationsMarkActiveRequest(
          googleCloudRecommenderV1alpha2MarkRecommendationActiveRequest=mark_recommendation_active_request,
          name=parent_resource)
    elif args.organization:
      request = messages.RecommenderOrganizationsLocationsRecommendersRecommendationsMarkActiveRequest(
          googleCloudRecommenderV1alpha2MarkRecommendationActiveRequest=mark_recommendation_active_request,
          name=parent_resource)
    elif args.folder:
      request = messages.RecommenderFoldersLocationsRecommendersRecommendationsMarkActiveRequest(
          googleCloudRecommenderV1alpha2MarkRecommendationActiveRequest=mark_recommendation_active_request,
          name=parent_resource)

  return request


def GetMarkClaimedRequestFromArgs(args, parent_resource):
  """Returns the mark_claimed_request from the user-specified arguments.

  Args:
    args: argparse.Namespace, An object that contains the values for the
      arguments specified in the Args method.
    parent_resource: resource location such as `organizations/123`
  """

  messages = recommender_service.RecommenderMessages()
  message_request_field = messages.GoogleCloudRecommenderV1alpha2MarkRecommendationClaimedRequest
  mark_recommendation_claimed_request = messages.GoogleCloudRecommenderV1alpha2MarkRecommendationClaimedRequest(
      etag=args.etag,
      stateMetadata=ParseStateMetadata(args.state_metadata,
                                       message_request_field))
  if args.project:
    mark_request = messages.RecommenderProjectsLocationsRecommendersRecommendationsMarkClaimedRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationClaimedRequest=mark_recommendation_claimed_request,
        name=parent_resource)
  elif args.billing_account:
    mark_request = messages.RecommenderBillingAccountsLocationsRecommendersRecommendationsMarkClaimedRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationClaimedRequest=mark_recommendation_claimed_request,
        name=parent_resource)
  elif args.organization:
    mark_request = messages.RecommenderOrganizationsLocationsRecommendersRecommendationsMarkClaimedRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationClaimedRequest=mark_recommendation_claimed_request,
        name=parent_resource)
  elif args.folder:
    mark_request = messages.RecommenderFoldersLocationsRecommendersRecommendationsMarkClaimedRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationClaimedRequest=mark_recommendation_claimed_request,
        name=parent_resource)

  return mark_request


def GetMarkFailedRequestFromArgs(args, parent_resource):
  """Returns the mark_failed_request from the user-specified arguments.

  Args:
    args: argparse.Namespace, An object that contains the values for the
      arguments specified in the Args method.
    parent_resource: resource location such as `organizations/123`
  """

  messages = recommender_service.RecommenderMessages()
  message_request_field = messages.GoogleCloudRecommenderV1alpha2MarkRecommendationFailedRequest
  mark_recommendation_failed_request = messages.GoogleCloudRecommenderV1alpha2MarkRecommendationFailedRequest(
      etag=args.etag,
      stateMetadata=ParseStateMetadata(args.state_metadata,
                                       message_request_field))
  if args.project:
    mark_request = messages.RecommenderProjectsLocationsRecommendersRecommendationsMarkFailedRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationFailedRequest=mark_recommendation_failed_request,
        name=parent_resource)
  elif args.billing_account:
    mark_request = messages.RecommenderBillingAccountsLocationsRecommendersRecommendationsMarkFailedRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationFailedRequest=mark_recommendation_failed_request,
        name=parent_resource)
  elif args.organization:
    mark_request = messages.RecommenderOrganizationsLocationsRecommendersRecommendationsMarkFailedRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationFailedRequest=mark_recommendation_failed_request,
        name=parent_resource)
  elif args.folder:
    mark_request = messages.RecommenderFoldersLocationsRecommendersRecommendationsMarkFailedRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationFailedRequest=mark_recommendation_failed_request,
        name=parent_resource)

  return mark_request


def GetMarkDismissedRequestFromArgs(args, parent_resource, is_insight_api):
  """Returns the mark dismissed request from the user-specified arguments.

  Args:
    args: argparse.Namespace, An object that contains the values for the
      arguments specified in the Args method.
    parent_resource: resource url string, the flags are already defined in
      argparse namespace, including project, billing-account, folder,
      organization, etc.
    is_insight_api: boolean value specifying whether this is a insight api,
      otherwise treat as a recommender service api and return related list
      request message.
  """

  messages = recommender_service.RecommenderMessages()
  if is_insight_api:
    mark_insight_dismissed_message = messages.GoogleCloudRecommenderV1alpha2MarkInsightDismissedRequest(
        etag=args.etag,
        recommendationChangeType=_GetRecommendationChangeType(
            args.recommendation_change_type))
    if args.project:
      request = messages.RecommenderProjectsLocationsInsightTypesInsightsMarkDismissedRequest(
          googleCloudRecommenderV1alpha2MarkInsightDismissedRequest=mark_insight_dismissed_message,
          name=parent_resource)
    elif args.billing_account:
      request = messages.RecommenderBillingAccountsLocationsInsightTypesInsightsMarkDismissedRequest(
          googleCloudRecommenderV1alpha2MarkInsightDismissedRequest=mark_insight_dismissed_message,
          name=parent_resource)
    elif args.organization:
      request = messages.RecommenderOrganizationsLocationsInsightTypesInsightsMarkDismissedRequest(
          googleCloudRecommenderV1alpha2MarkInsightDismissedRequest=mark_insight_dismissed_message,
          name=parent_resource)
    elif args.folder:
      request = messages.RecommenderFoldersLocationsInsightTypesInsightsMarkDismissedRequest(
          googleCloudRecommenderV1alpha2MarkInsightDismissedRequest=mark_insight_dismissed_message,
          name=parent_resource)
  else:
    mark_recommendation_dismissed_message = messages.GoogleCloudRecommenderV1alpha2MarkRecommendationDismissedRequest(
        etag=args.etag)
    if args.project:
      request = messages.RecommenderProjectsLocationsRecommendersRecommendationsMarkDismissedRequest(
          googleCloudRecommenderV1alpha2MarkRecommendationDismissedRequest=mark_recommendation_dismissed_message,
          name=parent_resource)
    elif args.billing_account:
      request = messages.RecommenderBillingAccountsLocationsRecommendersRecommendationsMarkDismissedRequest(
          googleCloudRecommenderV1alpha2MarkRecommendationDismissedRequest=mark_recommendation_dismissed_message,
          name=parent_resource)
    elif args.organization:
      request = messages.RecommenderOrganizationsLocationsRecommendersRecommendationsMarkDismissedRequest(
          googleCloudRecommenderV1alpha2MarkRecommendationDismissedRequest=mark_recommendation_dismissed_message,
          name=parent_resource)
    elif args.folder:
      request = messages.RecommenderFoldersLocationsRecommendersRecommendationsMarkDismissedRequest(
          googleCloudRecommenderV1alpha2MarkRecommendationDismissedRequest=mark_recommendation_dismissed_message,
          name=parent_resource)

  return request


def GetMarkAcceptedRequestFromArgs(args, parent_resource, is_insight_api):
  """Returns the mark accepted request.

  Args:
    args: argparse.Namespace, An object that contains the values for the
      arguments specified in the Args method.
    parent_resource: resource url string, the flags are already defined in
      argparse namespace, including project, billing-account, folder,
      organization, etc.
    is_insight_api: boolean value specifying whether this is a insight api,
      otherwise treat as a recommender service api and return related list
      request message.
  """

  messages = recommender_service.RecommenderMessages()
  if is_insight_api:
    message_request_field = messages.GoogleCloudRecommenderV1alpha2MarkInsightAcceptedRequest
    mark_insight_accepted_message = messages.GoogleCloudRecommenderV1alpha2MarkInsightAcceptedRequest(
        etag=args.etag,
        stateMetadata=ParseStateMetadata(args.state_metadata,
                                         message_request_field))
    if args.project:
      request = messages.RecommenderProjectsLocationsInsightTypesInsightsMarkAcceptedRequest(
          googleCloudRecommenderV1alpha2MarkInsightAcceptedRequest=mark_insight_accepted_message,
          name=parent_resource)
    elif args.billing_account:
      request = messages.RecommenderBillingAccountsLocationsInsightTypesInsightsMarkAcceptedRequest(
          googleCloudRecommenderV1alpha2MarkInsightAcceptedRequest=mark_insight_accepted_message,
          name=parent_resource)
    elif args.organization:
      request = messages.RecommenderOrganizationsLocationsInsightTypesInsightsMarkAcceptedRequest(
          googleCloudRecommenderV1alpha2MarkInsightAcceptedRequest=mark_insight_accepted_message,
          name=parent_resource)
    elif args.folder:
      request = messages.RecommenderFoldersLocationsInsightTypesInsightsMarkAcceptedRequest(
          googleCloudRecommenderV1alpha2MarkInsightAcceptedRequest=mark_insight_accepted_message,
          name=parent_resource)

  return request


def GetMarkSucceededRequestFromArgs(args, parent_resource):
  """Returns the mark_succeeded_request from the user-specified arguments.

  Args:
    args: argparse.Namespace, An object that contains the values for the
      arguments specified in the Args method.
    parent_resource: resource location such as `organizations/123`
  """

  messages = recommender_service.RecommenderMessages()
  message_request_field = messages.GoogleCloudRecommenderV1alpha2MarkRecommendationSucceededRequest
  mark_recommendation_succeeded_request = messages.GoogleCloudRecommenderV1alpha2MarkRecommendationSucceededRequest(
      etag=args.etag,
      stateMetadata=ParseStateMetadata(args.state_metadata,
                                       message_request_field))
  if args.project:
    mark_request = messages.RecommenderProjectsLocationsRecommendersRecommendationsMarkSucceededRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationSucceededRequest=mark_recommendation_succeeded_request,
        name=parent_resource)
  elif args.billing_account:
    mark_request = messages.RecommenderBillingAccountsLocationsRecommendersRecommendationsMarkSucceededRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationSucceededRequest=mark_recommendation_succeeded_request,
        name=parent_resource)
  elif args.organization:
    mark_request = messages.RecommenderOrganizationsLocationsRecommendersRecommendationsMarkSucceededRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationSucceededRequest=mark_recommendation_succeeded_request,
        name=parent_resource)
  elif args.folder:
    mark_request = messages.RecommenderFoldersLocationsRecommendersRecommendationsMarkSucceededRequest(
        googleCloudRecommenderV1alpha2MarkRecommendationSucceededRequest=mark_recommendation_succeeded_request,
        name=parent_resource)

  return mark_request


def ParseStateMetadata(metadata, message_request):
  """Parsing args to get full state metadata string.

  Args:
      metadata: dict, key-value pairs passed in from the --metadata flag.
      message_request: specific request field of message module

  Returns:
      A message object.
  """
  if not metadata:
    return None

  additional_properties = []

  metadata_value_msg = message_request.StateMetadataValue

  for key, value in six.iteritems(metadata):
    additional_properties.append(
        metadata_value_msg.AdditionalProperty(key=key, value=value))

  return metadata_value_msg(additionalProperties=additional_properties) or None


def _GetRecommendationChangeType(recommendation_change_type):
  """Get RecommendationChangeType enum value.

  Converts recommendation_change_type argument value to
  RecommendationChangeType enum value.

  Args:
    recommendation_change_type: recommendation_change_type flag value

  Returns:
    RecommendationChangeType enum value
  """
  if not recommendation_change_type:
    return None

  messages = recommender_service.RecommenderMessages()

  if recommendation_change_type.lower() == 'leave_unchanged':
    return messages.GoogleCloudRecommenderV1alpha2MarkInsightDismissedRequest.RecommendationChangeTypeValueValuesEnum(
        'LEAVE_RECOMMENDATIONS_UNCHANGED')
  elif recommendation_change_type.lower() == 'dismiss':
    return messages.GoogleCloudRecommenderV1alpha2MarkInsightDismissedRequest.RecommendationChangeTypeValueValuesEnum(
        'DISMISS_RECOMMENDATIONS')
  else:
    return None
