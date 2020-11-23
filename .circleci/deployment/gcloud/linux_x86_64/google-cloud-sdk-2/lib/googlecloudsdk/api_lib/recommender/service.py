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
"""recommender API recommendations service."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.util import apis

RECOMMENDER_API_NAME = 'recommender'

RECOMMENDER_API_VERSION = 'v1alpha2'


def RecommenderClient():
  return apis.GetClientInstance(RECOMMENDER_API_NAME, RECOMMENDER_API_VERSION)


def RecommenderMessages():
  """Returns the messages module for the Resource Settings service."""
  return apis.GetMessagesModule(RECOMMENDER_API_NAME, RECOMMENDER_API_VERSION)


def BillingAccountsRecommenderRecommendationsService():
  """Returns the service class for the Billing Account recommendations."""
  client = RecommenderClient()
  return client.billingAccounts_locations_recommenders_recommendations


def ProjectsRecommenderRecommendationsService():
  """Returns the service class for the Project recommendations."""
  client = RecommenderClient()
  return client.projects_locations_recommenders_recommendations


def FoldersRecommenderRecommendationsService():
  """Returns the service class for the Folders recommendations."""
  client = RecommenderClient()
  return client.folders_locations_recommenders_recommendations


def OrganizationsRecommenderRecommendationsService():
  """Returns the service class for the Organization recommendations."""
  client = RecommenderClient()
  return client.organizations_locations_recommenders_recommendations


def BillingAccountsInsightTypeInsightsService():
  """Returns the service class for the Billing Account insights."""
  client = RecommenderClient()
  return client.billingAccounts_locations_insightTypes_insights


def ProjectsInsightTypeInsightsService():
  """Returns the service class for the Project insights."""
  client = RecommenderClient()
  return client.projects_locations_insightTypes_insights


def FoldersInsightTypeInsightsService():
  """Returns the service class for the Folders insights."""
  client = RecommenderClient()
  return client.folders_locations_insightTypes_insights


def OrganizationsInsightTypeInsightsService():
  """Returns the service class for the Organization insights."""
  client = RecommenderClient()
  return client.organizations_locations_insightTypes_insights
