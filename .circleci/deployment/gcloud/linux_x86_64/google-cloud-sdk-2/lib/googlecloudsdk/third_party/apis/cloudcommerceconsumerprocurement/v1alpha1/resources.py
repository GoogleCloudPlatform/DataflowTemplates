# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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
"""Resource definitions for cloud platform apis."""

import enum


BASE_URL = 'https://cloudcommerceconsumerprocurement.googleapis.com/v1alpha1/'
DOCS_URL = 'https://cloud.google.com/marketplace/docs/'


class Collections(enum.Enum):
  """Collections for all supported apis."""

  BILLINGACCOUNTS = (
      'billingAccounts',
      'billingAccounts/{billingAccountsId}',
      {},
      ['billingAccountsId'],
      True
  )
  BILLINGACCOUNTS_ACCOUNTS = (
      'billingAccounts.accounts',
      '{+name}',
      {
          '':
              'billingAccounts/{billingAccountsId}/accounts/{accountsId}',
      },
      ['name'],
      True
  )
  BILLINGACCOUNTS_ACCOUNTS_OPERATIONS = (
      'billingAccounts.accounts.operations',
      '{+name}',
      {
          '':
              'billingAccounts/{billingAccountsId}/accounts/{accountsId}/'
              'operations/{operationsId}',
      },
      ['name'],
      True
  )
  BILLINGACCOUNTS_ORDERS = (
      'billingAccounts.orders',
      '{+name}',
      {
          '':
              'billingAccounts/{billingAccountsId}/orders/{ordersId}',
      },
      ['name'],
      True
  )
  BILLINGACCOUNTS_ORDERS_OPERATIONS = (
      'billingAccounts.orders.operations',
      '{+name}',
      {
          '':
              'billingAccounts/{billingAccountsId}/orders/{ordersId}/'
              'operations/{operationsId}',
      },
      ['name'],
      True
  )
  BILLINGACCOUNTS_ORDERS_ORDERALLOCATIONS = (
      'billingAccounts.orders.orderAllocations',
      '{+name}',
      {
          '':
              'billingAccounts/{billingAccountsId}/orders/{ordersId}/'
              'orderAllocations/{orderAllocationsId}',
      },
      ['name'],
      True
  )
  PROJECTS = (
      'projects',
      'projects/{projectsId}',
      {},
      ['projectsId'],
      True
  )
  PROJECTS_ENTITLEMENTS = (
      'projects.entitlements',
      '{+name}',
      {
          '':
              'projects/{projectsId}/entitlements/{entitlementsId}',
      },
      ['name'],
      True
  )
  PROJECTS_FREETRIALS = (
      'projects.freeTrials',
      '{+name}',
      {
          '':
              'projects/{projectsId}/freeTrials/{freeTrialsId}',
      },
      ['name'],
      True
  )
  PROJECTS_FREETRIALS_OPERATIONS = (
      'projects.freeTrials.operations',
      '{+name}',
      {
          '':
              'projects/{projectsId}/freeTrials/{freeTrialsId}/operations/'
              '{operationsId}',
      },
      ['name'],
      True
  )

  def __init__(self, collection_name, path, flat_paths, params,
               enable_uri_parsing):
    self.collection_name = collection_name
    self.path = path
    self.flat_paths = flat_paths
    self.params = params
    self.enable_uri_parsing = enable_uri_parsing
