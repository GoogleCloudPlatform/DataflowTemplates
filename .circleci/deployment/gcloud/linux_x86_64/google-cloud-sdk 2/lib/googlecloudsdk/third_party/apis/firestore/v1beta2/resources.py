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


BASE_URL = 'https://firestore.googleapis.com/v1beta2/'
DOCS_URL = 'https://cloud.google.com/firestore'


class Collections(enum.Enum):
  """Collections for all supported apis."""

  PROJECTS = (
      'projects',
      'projects/{projectsId}',
      {},
      ['projectsId'],
      True
  )
  PROJECTS_DATABASES = (
      'projects.databases',
      'projects/{projectsId}/databases/{databasesId}',
      {},
      ['projectsId', 'databasesId'],
      True
  )
  PROJECTS_DATABASES_COLLECTIONGROUPS = (
      'projects.databases.collectionGroups',
      'projects/{projectsId}/databases/{databasesId}/collectionGroups/'
      '{collectionGroupsId}',
      {},
      ['projectsId', 'databasesId', 'collectionGroupsId'],
      True
  )
  PROJECTS_DATABASES_COLLECTIONGROUPS_FIELDS = (
      'projects.databases.collectionGroups.fields',
      '{+name}',
      {
          '':
              'projects/{projectsId}/databases/{databasesId}/'
              'collectionGroups/{collectionGroupsId}/fields/{fieldsId}',
      },
      ['name'],
      True
  )
  PROJECTS_DATABASES_COLLECTIONGROUPS_INDEXES = (
      'projects.databases.collectionGroups.indexes',
      '{+name}',
      {
          '':
              'projects/{projectsId}/databases/{databasesId}/'
              'collectionGroups/{collectionGroupsId}/indexes/{indexesId}',
      },
      ['name'],
      True
  )
  PROJECTS_DATABASES_OPERATIONS = (
      'projects.databases.operations',
      'projects/{projectsId}/databases/{databasesId}/operations/'
      '{operationsId}',
      {},
      ['projectsId', 'databasesId', 'operationsId'],
      True
  )

  def __init__(self, collection_name, path, flat_paths, params,
               enable_uri_parsing):
    self.collection_name = collection_name
    self.path = path
    self.flat_paths = flat_paths
    self.params = params
    self.enable_uri_parsing = enable_uri_parsing
