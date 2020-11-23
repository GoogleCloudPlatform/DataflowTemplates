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


BASE_URL = 'https://run.googleapis.com/v1alpha1/'
DOCS_URL = 'https://cloud.google.com/run/'


class Collections(enum.Enum):
  """Collections for all supported apis."""

  NAMESPACES = (
      'namespaces',
      'namespaces/{namespacesId}',
      {},
      ['namespacesId'],
      True
  )
  NAMESPACES_CLOUDAUDITLOGSSOURCES = (
      'namespaces.cloudauditlogssources',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/cloudauditlogssources/'
              '{cloudauditlogssourcesId}',
      },
      ['name'],
      True
  )
  NAMESPACES_CLOUDPUBSUBSOURCES = (
      'namespaces.cloudpubsubsources',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/cloudpubsubsources/'
              '{cloudpubsubsourcesId}',
      },
      ['name'],
      True
  )
  NAMESPACES_CLOUDSCHEDULERSOURCES = (
      'namespaces.cloudschedulersources',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/cloudschedulersources/'
              '{cloudschedulersourcesId}',
      },
      ['name'],
      True
  )
  NAMESPACES_CLOUDSTORAGESOURCES = (
      'namespaces.cloudstoragesources',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/cloudstoragesources/'
              '{cloudstoragesourcesId}',
      },
      ['name'],
      True
  )
  NAMESPACES_CONFIGURATIONS = (
      'namespaces.configurations',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/configurations/{configurationsId}',
      },
      ['name'],
      True
  )
  NAMESPACES_DOMAINMAPPINGS = (
      'namespaces.domainmappings',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/domainmappings/{domainmappingsId}',
      },
      ['name'],
      True
  )
  NAMESPACES_REVISIONS = (
      'namespaces.revisions',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/revisions/{revisionsId}',
      },
      ['name'],
      True
  )
  NAMESPACES_ROUTES = (
      'namespaces.routes',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/routes/{routesId}',
      },
      ['name'],
      True
  )
  NAMESPACES_SERVICES = (
      'namespaces.services',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/services/{servicesId}',
      },
      ['name'],
      True
  )
  NAMESPACES_TRIGGERS = (
      'namespaces.triggers',
      '{+name}',
      {
          '':
              'namespaces/{namespacesId}/triggers/{triggersId}',
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
  PROJECTS_LOCATIONS = (
      'projects.locations',
      'projects/{projectsId}/locations/{locationsId}',
      {},
      ['projectsId', 'locationsId'],
      True
  )
  PROJECTS_LOCATIONS_CLOUDAUDITLOGSSOURCES = (
      'projects.locations.cloudauditlogssources',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/'
              'cloudauditlogssources/{cloudauditlogssourcesId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_CLOUDPUBSUBSOURCES = (
      'projects.locations.cloudpubsubsources',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/'
              'cloudpubsubsources/{cloudpubsubsourcesId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_CLOUDSCHEDULERSOURCES = (
      'projects.locations.cloudschedulersources',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/'
              'cloudschedulersources/{cloudschedulersourcesId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_CLOUDSTORAGESOURCES = (
      'projects.locations.cloudstoragesources',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/'
              'cloudstoragesources/{cloudstoragesourcesId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_CONFIGURATIONS = (
      'projects.locations.configurations',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/configurations/'
              '{configurationsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_DOMAINMAPPINGS = (
      'projects.locations.domainmappings',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/domainmappings/'
              '{domainmappingsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_REVISIONS = (
      'projects.locations.revisions',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/revisions/'
              '{revisionsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_ROUTES = (
      'projects.locations.routes',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/routes/'
              '{routesId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_SERVICES = (
      'projects.locations.services',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/services/'
              '{servicesId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_TRIGGERS = (
      'projects.locations.triggers',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/triggers/'
              '{triggersId}',
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
