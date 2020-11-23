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


BASE_URL = 'https://sddc.googleapis.com/v1alpha1/'
DOCS_URL = 'https://cloud.google.com/vmware/'


class Collections(enum.Enum):
  """Collections for all supported apis."""

  PROJECTS = (
      'projects',
      'projects/{projectsId}',
      {},
      ['projectsId'],
      True
  )
  PROJECTS_LOCATIONS = (
      'projects.locations',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_CLUSTERGROUPBACKUPS = (
      'projects.locations.clusterGroupBackups',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/'
              'clusterGroupBackups/{clusterGroupBackupsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_CLUSTERGROUPS = (
      'projects.locations.clusterGroups',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/clusterGroups/'
              '{clusterGroupsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_CLUSTERGROUPS_CLUSTERS = (
      'projects.locations.clusterGroups.clusters',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/clusterGroups/'
              '{clusterGroupsId}/clusters/{clustersId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_CLUSTERGROUPS_IPADDRESSES = (
      'projects.locations.clusterGroups.ipAddresses',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/clusterGroups/'
              '{clusterGroupsId}/ipAddresses/{ipAddressesId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_OPERATIONS = (
      'projects.locations.operations',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/operations/'
              '{operationsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_PRIVATECLOUDBACKUPS = (
      'projects.locations.privateCloudBackups',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/'
              'privateCloudBackups/{privateCloudBackupsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_PRIVATECLOUDS = (
      'projects.locations.privateClouds',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/privateClouds/'
              '{privateCloudsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_PRIVATECLOUDS_CLUSTERS = (
      'projects.locations.privateClouds.clusters',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/privateClouds/'
              '{privateCloudsId}/clusters/{clustersId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_PRIVATECLOUDS_IPADDRESSES = (
      'projects.locations.privateClouds.ipAddresses',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/privateClouds/'
              '{privateCloudsId}/ipAddresses/{ipAddressesId}',
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
