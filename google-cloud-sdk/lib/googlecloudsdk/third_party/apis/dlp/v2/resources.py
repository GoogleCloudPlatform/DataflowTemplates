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


BASE_URL = 'https://dlp.googleapis.com/v2/'
DOCS_URL = 'https://cloud.google.com/dlp/docs/'


class Collections(enum.Enum):
  """Collections for all supported apis."""

  INFOTYPES = (
      'infoTypes',
      'infoTypes',
      {},
      [],
      False
  )
  ORGANIZATIONS = (
      'organizations',
      'organizations/{organizationsId}',
      {},
      ['organizationsId'],
      True
  )
  ORGANIZATIONS_DEIDENTIFYTEMPLATES = (
      'organizations.deidentifyTemplates',
      '{+name}',
      {
          '':
              'organizations/{organizationsId}/deidentifyTemplates/'
              '{deidentifyTemplatesId}',
      },
      ['name'],
      True
  )
  ORGANIZATIONS_INSPECTTEMPLATES = (
      'organizations.inspectTemplates',
      '{+name}',
      {
          '':
              'organizations/{organizationsId}/inspectTemplates/'
              '{inspectTemplatesId}',
      },
      ['name'],
      True
  )
  ORGANIZATIONS_LOCATIONS = (
      'organizations.locations',
      'organizations/{organizationsId}/locations/{locationsId}',
      {},
      ['organizationsId', 'locationsId'],
      True
  )
  ORGANIZATIONS_LOCATIONS_DEIDENTIFYTEMPLATES = (
      'organizations.locations.deidentifyTemplates',
      '{+name}',
      {
          '':
              'organizations/{organizationsId}/locations/{locationsId}/'
              'deidentifyTemplates/{deidentifyTemplatesId}',
      },
      ['name'],
      True
  )
  ORGANIZATIONS_LOCATIONS_INSPECTTEMPLATES = (
      'organizations.locations.inspectTemplates',
      '{+name}',
      {
          '':
              'organizations/{organizationsId}/locations/{locationsId}/'
              'inspectTemplates/{inspectTemplatesId}',
      },
      ['name'],
      True
  )
  ORGANIZATIONS_LOCATIONS_STOREDINFOTYPES = (
      'organizations.locations.storedInfoTypes',
      '{+name}',
      {
          '':
              'organizations/{organizationsId}/locations/{locationsId}/'
              'storedInfoTypes/{storedInfoTypesId}',
      },
      ['name'],
      True
  )
  ORGANIZATIONS_STOREDINFOTYPES = (
      'organizations.storedInfoTypes',
      '{+name}',
      {
          '':
              'organizations/{organizationsId}/storedInfoTypes/'
              '{storedInfoTypesId}',
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
  PROJECTS_CONTENT = (
      'projects.content',
      'projects/{projectsId}',
      {},
      ['projectsId'],
      False
  )
  PROJECTS_DEIDENTIFYTEMPLATES = (
      'projects.deidentifyTemplates',
      '{+name}',
      {
          '':
              'projects/{projectsId}/deidentifyTemplates/'
              '{deidentifyTemplatesId}',
      },
      ['name'],
      True
  )
  PROJECTS_DLPJOBS = (
      'projects.dlpJobs',
      '{+name}',
      {
          '':
              'projects/{projectsId}/dlpJobs/{dlpJobsId}',
      },
      ['name'],
      True
  )
  PROJECTS_IMAGE = (
      'projects.image',
      'projects/{projectsId}',
      {},
      ['projectsId'],
      False
  )
  PROJECTS_INSPECTTEMPLATES = (
      'projects.inspectTemplates',
      '{+name}',
      {
          '':
              'projects/{projectsId}/inspectTemplates/{inspectTemplatesId}',
      },
      ['name'],
      True
  )
  PROJECTS_JOBTRIGGERS = (
      'projects.jobTriggers',
      '{+name}',
      {
          '':
              'projects/{projectsId}/jobTriggers/{jobTriggersId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS = (
      'projects.locations',
      'projects/{projectsId}/locations/{locationsId}',
      {},
      ['projectsId', 'locationsId'],
      True
  )
  PROJECTS_LOCATIONS_DEIDENTIFYTEMPLATES = (
      'projects.locations.deidentifyTemplates',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/'
              'deidentifyTemplates/{deidentifyTemplatesId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_DLPJOBS = (
      'projects.locations.dlpJobs',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/dlpJobs/'
              '{dlpJobsId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_INSPECTTEMPLATES = (
      'projects.locations.inspectTemplates',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/'
              'inspectTemplates/{inspectTemplatesId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_JOBTRIGGERS = (
      'projects.locations.jobTriggers',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/jobTriggers/'
              '{jobTriggersId}',
      },
      ['name'],
      True
  )
  PROJECTS_LOCATIONS_STOREDINFOTYPES = (
      'projects.locations.storedInfoTypes',
      '{+name}',
      {
          '':
              'projects/{projectsId}/locations/{locationsId}/storedInfoTypes/'
              '{storedInfoTypesId}',
      },
      ['name'],
      True
  )
  PROJECTS_STOREDINFOTYPES = (
      'projects.storedInfoTypes',
      '{+name}',
      {
          '':
              'projects/{projectsId}/storedInfoTypes/{storedInfoTypesId}',
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
