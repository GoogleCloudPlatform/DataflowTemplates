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


BASE_URL = 'https://dialogflow.googleapis.com/v2/'
DOCS_URL = 'https://cloud.google.com/dialogflow/'


class Collections(enum.Enum):
  """Collections for all supported apis."""

  PROJECTS = (
      'projects',
      'projects/{projectsId}',
      {},
      ['projectsId'],
      True
  )
  PROJECTS_AGENT = (
      'projects.agent',
      'projects/{projectsId}/agent',
      {},
      ['projectsId'],
      True
  )
  PROJECTS_AGENT_ENTITYTYPES = (
      'projects.agent.entityTypes',
      '{+name}',
      {
          '':
              'projects/{projectsId}/agent/entityTypes/{entityTypesId}',
      },
      ['name'],
      True
  )
  PROJECTS_AGENT_ENVIRONMENTS = (
      'projects.agent.environments',
      'projects/{projectsId}/agent/environments/{environmentsId}',
      {},
      ['projectsId', 'environmentsId'],
      True
  )
  PROJECTS_AGENT_ENVIRONMENTS_USERS = (
      'projects.agent.environments.users',
      'projects/{projectsId}/agent/environments/{environmentsId}/users/'
      '{usersId}',
      {},
      ['projectsId', 'environmentsId', 'usersId'],
      True
  )
  PROJECTS_AGENT_ENVIRONMENTS_USERS_SESSIONS = (
      'projects.agent.environments.users.sessions',
      'projects/{projectsId}/agent/environments/{environmentsId}/users/'
      '{usersId}/sessions/{sessionsId}',
      {},
      ['projectsId', 'environmentsId', 'usersId', 'sessionsId'],
      True
  )
  PROJECTS_AGENT_ENVIRONMENTS_USERS_SESSIONS_CONTEXTS = (
      'projects.agent.environments.users.sessions.contexts',
      '{+name}',
      {
          '':
              'projects/{projectsId}/agent/environments/{environmentsId}/'
              'users/{usersId}/sessions/{sessionsId}/contexts/{contextsId}',
      },
      ['name'],
      True
  )
  PROJECTS_AGENT_ENVIRONMENTS_USERS_SESSIONS_ENTITYTYPES = (
      'projects.agent.environments.users.sessions.entityTypes',
      '{+name}',
      {
          '':
              'projects/{projectsId}/agent/environments/{environmentsId}/'
              'users/{usersId}/sessions/{sessionsId}/entityTypes/'
              '{entityTypesId}',
      },
      ['name'],
      True
  )
  PROJECTS_AGENT_INTENTS = (
      'projects.agent.intents',
      '{+name}',
      {
          '':
              'projects/{projectsId}/agent/intents/{intentsId}',
      },
      ['name'],
      True
  )
  PROJECTS_AGENT_SESSIONS = (
      'projects.agent.sessions',
      'projects/{projectsId}/agent/sessions/{sessionsId}',
      {},
      ['projectsId', 'sessionsId'],
      True
  )
  PROJECTS_AGENT_SESSIONS_CONTEXTS = (
      'projects.agent.sessions.contexts',
      '{+name}',
      {
          '':
              'projects/{projectsId}/agent/sessions/{sessionsId}/contexts/'
              '{contextsId}',
      },
      ['name'],
      True
  )
  PROJECTS_AGENT_SESSIONS_ENTITYTYPES = (
      'projects.agent.sessions.entityTypes',
      '{+name}',
      {
          '':
              'projects/{projectsId}/agent/sessions/{sessionsId}/entityTypes/'
              '{entityTypesId}',
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
  PROJECTS_OPERATIONS = (
      'projects.operations',
      '{+name}',
      {
          '':
              'projects/{projectsId}/operations/{operationsId}',
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
