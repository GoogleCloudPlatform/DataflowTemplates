# -*- coding: utf-8 -*- #
# Copyright 2017 Google LLC. All Rights Reserved.
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
"""Constants for `gcloud tasks` and `gcloud app deploy` commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


PROJECTS_COLLECTION = 'cloudtasks.projects'
LOCATIONS_COLLECTION = 'cloudtasks.projects.locations'
QUEUES_COLLECTION = 'cloudtasks.projects.locations.queues'
TASKS_COLLECTION = 'cloudtasks.projects.locations.queues.tasks'

PULL_QUEUE = 'pull'
PUSH_QUEUE = 'push'
VALID_QUEUE_TYPES = [PULL_QUEUE, PUSH_QUEUE]

PULL_TASK = 'pull'
APP_ENGINE_TASK = 'app-engine'
HTTP_TASK = 'http'

APP_ENGINE_ROUTING_KEYS = ['service', 'version', 'instance']

QUEUE_MANAGEMENT_WARNING = (
    'You are managing queues with gcloud, do not use queue.yaml or queue.xml '
    'in the future. More details at: '
    'https://cloud.google.com/tasks/docs/queue-yaml.')

MAX_RATE = 500
MAX_BUCKET_SIZE = 500

TIME_IN_SECONDS = {
    's': 1,
    'm': 60,
    'h': 3600,
    'd': 86400,
}

APP_TO_TASKS_ATTRIBUTES_MAPPING = {
    'bucket_size': 'max_burst_size',
    'max_concurrent_requests': 'max_concurrent_dispatches',
    'mode': 'type',
    'name': 'name',
    'rate': 'max_dispatches_per_second',
    'retry_parameters.min_backoff_seconds': 'min_backoff',
    'retry_parameters.max_backoff_seconds': 'max_backoff',
    'retry_parameters.max_doublings': 'max_doublings',
    'retry_parameters.task_age_limit': 'max_retry_duration',
    'retry_parameters.task_retry_limit': 'max_attempts',
    'target': 'routing_override',
    # Not supported and need to deprecate if possible. See go/remove-tq-quotas
    # 'total_storage_limit': 'total_storage_limit'
}

PUSH_QUEUES_APP_DEPLOY_DEFAULT_VALUES = {
    'max_attempts': -1,  # Translates as 'unlimited' in CT-FE
    'max_backoff': '3600s',
    'max_doublings': 16,
    'max_burst_size': 5,
    # The previous behavior when max_concurrent_dispactches was not present in
    # the YAML file was to NOT set it at all which would show up as 0 in the UI.
    # However, functionally it is no different from using the default value of
    # 1000 and this is more or less a UI fix.
    'max_concurrent_dispatches': 1000,
    'max_retry_duration': '0s',  # Translates as 'unlimited' in CT-FE
    'min_backoff': '0.100s',
}

# Note currently CT APIs do not support modifying any pull-queue attributes
# except max_attempts and max_retry_duration while queue.yaml does not support
# max_retry_duration.
PULL_QUEUES_APP_DEPLOY_DEFAULT_VALUES = {
    'max_attempts': -1,  # Translates as 'unlimited' in CT-FE
}
