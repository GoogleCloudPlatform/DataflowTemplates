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
"""Code for making shared messages between commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


def GetSuccessMessageForSynchronousDeploy(service):
  """Returns a user message for a successful synchronous deploy.

  Args:
    service: googlecloudsdk.api_lib.run.service.Service, Deployed service for
      which to build a success message.
  """
  latest_ready = service.status.latestReadyRevisionName
  latest_percent_traffic = service.latest_percent_traffic
  msg = ('Service [{{bold}}{serv}{{reset}}] '
         'revision [{{bold}}{rev}{{reset}}] '
         'has been deployed and is serving '
         '{{bold}}{latest_percent_traffic}{{reset}} percent of traffic.')
  if latest_percent_traffic:
    msg += '\nService URL: {{bold}}{url}{{reset}}'
  latest_url = service.latest_url
  tag_url_message = ''
  if latest_url:
    tag_url_message = '\nThe revision can be reached directly at {}'.format(
        latest_url)
  return msg.format(
      serv=service.name,
      rev=latest_ready,
      url=service.domain,
      latest_percent_traffic=latest_percent_traffic) + tag_url_message


def GetStartDeployMessage(conn_context,
                          service_ref,
                          operation='Deploying container'):
  """Returns a user mesage for starting a deploy.

  Args:
    conn_context: connection_context.ConnectionInfo, Metadata for the
      run API client.
    service_ref: protorpc.messages.Message, A resource reference object
      for the service See googlecloudsdk.core.resources.Registry.ParseResourceId
      for details.
    operation: str, what deploy action is being done.
  """
  msg = ('{operation} to {operator} service '
         '[{{bold}}{service}{{reset}}] in {ns_label} [{{bold}}{ns}{{reset}}]')
  msg += conn_context.location_label

  return msg.format(
      operation=operation,
      operator=conn_context.operator,
      ns_label=conn_context.ns_label,
      service=service_ref.servicesId,
      ns=service_ref.namespacesId)
