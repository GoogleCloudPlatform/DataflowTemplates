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
"""Service-specific printer."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from googlecloudsdk.api_lib.run import service
from googlecloudsdk.command_lib.run import flags
from googlecloudsdk.command_lib.run import k8s_object_printer
from googlecloudsdk.command_lib.run import revision_printer
from googlecloudsdk.command_lib.run import traffic_printer
from googlecloudsdk.core.console import console_attr
from googlecloudsdk.core.resource import custom_printer_base as cp


SERVICE_PRINTER_FORMAT = 'service'
_INGRESS_UNSPECIFIED = '-'


class ServicePrinter(k8s_object_printer.K8sObjectPrinter):
  """Prints the run Service in a custom human-readable format.

  Format specific to Cloud Run services. Only available on Cloud Run commands
  that print services.
  """

  def _GetIngress(self, record):
    """Gets the ingress traffic allowed to call the service."""
    if flags.GetPlatform() == flags.PLATFORM_MANAGED:
      spec_ingress = record.annotations.get(service.INGRESS_ANNOTATION)
      status_ingress = record.annotations.get(service.INGRESS_STATUS_ANNOTATION)
      if spec_ingress == status_ingress:
        return spec_ingress
      else:
        spec_ingress = spec_ingress or _INGRESS_UNSPECIFIED
        status_ingress = status_ingress or _INGRESS_UNSPECIFIED
        return '{} (currently {})'.format(spec_ingress, status_ingress)
    elif (record.labels.get(
        service.ENDPOINT_VISIBILITY) == service.CLUSTER_LOCAL):
      return service.INGRESS_INTERNAL
    else:
      return service.INGRESS_ALL

  def _GetRevisionHeader(self, record):
    return console_attr.GetConsoleAttr().Emphasize('Revision {}'.format(
        record.status.latestCreatedRevisionName))

  def _RevisionPrinters(self, record):
    """Adds printers for the revision."""
    return cp.Lines([
        self._GetRevisionHeader(record),
        self._GetLabels(record.template.labels),
        revision_printer.RevisionPrinter.TransformSpec(record.template),
    ])

  def Transform(self, record):
    """Transform a service into the output structure of marker classes."""
    fmt = cp.Lines([
        self._GetHeader(record),
        self._GetLabels(record.labels), ' ',
        cp.Section([
            traffic_printer.TransformTraffic(record),
            cp.Labeled([('Ingress', self._GetIngress(record))]), ' ',
        ], max_column_width=60),
        cp.Labeled([(self._GetLastUpdated(record),
                     self._RevisionPrinters(record))]),
        self._GetReadyMessage(record)
    ])
    return fmt
