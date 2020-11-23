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

import collections

from googlecloudsdk.api_lib.run import revision
from googlecloudsdk.command_lib.run import k8s_object_printer
from googlecloudsdk.core.resource import custom_printer_base as cp
import six


REVISION_PRINTER_FORMAT = 'revision'


def FormatSecretKeyRef(v):
  return '{}:{}'.format(v.secretKeyRef.name, v.secretKeyRef.key)


def FormatSecretVolumeSource(v):
  if v.items:
    return '{}:{}'.format(v.secretName, v.items[0].key)
  else:
    return v.secretName


def FormatConfigMapKeyRef(v):
  return '{}:{}'.format(v.configMapKeyRef.name, v.configMapKeyRef.key)


def FormatConfigMapVolumeSource(v):
  if v.items:
    return '{}:{}'.format(v.name, v.items[0].key)
  else:
    return v.name


class RevisionPrinter(k8s_object_printer.K8sObjectPrinter):
  """Prints the run Revision in a custom human-readable format.

  Format specific to Cloud Run revisions. Only available on Cloud Run commands
  that print revisions.
  """

  def Transform(self, record):
    """Transform a service into the output structure of marker classes."""
    fmt = cp.Lines([
        self._GetHeader(record),
        self._GetLabels(record.labels),
        ' ',
        self.TransformSpec(record),
        self._GetReadyMessage(record)])
    return fmt

  @staticmethod
  def GetLimits(rev):
    return collections.defaultdict(str, rev.resource_limits)

  @staticmethod
  def GetUserEnvironmentVariables(record):
    return cp.Mapped(
        k8s_object_printer.OrderByKey(
            record.env_vars.literals))

  @staticmethod
  def GetSecrets(record):
    secrets = {}
    secrets.update({
        k: FormatSecretKeyRef(v)
        for k, v in record.env_vars.secrets.items()
    })
    secrets.update({
        k: FormatSecretVolumeSource(v)
        for k, v in record.MountedVolumeJoin('secrets').items()
    })
    return cp.Mapped(k8s_object_printer.OrderByKey(secrets))

  @staticmethod
  def GetConfigMaps(record):
    config_maps = {}
    config_maps.update({
        k: FormatConfigMapKeyRef(v)
        for k, v in record.env_vars.config_maps.items()
    })
    config_maps.update({
        k: FormatConfigMapVolumeSource(v)
        for k, v in record.MountedVolumeJoin('config_maps').items()
    })
    return cp.Mapped(k8s_object_printer.OrderByKey(config_maps))

  @staticmethod
  def GetCloudSqlInstances(record):
    instances = record.annotations.get(revision.CLOUDSQL_ANNOTATION, '')
    return instances.replace(',', ', ')

  @staticmethod
  def GetTimeout(record):
    if record.timeout is not None:
      return '{}s'.format(record.timeout)
    return None

  @staticmethod
  def GetVpcConnector(record):
    return cp.Labeled([
        ('Name', record.annotations.get(revision.VPC_ACCESS_ANNOTATION, '')),
        ('Egress',
         record.annotations.get(revision.EGRESS_SETTINGS_ANNOTATION, ''))
    ])

  @staticmethod
  def GetMinInstances(record):
    return record.annotations.get(revision.MIN_SCALE_ANNOTATION, '')

  @staticmethod
  def GetMaxInstances(record):
    return record.annotations.get(revision.MAX_SCALE_ANNOTATION, '')

  @staticmethod
  def TransformSpec(record):
    limits = RevisionPrinter.GetLimits(record)
    return cp.Labeled([
        ('Image', record.UserImage()),
        ('Command', ' '.join(record.container.command)),
        ('Args', ' '.join(record.container.args)),
        ('Port', ' '.join(
            six.text_type(p.containerPort)
            for p in record.container.ports)),
        ('Memory', limits['memory']),
        ('CPU', limits['cpu']),
        ('Service account', record.spec.serviceAccountName),
        ('Env vars', RevisionPrinter.GetUserEnvironmentVariables(record)),
        ('Secrets', RevisionPrinter.GetSecrets(record)),
        ('Config Maps', RevisionPrinter.GetConfigMaps(record)),
        ('Concurrency', record.concurrency),
        ('Min Instances', RevisionPrinter.GetMinInstances(record)),
        ('Max Instances', RevisionPrinter.GetMaxInstances(record)),
        ('SQL connections', RevisionPrinter.GetCloudSqlInstances(record)),
        ('Timeout', RevisionPrinter.GetTimeout(record)),
        ('VPC connector', RevisionPrinter.GetVpcConnector(record)),
    ])
