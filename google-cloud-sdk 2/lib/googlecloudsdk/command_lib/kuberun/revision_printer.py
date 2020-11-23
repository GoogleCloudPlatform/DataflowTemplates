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
"""Revision-specific printer."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections

from googlecloudsdk.api_lib.kuberun import revision
from googlecloudsdk.command_lib.kuberun import k8s_object_printer
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


class RevisionPrinter(cp.CustomPrinterBase):
  """Prints the run Revision in a custom human-readable format.

  Format specific to Cloud Run revisions. Only available on Cloud Run commands
  that print revisions.
  """

  def Transform(self, record):
    """Transform a revision into the output structure of marker classes."""
    fmt = cp.Lines([
        k8s_object_printer.GetHeader(record),
        k8s_object_printer.GetLabels(record.labels), ' ',
        self.TransformSpec(record),
        k8s_object_printer.GetReadyMessage(record)
    ])
    return fmt

  def GetLimits(self, rev):
    return collections.defaultdict(str, rev.resource_limits)

  def GetUserEnvironmentVariables(self, record):
    return cp.Mapped(
        k8s_object_printer.OrderByKey(
            record.env_vars.literals))

  def GetSecrets(self, record):
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

  def GetConfigMaps(self, record):
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

  def GetTimeout(self, record):
    if record.timeout is not None:
      return '{}s'.format(record.timeout)
    return None

  def GetMinInstances(self, record):
    if record.annotations:
      return record.annotations.get(revision.MIN_SCALE_ANNOTATION, '')
    return None

  def GetMaxInstances(self, record):
    if record.annotations:
      return record.annotations.get(revision.MAX_SCALE_ANNOTATION, '')
    return None

  def TransformSpec(self, record):
    limits = self.GetLimits(record)
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
        ('Env vars', self.GetUserEnvironmentVariables(record)),
        ('Secrets', self.GetSecrets(record)),
        ('Config Maps', self.GetConfigMaps(record)),
        ('Concurrency', record.concurrency),
        ('Min Instances', self.GetMinInstances(record)),
        ('Max Instances', self.GetMaxInstances(record)),
        ('Timeout', self.GetTimeout(record)),
    ])
