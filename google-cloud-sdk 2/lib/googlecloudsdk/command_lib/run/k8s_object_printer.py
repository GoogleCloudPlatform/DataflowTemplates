# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
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

import abc
import textwrap

from googlecloudsdk.api_lib.run import k8s_object
from googlecloudsdk.core.console import console_attr
from googlecloudsdk.core.resource import custom_printer_base as cp

import six


def OrderByKey(map_):
  for k in sorted(map_):
    yield k, map_[k]


@six.add_metaclass(abc.ABCMeta)
class K8sObjectPrinter(cp.CustomPrinterBase):
  """Base class for printing kubernetes objects in a human-readable way.

  Contains shared methods for printing status and metadata.
  """

  def _GetReadyMessage(self, record):
    if record.ready_condition and record.ready_condition['message']:
      symbol, color = record.ReadySymbolAndColor()
      return console_attr.GetConsoleAttr().Colorize(
          textwrap.fill('{} {}'.format(
              symbol, record.ready_condition['message']), 100), color)
    else:
      return ''

  def _GetLastUpdated(self, record):
    modifier = record.last_modifier or '?'
    last_transition_time = '?'
    for condition in record.status.conditions:
      if condition.type == 'Ready' and condition.lastTransitionTime:
        last_transition_time = condition.lastTransitionTime
    return 'Last updated on {} by {}'.format(last_transition_time, modifier)

  def _GetLabels(self, labels):
    """Returns a human readable description of user provided labels if any."""
    if not labels:
      return ''
    return ' '.join(
        sorted([
            '{}:{}'.format(k, v)
            for k, v in labels.items()
            if not k.startswith(k8s_object.INTERNAL_GROUPS)
        ]))

  def _GetHeader(self, record):
    con = console_attr.GetConsoleAttr()
    status = con.Colorize(*record.ReadySymbolAndColor())
    try:
      place = 'region ' + record.region
    except KeyError:
      place = 'namespace ' + record.namespace
    return con.Emphasize('{} {} {} in {}'.format(status, record.Kind(),
                                                 record.name, place))
