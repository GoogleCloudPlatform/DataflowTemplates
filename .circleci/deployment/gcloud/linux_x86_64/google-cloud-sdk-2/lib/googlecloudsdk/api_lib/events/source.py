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
"""Wraps an Events Source message, making fields more convenient."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json

from googlecloudsdk.api_lib.run import k8s_object
from googlecloudsdk.command_lib.util.apis import arg_utils

_BROKER_API_CATEGORY = 'eventing.knative.dev'
_BROKER_KIND = 'Broker'


class Source(k8s_object.KubernetesObject):
  """Wraps a Source message, making fields more convenient.

  As there's no single Source kind, this class is meant to wrap all sources.
  Sources should be mostly the same but may differ in spec fields (with some
  available in all, e.g. sink and ceOverrides) and status conditions (with some
  available in all, e.g. Ready).
  """

  READY_CONDITION = 'Ready'
  TERMINAL_CONDITIONS = {
      READY_CONDITION,
  }
  # localObjectReference is not a recognized field in SecretKeySelector
  # messages, so don't try to set it for any secrets in the source spec.
  FIELD_BLACKLIST = ['localObjectReference', 'pubsubSecret']

  @property
  def sink(self):
    return self._m.spec.sink.ref.name

  def set_sink(self, broker_name, api_version):
    """Set the sink to a broker."""
    self._m.spec.sink.ref.apiVersion = '{}/{}'.format(_BROKER_API_CATEGORY,
                                                      api_version)
    self._m.spec.sink.ref.kind = _BROKER_KIND
    self._m.spec.sink.ref.name = broker_name

  @property
  def ce_overrides(self):
    """The CloudEvents Overrides Extensions map.

    This property is a dict-like object for managed CE override extensions.
    These key-value pairs will be added to the Extensions section of all
    CloudEvents produced by this source.

    Returns:
      A dict-like object for managing CE override extensions.
    """
    if self._m.spec.ceOverrides is None:
      self._m.spec.ceOverrides = self._messages.CloudEventOverrides(
          extensions=self._messages.CloudEventOverrides.ExtensionsValue())
    return k8s_object.ListAsDictionaryWrapper(
        self._m.spec.ceOverrides.extensions.additionalProperties,
        self._messages.CloudEventOverrides.ExtensionsValue.AdditionalProperty,
        key_field='key')


def ParseDynamicFieldsIntoMessage(message, parameters):
  """Set fields in message corresponding to a dict of usually static fields.

  For repeated fields interpreted as json.

  Args:
    message: The source spec.
    parameters: dict of fields to values. The values are by default interpreted
      as raw string, but values are interpreted as a nested data structure,
      depending on the whitelist.
  """
  parameters = parameters or {}
  for field_path, value in parameters.items():
    field = arg_utils.GetFieldFromMessage(message, field_path)

    if field.repeated and not isinstance(value, list):
      value = arg_utils.ConvertValue(field, value, processor=JSONProcessor)
    value = arg_utils.ConvertValue(field, value)

    arg_utils.SetFieldInMessage(message, field_path, value)


def JSONProcessor(value):
  """Convert string into python dictionary and lists based on json format."""
  return json.loads(value)


def SourceFix(source):
  """Set spec.owner field to None if both subfields are None."""
  # TODO(b/168572978) replace with a more generalized solution for clearing
  # undesired None fields from Source spec
  if source.kind == 'ApiServerSource':
    owner = source.spec.owner
    if owner.apiVersion is None and owner.kind is None:
      source.spec.owner = None
  return source
