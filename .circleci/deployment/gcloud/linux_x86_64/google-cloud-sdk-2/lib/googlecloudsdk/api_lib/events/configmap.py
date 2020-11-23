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
"""Wraps a k8s Secret message, making fields more convenient."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from googlecloudsdk.api_lib.run import k8s_object


class ConfigMap(k8s_object.KubernetesObject):
  """A kubernetes ConfigMap resource."""

  API_CATEGORY = None  # Core resources do not have an api category
  KIND = 'ConfigMap'

  @property
  def data(self):
    if self._m.data is None:
      self._m.data = self._messages.ConfigMap.DataValue()
    return k8s_object.ListAsDictionaryWrapper(
        self._m.data.additionalProperties,
        self._messages.ConfigMap.DataValue.AdditionalProperty,
        key_field='key',
        value_field='value')
