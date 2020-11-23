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
"""Wraps a CRD message, making fields more convenient."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.run import k8s_object


class CloudRun(k8s_object.KubernetesObject):
  """Wraps an Operator CloudRun message, making fields more convenient."""

  API_CATEGORY = 'operator.run.cloud.google.com'
  KIND = 'CloudRun'

  @property
  def spec(self):
    return self._m.spec

  @spec.setter
  def spec(self, value):
    self._m.spec = value

  @property
  def eventing(self):
    return self._m.spec.eventing

  @eventing.setter
  def eventing(self, value):
    self._m.spec.eventing = value

  @property
  def eventing_enabled(self):
    if self._m.spec is not None and self._m.spec.eventing is not None:
      return self._m.spec.eventing.enabled
    else:
      return False
