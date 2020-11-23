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
"""Wrapper for JSON-based Component metadata."""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import six


class Component(object):
  """Class that wraps a KubeRun Component JSON object."""

  def __init__(self, name, type_, devkit, ce_input, vars_):
    self.name = name
    self.type = type_
    self.devkit = devkit
    self.ce_input = ce_input
    self.vars = vars_

  @classmethod
  def FromJSON(cls, json_object):
    return cls(
        name=json_object.get('name', ''),
        type_=json_object.get('type', ''),
        devkit=json_object.get('devkit', ''),
        ce_input=json_object.get('input-cetype', ''),
        vars_=json_object.get('fdk-vars', []),
        )

  def __repr__(self):
    # TODO(b/171419038): Create a common base class for these data wrappers
    items = sorted(self.__dict__.items())
    attrs_as_kv_strings = ['{}={!r}'.format(k, v) for (k, v) in items]
    return six.text_type('Component({})').format(', '.join(attrs_as_kv_strings))

  def __eq__(self, other):
    if isinstance(other, self.__class__):
      return self.__dict__ == other.__dict__
    return False
