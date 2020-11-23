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
"""Wrapper for JSON-based Environment metadata."""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import six


class Environment(object):
  """Class that wraps a KubeRun Environment JSON object."""

  def __init__(self, name, namespace, target_configs=''):
    self.name = name
    self.namespace = namespace
    self.target_configs = target_configs

  @classmethod
  def FromJSON(cls, json_object):
    """Return an instance of Environment parsed out from the provided json.

    Args:
      json_object: JSON representation of the Environment

    Returns:
      An instance of Environment.

    Raises:
      AttributeError if the JSON provided is malformed.
    """
    spec = json_object.get('spec', {})
    cluster = spec.get('cluster', {})
    kubeconfig = spec.get('kubeconfig', {})
    if cluster:
      cluster_name = cluster.get('name', '')
      cluster_location = cluster.get('location', '')
      return cls(
          name=json_object.get('name', ''),
          namespace=cluster.get('namespace', ''),
          target_configs=(
              '{{cluster: \'{0}\', cluster location: \'{1}\'}}'.format(
                  cluster_name, cluster_location)))

    if kubeconfig:
      context = kubeconfig.get('context', '')
      return cls(
          name=json_object.get('name', ''),
          namespace=kubeconfig.get('namespace', ''),
          target_configs=('{{kubeconfig context: \'{0}\'}}'.format(context)))

    # Should never happen - return default values.
    return cls(
        name=json_object.get('name', ''), namespace='', target_configs='')

  def __repr__(self):
    # TODO(b/171419038): Create a common base class for these data wrappers
    items = sorted(self.__dict__.items())
    attrs_as_kv_strings = ['{}={!r}'.format(k, v) for (k, v) in items]
    return (six.text_type('Environment({})').format(
        ', '.join(attrs_as_kv_strings)))

  def __eq__(self, other):
    if isinstance(other, self.__class__):
      return self.__dict__ == other.__dict__
    return False
