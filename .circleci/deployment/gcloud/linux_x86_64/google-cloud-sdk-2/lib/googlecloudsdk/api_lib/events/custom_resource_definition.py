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

import json
import re

from googlecloudsdk.api_lib.run import k8s_object


# Identify parameters that are used to set secret values
_SECRET_PROPERTY_PATTERN = '^.*[sS]ecret$'
_SOURCE_CUSTOM_RESOURCE_DEFINITION_VERSION = 'v1'


def _IsSecretProperty(property_name, property_type):
  return (re.match(_SECRET_PROPERTY_PATTERN, property_name) and
          property_type == 'object')


class SourceSpecProperty(object):
  """Has details for a spec property of a source. Not write-through."""

  def __init__(self, name, description, type_, required):
    self.name = name
    self.description = description
    self.type = type_
    self.required = required


_EVENT_TYPE_REGISTRY_KEY = 'registry.knative.dev/eventTypes'


class EventType(object):
  """Has details for an event type of a source. Not write-through."""

  def __init__(self, source_crd, **kwargs):
    """Initialize a holder of info about an event type.

    Args:
      source_crd: SourceCustomResourceDefinition, the event type's parent
        source CRD.
      **kwargs: properties of the event type.
    """
    self._crd = source_crd
    self._properties = kwargs

  def __getattr__(self, attr):
    try:
      return self._properties[attr]
    except KeyError as e:
      raise AttributeError(e.args[0])

  @property
  def crd(self):
    """Returns a SourceCustomResourceDefinition."""
    return self._crd

  @property
  def details(self):
    """Returns a dict with details about this event type."""
    details = self.AsDict()
    details['source'] = self._crd.source_kind
    return details

  def AsDict(self):
    """Returns a dict with properties of this event type."""
    return self._properties.copy()

  def __eq__(self, other):
    if isinstance(other, type(self)):
      # pylint:disable=protected-access
      return self._properties == other._properties and self._crd == other._crd
    return False


class SourceCustomResourceDefinition(k8s_object.KubernetesObject):
  """Wraps an Source CRD message, making fields more convenient.

  Defined at
  https://github.com/google/knative-gcp/blob/master/config/core/resources/cloudpubsubsource.yaml
  self._m is a CustomResourceDefinition
  """

  API_CATEGORY = 'apiextensions.k8s.io'
  KIND = 'CustomResourceDefinition'
  READY_CONDITION = None  # The status field is not currently used on CRDs
  FIELD_BLACKLIST = ['openAPIV3Schema']
  # These fields should not be exposed to the user as regular parameters to be
  # set either because we'll provide another way to specify them, because
  # we'll set them ourselves, or because they're not meant to be set.
  _PRIVATE_PROPERTY_FIELDS = frozenset({'sink', 'ceOverrides'})

  # The list of crds within the CustomResourceDefinitionList response
  # lacks their apiVersions (intended behavior), so manually set it.
  def setApiVersion(self, api_version):  # pylint: disable=invalid-name
    self._m.apiVersion = api_version

  @property
  def source_kind(self):
    return self._m.spec.names.kind

  @property
  def source_kind_plural(self):
    return self._m.spec.names.plural

  @property
  def source_api_category(self):
    return self._m.spec.group

  @property
  def source_version(self):
    try:
      # Only exists in custom resource definition version v1beta1
      return self._m.spec.version
    except AttributeError:
      return _SOURCE_CUSTOM_RESOURCE_DEFINITION_VERSION

  def getActiveSourceVersions(self):
    """Returns list of active api versions for the source."""
    return [version.name for version in self._m.spec.versions if version.served]

  @property
  def schema(self):
    """Returns the SourceCustomResourceDefinition schema.

    Returns:
      k8s_object.ListAsReadOnlyDictionaryWrapper
    """
    # CustomResourceDefinition validation only exists in v1alpha1 and v1beta1
    # Under v1, validation now exists under spec.versions
    if self.source_version == 'v1alpha1' or self.source_version == 'v1beta1':
      return JsonSchemaPropsWrapper(self._m.spec.validation.openAPIV3Schema)

    # While the list CustomResourceDefinition is v1, the source itself could be
    # either v1 or v1beta1, therefore check for both possibilities that exist
    # under CustomResourceDefinitionVersions
    for crdv in self._m.spec.versions:
      if crdv.name == _SOURCE_CUSTOM_RESOURCE_DEFINITION_VERSION:
        return JsonSchemaPropsWrapper(crdv.schema.openAPIV3Schema)
    for crdv in self._m.spec.versions:
      if crdv.name == 'v1beta1':
        return JsonSchemaPropsWrapper(crdv.schema.openAPIV3Schema)
    raise AttributeError('CustomResourceDefinitionVersion not found')

  @property
  def event_types(self):
    """Returns List[EventType] from the registry annotation json string.

    Where metadata.annotations."registry.knative.dev/eventTypes" holds an array
    of {
      type: string of the event type,
      schema: string holding url to github proto defined,
      description: string describing the event type.
    }
    """
    if _EVENT_TYPE_REGISTRY_KEY not in self.annotations:
      return []
    event_types = json.loads(self.annotations[_EVENT_TYPE_REGISTRY_KEY])
    return [EventType(self, **et) for et in event_types]

  @event_types.setter
  def event_types(self, event_type_holders):
    """Sets the registry annotation given a List[EventType]."""
    event_type_dicts = [et.AsDict() for et in event_type_holders]
    self.annotations[_EVENT_TYPE_REGISTRY_KEY] = json.dumps(event_type_dicts)

  @property
  def secret_properties(self):
    """The properties used to define source secrets.

    Returns:
      List[SourceSpecProperty], modifying this list does *not* modify the
        underlying properties in the SourceCRD.
    """
    properties = []
    required_properties = self.schema['spec'].required
    for k, v in self.schema['spec'].items():
      if (k not in self._PRIVATE_PROPERTY_FIELDS and
          _IsSecretProperty(k, v.type)):
        properties.append(
            SourceSpecProperty(
                name=k,
                description=v.description,
                type_=v.type,
                required=k in required_properties))
    return properties

  @property
  def properties(self):
    """The user-configurable properties of the source.

    Returns:
      List[SourceSpecProperty], modifying this list does *not* modify the
        underlying properties in the SourceCRD.
    """
    properties = []
    required_properties = self.schema['spec'].required
    for k, v in self.schema['spec'].items():
      if (k not in self._PRIVATE_PROPERTY_FIELDS and
          not _IsSecretProperty(k, v.type)):
        properties.append(
            SourceSpecProperty(
                name=k,
                description=v.description,
                type_=v.type,
                required=k in required_properties))
    return properties


class JsonSchemaPropsWrapper(k8s_object.ListAsReadOnlyDictionaryWrapper):
  """Wrap a JSONSchemaProps message with properties in a dict-like manner.

  Nesting in JSONSchemaProps messages is done via lists of its own type.
  This class provides access to the underlying information in a dict-like
  manner rather than needing to handle accessing the lists directly.
  """

  def __init__(self, to_wrap):
    """Wrap the actual keys and values of a JSONSchemaProps message.

    Args:
      to_wrap: JSONSchemaProps message
    """
    super(JsonSchemaPropsWrapper, self).__init__(
        to_wrap.properties.additionalProperties, key_field='key')
    self._wrapped_json = to_wrap

  def __getattr__(self, attr):
    """Fallthrough to the underlying wrapped json to access other fields."""
    return getattr(self._wrapped_json, attr)

  def __getitem__(self, key):
    item = super(JsonSchemaPropsWrapper, self).__getitem__(key)
    value = item.value
    if value.properties is None:
      # It doesn't go any deeper, return the actual value
      return value
    return JsonSchemaPropsWrapper(value)
