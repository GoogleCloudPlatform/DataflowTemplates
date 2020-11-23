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
"""Provides common methods for the Events command surface."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from googlecloudsdk.api_lib.events import trigger
from googlecloudsdk.api_lib.util import waiter
from googlecloudsdk.command_lib.events import exceptions
from googlecloudsdk.core import resources
from googlecloudsdk.core.console import console_io
from googlecloudsdk.core.util import retry


SOURCE_COLLECTION_NAME = 'run.namespaces.{plural_kind}'
ANTHOS_SOURCE_COLLECTION_NAME = 'anthosevents.namespaces.{plural_kind}'
ANTHOS_TRIGGER_COLLECTION_NAME = 'anthosevents.namespaces.triggers'
ANTHOS_NAMESPACE_COLLECTION_NAME = 'anthosevents.namespaces'

# Max wait time before timing out
_POLLING_TIMEOUT_MS = 180000
# Max wait time between poll retries before timing out
_RETRY_TIMEOUT_MS = 1000


def EventTypeFromTypeString(source_crds, type_string, source=None):
  """Returns the matching event type object given a list of source crds.

  Return an EventType object given a type string and list of source CRDs.
  Optionally, can also pass a source string to further restrict matching event
  types across multiple sources.

  If multiple event type are found to match the given input, the user is
  prompted to pick one, or an error is raised if prompting is not available.

  Args:
    source_crds: list[SourceCustomResourceDefinition]
    type_string: str, matching an event type string
      (e.g. "google.cloud.pubsub.topic.v1.messagePublished").
    source: str, optional source to further specify which event type in the case
      of multiple sources having event types with the same type string.
  """
  possible_matches = []
  for crd in source_crds:
    # Only match the specified source, if provided
    if source is not None and source != crd.source_kind:
      continue
    for event_type in crd.event_types:
      if type_string == event_type.type:
        possible_matches.append(event_type)

  # No matches
  if not possible_matches:
    raise exceptions.EventTypeNotFound(
        "Unknown event type: {}. If you're trying to use a custom event type, "
        'add the "--custom-type" flag.'.format(type_string))

  # Single match
  if len(possible_matches) == 1:
    return possible_matches[0]

  # Multiple matches
  if not console_io.CanPrompt():
    raise exceptions.MultipleEventTypesFound(
        'Multiple matching event types found: {}.'.format(type_string))
  index = console_io.PromptChoice(
      [
          '{} from {}'.format(et.type, et.crd.source_kind)
          for et in possible_matches
      ],
      message=('Multiple matching event types found. '
               'Please choose an event type:'),
      cancel_option=True)
  return possible_matches[index]


def GetSourceRef(name, namespace, source_crd, is_cluster):
  """Returns a resources.Resource from the given source_crd and name."""
  if is_cluster:
    collection_name = ANTHOS_SOURCE_COLLECTION_NAME.format(
        plural_kind=source_crd.source_kind_plural)
    api_version = 'v1beta1'
  else:
    collection_name = SOURCE_COLLECTION_NAME.format(
        plural_kind=source_crd.source_kind_plural)
    api_version = 'v1alpha1'
  return resources.REGISTRY.Parse(
      name, {'namespacesId': namespace},
      collection_name,
      api_version=api_version)


def GetSourceRefAndCrdForTrigger(trigger_obj, source_crds, is_cluster):
  """Returns a tuple of a source ref and its matching CRD."""
  # Get the source depedency on the trigger
  source_obj_ref = trigger_obj.dependency
  if source_obj_ref is None:
    return None, None

  # Determine which CRD matches the dependency source
  source_crd = next(
      (s for s in source_crds if s.source_kind == source_obj_ref.kind),
      None)
  if source_crd is None:
    return None, None

  return GetSourceRef(source_obj_ref.name, source_obj_ref.namespace, source_crd,
                      is_cluster), source_crd


def ValidateTrigger(trigger_obj, expected_source_obj, expected_event_type):
  """Validates the given trigger to reference the given source and event type.

  Args:
    trigger_obj: trigger.Trigger, the trigger to validate.
    expected_source_obj: source.Source, the source the trigger should reference.
    expected_event_type: custom_resource_definition.EventTYpe, the event type
      the trigger should reference.

  Raises:
    AssertionError if the trigger does not have matching values.
  """
  source_obj_ref = trigger_obj.dependency
  assert source_obj_ref == expected_source_obj.AsObjectReference()
  try:
    assert trigger_obj.filter_attributes[
        trigger.EVENT_TYPE_FIELD] == expected_event_type.type
  except KeyError:
    raise AssertionError


def WaitForCondition(poller, error_class):
  """Wait for a configuration to be ready in latest revision.

  Args:
    poller: A serverless_operations.ConditionPoller object.
    error_class: Error to raise on timeout failure

  Returns:
    A googlecloudsdk.command_lib.run.condition.Conditions object.

  Raises:
    error_class: Max retry limit exceeded.
  """

  try:
    return waiter.PollUntilDone(
        poller,
        None,
        max_wait_ms=_POLLING_TIMEOUT_MS,
        wait_ceiling_ms=_RETRY_TIMEOUT_MS)
  except retry.RetryException:
    conditions = poller.GetConditions()
    # err.message already indicates timeout. Check ready_cond_type for more
    # information.
    msg = conditions.DescriptiveMessage() if conditions else None
    raise error_class(msg)
