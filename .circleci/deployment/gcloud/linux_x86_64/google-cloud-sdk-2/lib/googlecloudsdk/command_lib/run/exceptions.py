# -*- coding: utf-8 -*- #
# Copyright 2018 Google LLC. All Rights Reserved.
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
"""This module holds exceptions raised by Cloud Run commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
import re

from googlecloudsdk.api_lib.util import exceptions as exceptions_util
from googlecloudsdk.core import exceptions


class BucketAccessError(exceptions.Error):
  """Indicates a failed attempt to access a GCS bucket."""
  pass


class ConfigurationError(exceptions.Error):
  """Indicates an error in configuration."""
  pass


class ServiceNotFoundError(exceptions.Error):
  """Indicates that a provided service name was not found."""
  pass


class RevisionNotFoundError(exceptions.Error):
  """Indicates that a provided revision name was not found."""
  pass


class DockerVersionError(exceptions.Error):
  """Indicates an error in determining the docker version."""
  pass


class AmbiguousContainerError(exceptions.Error):
  """More than one container fits our criteria, we do not know which to run."""
  pass


class CloudSQLError(exceptions.Error):
  """Malformed instances string for CloudSQL."""
  pass


class ContainerIdError(exceptions.Error):
  """Container Id cannot be found by docker."""
  pass


class NoActiveRevisionsError(exceptions.Error):
  """Active revisions were expected but not found."""
  pass


class SourceNotSupportedError(exceptions.Error):
  """Your Cloud Run install does not support source deployment."""
  pass


class NoConfigurationChangeError(exceptions.Error):
  """No configuration changes were requested."""
  pass


class UnknownDeployableError(exceptions.Error):
  """Could not identify the deployable app, function, or container."""
  pass


class AppNotReadyError(exceptions.InternalError):
  """The application must be uploaded before it can be deployed."""
  pass


class DeploymentFailedError(exceptions.Error):
  """An error was encountered during deployment."""
  pass


class DomainMappingCreationError(exceptions.Error):
  """An error was encountered during the creation of a domain mapping."""


class DomainMappingAlreadyExistsError(DomainMappingCreationError):
  """Domain mapping already exists in another project, GCP service, or region.

  This indicates a succesfully created DomainMapping resource but with the
  domain it intends to map being unavailable because it's already in use.
  Not to be confused with a 409 error indicating a DomainMapping resource with
  this same name (the domain name) already exists in this region.
  """


class PlatformError(exceptions.Error):
  """Command not supported for the platform."""


class NoTLSError(exceptions.Error):
  """TLS 1.2 support is required to connect to GKE.

  Your Python installation does not support TLS 1.2. For Python2, please upgrade
  to version 2.7.9 or greater; for Python3, please upgrade to version 3.4 or
  greater.
  """


class HttpError(exceptions_util.HttpException):
  """More prettily prints apitools HttpError."""

  def __init__(self, error):
    super(HttpError, self).__init__(error)
    if self.payload.field_violations:
      self.error_format = '\n'.join([
          '{{field_violations.{}}}'.format(k)
          for k in self.payload.field_violations.keys()
      ])


class FieldMismatchError(exceptions.Error):
  """Given field value doesn't match the expected type."""


# Should match
# https://github.com/google/apitools/blob/02db277e2bbc5906c8787f64dc9a743fe3327f90/apitools/base/protorpclite/messages.py#L1338-L1340..
# apitools reraises the ValidationError as its own InvalidDataFromServerError
# https://github.com/google/apitools/blob/ecbcd3e9e5ec44826d35fd0d0a35387c4d66c2b9/apitools/base/py/base_api.py#L449-L451
# so the start of the regex is to account for the additional error message
# prefix added by that.
VALIDATION_ERROR_MSG_REGEX = re.compile(
    r'^.*(?:\n.*)*Expected type .+? for field (.+?), found (.+?) \(type .+?\)',
    re.MULTILINE)


def MaybeRaiseCustomFieldMismatch(error):
  """Special handling for port field type mismatch.

  Due to differences in golang structs used by clusters and proto messages used
  by gcloud, some invalid service responses should be specially handled.
  See b/149365868#comment5 for more info.

  Args:
    error: original error complaining of a type mismatch.
  Raises:
    FieldMismatchError: If the error is due to our own custom handling or the
      original error if not.
  """
  regex_match = VALIDATION_ERROR_MSG_REGEX.match(str(error))
  if regex_match and regex_match.group(1) == 'port':
    raise FieldMismatchError(
        'Error decoding the "port" field. Only integer ports are supported '
        'by gcloud. Please change your port from "{}" to an integer value to '
        'be compatible with gcloud.'.format(regex_match.group(2)))
  else:
    raise error


class KubernetesError(exceptions.Error):
  """A generic kubernetes error was encountered."""


class UnsupportedOperationError(exceptions.Error):
  """The requested operation is not supported."""
  pass


class KubernetesExceptionParser(object):
  """Converts a kubernetes exception to an object."""

  def __init__(self, http_error):
    """Wraps a generic http error returned by kubernetes.

    Args:
      http_error: apitools.base.py.exceptions.HttpError, The error to wrap.
    """
    self._wrapped_error = http_error
    self._content = json.loads(http_error.content)

  @property
  def status_code(self):
    try:
      return self._wrapped_error.status_code
    except KeyError:
      return None

  @property
  def url(self):
    return self._wrapped_error.url

  @property
  def api_version(self):
    try:
      return self._content['apiVersion']
    except KeyError:
      return None

  @property
  def api_name(self):
    try:
      return self._content['details']['group']
    except KeyError:
      return None

  @property
  def resource_name(self):
    try:
      return self._content['details']['name']
    except KeyError:
      return None

  @property
  def resource_kind(self):
    try:
      return self._content['details']['kind']
    except KeyError:
      return None

  @property
  def default_message(self):
    try:
      return self._content['message']
    except KeyError:
      return None

  @property
  def error(self):
    return self._wrapped_error

  @property
  def causes(self):
    """Returns list of causes uniqued by the message."""
    try:
      messages = {
          c['message']: c for c in self._content['details'][
              'causes']}
      return [messages[k] for k in sorted(messages)]
    except KeyError:
      return []
