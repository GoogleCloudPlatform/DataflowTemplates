# -*- coding: utf-8 -*- # Lint as: python3
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
"""Error classes for gcloud Apigee commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections
import json
from googlecloudsdk.core import exceptions
from googlecloudsdk.core import yaml
import six


def _GetResourceIdentifierString(resource_type, resource_identifier):
  """Returns a human readable string representation of a resource identifier.

  Args:
    resource_type: the type of resource identified by `resource_identifier`.
    resource_identifier: an ordered mapping representing a fully qualified
      identifier for an Apigee Management API resource.
  """
  name_words = [word[0].upper() + word[1:] for word in resource_type.split(" ")]
  capitalized_type = "".join(name_words)

  # Format as a namedtuple repr; it cleanly handles special character escaping
  # while keeping everything on one line.
  tuple_type = collections.namedtuple(capitalized_type,
                                      resource_identifier.keys())

  return repr(tuple_type(**dict(resource_identifier)))


def _GetErrorDetailsSummary(error_info):
  """Returns a string summarizing `error_info`.

  Attempts to interpret error_info as an error JSON returned by the Apigee
  management API. If successful, the returned string will be an error message
  from that data structure - either its top-level error message, or a list of
  precondition violations.

  If `error_info` can't be parsed, or has no known error message, returns a YAML
  formatted copy of `error_info` instead.

  Args:
    error_info: a dictionary containing the error data structure returned by the
      Apigee Management API.
  """
  try:
    if "details" in error_info:
      # Error response might have info on exactly what preconditions failed or
      # what about the arguments was invalid.
      violations = []
      for item in error_info["details"]:
        # Include only those details whose format is known.
        detail_types = (
            "type.googleapis.com/google.rpc.QuotaFailure",
            "type.googleapis.com/google.rpc.PreconditionFailure",
            "type.googleapis.com/edge.configstore.bundle.BadBundle",
            )
        if item["@type"] in detail_types and "violations" in item:
          violations += item["violations"]
      descriptions = [violation["description"] for violation in violations]
      if descriptions:
        return error_info["message"] + "\n" + yaml.dump(descriptions)

    # Almost always seems to be included.
    return error_info["message"]

  except KeyError:
    # Format of the error details is not as expected. As a fallback, just give
    # the user the whole thing.
    return "\n" + yaml.dump(error_info)


class AmbiguousRequestError(exceptions.Error):
  """Raised when the user makes a request for an ambiguously defined resource.

  Sometimes arguments are optional in the general case because their correct
  values can generally be inferred, but required for cases when that inferrence
  isn't possible. This error covers that scenario.
  """
  pass


class RequestError(exceptions.Error):
  """Raised when a request to the Apigee Management API has failed."""

  def __init__(self,
               resource_type=None,
               resource_identifier=None,
               method=None,
               reason=None,
               body=None,
               message=None):
    self.details = None
    if body:
      try:
        # In older versions of Python 3, the built-in JSON library will only
        # accept strings, not bytes.
        if not isinstance(body, str) and hasattr(body, "decode"):
          body = body.decode()
        self.details = json.loads(body)
        if "error" in self.details:
          self.details = self.details["error"]
      except ValueError:
        pass
    self.reason = reason
    self.resource_type = resource_type
    self.resource_identifier = resource_identifier

    if not message:
      if not method:
        method = "access"
      if not resource_type:
        resource_type = "resource"
      message = "Failed to %s %s" % (method, resource_type)
      if reason:
        message += " (%s)" % (reason) if reason else ""
      if resource_identifier:
        message += ":\n" + _GetResourceIdentifierString(resource_type,
                                                        resource_identifier)
      if self.details:
        message += "\nDetails: " + _GetErrorDetailsSummary(self.details)

    super(RequestError, self).__init__(message)

  def RewrittenError(self, resource_type, method):
    """Returns a copy of the error with a new resource type and method."""
    body = json.dumps(self.details) if self.details else None
    return type(self)(
        resource_type, self.resource_identifier, method=method,
        reason=self.reason, body=body)


class ResponseNotJSONError(RequestError):
  """Raised when a request to the Apigee API returns a malformed response."""

  def __init__(self,
               error,
               resource_type=None,
               resource_identifier=None,
               body=None):
    if all(hasattr(error, attr) for attr in ["msg", "lineno", "colno"]):
      reason = "%s at %d:%d" % (error.msg, error.lineno, error.colno)
    else:
      reason = six.text_type(error)
    super(ResponseNotJSONError,
          self).__init__(resource_type, resource_identifier, "parse", reason,
                         json.dumps({"response": body}))
    self.base_error = error

  def RewrittenError(self, resource_type, method):
    """Returns a copy of the error with a new resource type."""
    body = self.details["response"] if self.details else None
    return type(self)(
        self.base_error, resource_type, self.resource_identifier, body=body)


class UnauthorizedRequestError(RequestError):
  """Raised when a request to the Apigee API had insufficient privileges."""

  def __init__(self,
               resource_type=None,
               resource_identifier=None,
               method=None,
               reason=None,
               body=None,
               message=None):
    resource_type = resource_type or "resource"
    method = method or "access"
    if not message:
      message = "Insufficient privileges to %s the requested %s" % (
          method, resource_type)
      if reason:
        message += "; " + reason
      if resource_identifier:
        message += "\nRequested: " + _GetResourceIdentifierString(
            resource_type, resource_identifier)
    super(UnauthorizedRequestError,
          self).__init__(resource_type, resource_identifier, method, reason,
                         body, message)

  pass


class EntityNotFoundError(RequestError):
  """Raised when a request to the Apigee API mentions a nonexistant resource."""

  def __init__(self,
               resource_type=None,
               resource_identifier=None,
               method=None,
               reason=None,
               body=None,
               message=None):
    resource_type = resource_type or "resource"
    if not message:
      message = "Requested %s does not exist" % (resource_type)
      if resource_identifier:
        message += ": " + _GetResourceIdentifierString(resource_type,
                                                       resource_identifier)
    super(EntityNotFoundError,
          self).__init__(resource_type, resource_identifier, method, reason,
                         body, message)


class MissingIdentifierError(exceptions.Error):
  """Raised when a request to the Apigee API is missing an expected identifier.

  Normally this situation should be caught by a required argument being missing
  or similar; this error is a fallback in case a corner case slips past those
  higher level checks.
  """

  def __init__(self, name):
    message = "Command requires a %s but no %s was provided." % (name, name)
    super(MissingIdentifierError, self).__init__(message)
