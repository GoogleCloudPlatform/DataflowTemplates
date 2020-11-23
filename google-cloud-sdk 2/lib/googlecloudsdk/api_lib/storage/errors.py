# -*- coding: utf-8 -*- #
# Copyright 2020 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""API interface for interacting with cloud storage providers."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.util import exceptions as api_exceptions
from googlecloudsdk.core import exceptions as core_exceptions


class CloudApiError(core_exceptions.Error):
  pass


class GcsApiError(CloudApiError, api_exceptions.HttpException):
  pass


class NotFoundError(CloudApiError):
  """Error raised when the requested resource does not exist.

  Both GCS and S3 APIs should raise this same error if a resource
  does not exist so that the caller can handle the error in an API agnostic
  manner.
  """
  pass


class S3ErrorPayload(api_exceptions.FormattableErrorPayload):
  """Allows using format strings to create strings from botocore ClientErrors.

  Format strings of the form '{field_name}' will be populated from class
  attributes. Strings of the form '{.field_name}' will be populated from the
  self.content JSON dump. See api_lib.util.HttpErrorPayload for more detail and
  sample usage.

  Attributes:
    content (dict): The dumped JSON content.
    message (str): The human readable error message.
    status_code (int): The HTTP status code number.
    status_description (str): The status_code description.
    status_message (str): Context specific status message.
  """

  def __init__(self, client_error):
    """Initializes an S3ErrorPayload instance.

    Args:
      client_error (Union[botocore.exceptions.ClientError, str]): The error
        thrown by botocore, or a string that will be displayed as the error
        message.
    """
    super().__init__(client_error)
    # TODO(b/170215786): Remove botocore_error_string attribute when S3 api
    # tests no longer expect the botocore error format.
    self.botocore_error_string = str(client_error)
    if not isinstance(client_error, str):
      self.content = client_error.response
      if 'ResponseMetadata' in client_error.response:
        self.status_code = client_error.response['ResponseMetadata'].get(
            'HttpStatusCode', 0)
      if 'Error' in client_error.response:
        error = client_error.response['Error']
        self.status_description = error.get('Code', '')
        self.status_message = error.get('Message', '')
      self.message = self._MakeGenericMessage()


class S3ApiError(CloudApiError, api_exceptions.HttpException):
  """Translates a botocore ClientError and allows formatting.

  Attributes:
    error: The original ClientError.
    error_format: An S3ErrorPayload format string.
    payload: The S3ErrorPayload object.
  """

  # TODO(b/170215786): Set error_format=None when S3 api tests no longer expect
  # the botocore error format.
  def __init__(self, error, error_format='{botocore_error_string}'):
    super().__init__(
        error, error_format=error_format, payload_class=S3ErrorPayload)


def catch_error_raise_cloud_api_error(untranslated_error_class,
                                      cloud_api_error_class,
                                      format_str=None):
  """Decorator catches an error and raises CloudApiError with a custom message.

  Args:
    untranslated_error_class (Exception): An error class that needs to be
      translated to a CloudApiError.
    cloud_api_error_class (CloudApiError): A subclass of CloudApiError to be
      raised instead of untranslated_error_class.
    format_str (str): An api_lib.util.exceptions.FormattableErrorPayload format
      string. Note that any properties that are accessed here are on the
      FormattableErrorPayload object, not the object returned from the server.

  Returns:
    A decorator that catches errors and raises a CloudApiError with a
      customizable error message.

  Example:
    @catch_error_raise_cloud_api_error(apitools_exceptions.HttpError,
        GcsApiError, 'Error [{status_code}]')
    def some_func_that_might_throw_an_error():
  """

  def translate_api_error_decorator(function):
    # Need to define a secondary wrapper to get an argument to the outer
    # decorator.
    def wrapper(*args, **kwargs):
      try:
        return function(*args, **kwargs)
      except untranslated_error_class as error:
        cloud_api_error = cloud_api_error_class(error, format_str)
        core_exceptions.reraise(cloud_api_error)

    return wrapper

  return translate_api_error_decorator
