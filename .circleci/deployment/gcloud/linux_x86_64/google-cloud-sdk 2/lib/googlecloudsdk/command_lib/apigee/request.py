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
"""Generalized Apigee Management API request handler.

The Apigee Management APIs were designed before One Platform, and include some
design decisions incompatible with apitools (see b/151099218). So the gcloud
apigee surface must make its own HTTPS requests instead of relying on an
apitools-generated client.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections
import json

from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.apigee import errors
from googlecloudsdk.command_lib.apigee import resource_args
from googlecloudsdk.core import properties
from googlecloudsdk.core.credentials import http
from googlecloudsdk.core.credentials import requests
from six.moves import urllib


APIGEE_HOST = "apigee.googleapis.com"


def _ResourceIdentifier(identifiers, entity_path):
  """Returns an OrderedDict uniquely identifying the resource to be accessed.

  Args:
    identifiers: a collection that maps entity type names to identifiers.
    entity_path: a list of entity type names from least to most specific.

  Raises:
    MissingIdentifierError: an entry in entity_path is missing from
      `identifiers`.
  """
  resource_identifier = collections.OrderedDict()

  for entity_name in entity_path:
    entity = resource_args.ENTITIES[entity_name]
    id_key = entity.plural + "Id"
    if id_key not in identifiers or identifiers[id_key] is None:
      raise errors.MissingIdentifierError(entity.singular)
    resource_identifier[entity] = identifiers[id_key]
  return resource_identifier


def _Communicate(url, method, body, headers):
  """Returns HTTP status, reason, and response body for a given HTTP request."""
  if base.UseRequests():
    response = requests.GetSession().request(
        method, url, data=body, headers=headers, stream=True)
    status = response.status_code
    reason = response.reason
    data = response.content
    return status, reason, data

  response, data = http.Http().request(
      url, method, body=body, headers=headers)
  status = response.status
  reason = response.reason
  return status, reason, data


def ResponseToApiRequest(identifiers,
                         entity_path,
                         entity_collection=None,
                         method="GET",
                         query_params=None,
                         accept_mimetype=None,
                         body=None,
                         body_mimetype="application/json"):
  """Makes a request to the Apigee API and returns the response.

  Args:
    identifiers: a collection that maps entity type names to identifiers.
    entity_path: a list of entity type names from least to most specific.
    entity_collection: if provided, the final entity type; the request will not
      be specific as to which entity of that type is being referenced.
    method: an HTTP method string specifying what to do with the accessed
      entity. If the method begins with a colon, it will be interpreted as a
      Cloud custom method (https://cloud.google.com/apis/design/custom_methods)
      and appended to the request URL with the POST HTTP method.
    query_params: any extra query parameters to be sent in the request.
    accept_mimetype: the mimetype to expect in the response body. If not
      provided, the response will be parsed as JSON.
    body: data to send in the request body.
    body_mimetype: the mimetype of the body data, if not JSON.

  Returns:
    an object containing the API's response. If accept_mimetype was set, this
      will be raw bytes. Otherwise, it will be a parsed JSON object.

  Raises:
    MissingIdentifierError: an entry in entity_path is missing from
      `identifiers`.
    RequestError: if the request itself fails.
  """
  headers = {}
  if body:
    headers["Content-Type"] = body_mimetype
  if accept_mimetype:
    headers["Accept"] = accept_mimetype

  resource_identifier = _ResourceIdentifier(identifiers, entity_path)
  url_path_elements = ["v1"]
  for key, value in resource_identifier.items():
    url_path_elements += [key.plural, urllib.parse.quote(value)]
  if entity_collection:
    collection_name = resource_args.ENTITIES[entity_collection].plural
    url_path_elements.append(urllib.parse.quote(collection_name))

  query_string = urllib.parse.urlencode(query_params) if query_params else ""

  endpoint_override = properties.VALUES.api_endpoint_overrides.apigee.Get()
  if endpoint_override:
    endpoint = urllib.parse.urlparse(endpoint_override)
    scheme = endpoint.scheme
    host = endpoint.netloc
  else:
    scheme = "https"
    host = APIGEE_HOST

  url_path = "/".join(url_path_elements)
  if method and method[0] == ":":
    url_path += method
    method = "POST"
  url = urllib.parse.urlunparse((scheme, host, url_path, "", query_string, ""))

  status, reason, response = _Communicate(url, method, body, headers)

  if status >= 400:
    resource_type = entity_collection or entity_path[-1]
    if status == 404:
      exception_class = errors.EntityNotFoundError
    elif status in (401, 403):
      exception_class = errors.UnauthorizedRequestError
    else:
      exception_class = errors.RequestError
    error_identifier = collections.OrderedDict([
        (key.singular, value) for key, value in resource_identifier.items()
    ])
    raise exception_class(resource_type, error_identifier, method,
                          reason, response)

  if accept_mimetype is None:
    try:
      # In older versions of Python 3, the built-in JSON library will only
      # accept strings, not bytes.
      if not isinstance(response, str) and hasattr(response, "decode"):
        response = response.decode()
      response = json.loads(response)
    except ValueError as error:
      resource_type = (
          entity_collection if entity_collection else entity_path[-1])
      error_identifier = collections.OrderedDict([
          (key.singular, value) for key, value in resource_identifier.items()
      ])
      raise errors.ResponseNotJSONError(error, resource_type, error_identifier,
                                        response)

  return response
