# Lint as: python3 # -*- coding: utf-8 -*- #
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
"""Generic implementations of Apigee Management APIs."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.command_lib.apigee import request


class BaseClient(object):
  """Base class for Apigee Management API clients."""

  _entity_path = None
  """List of identifiers that uniquely identify the object.

  Must be in the same order as the REST API expects.
  """

  @classmethod
  def List(cls, identifiers=None, extra_params=None):
    if cls._entity_path is None:
      raise NotImplementedError("%s class must provide an entity path." % cls)
    return request.ResponseToApiRequest(
        identifiers or {},
        cls._entity_path[:-1],
        cls._entity_path[-1],
        query_params=extra_params)

  @classmethod
  def Describe(cls, identifiers=None):
    if cls._entity_path is None:
      raise NotImplementedError("%s class must provide an entity path." % cls)
    return request.ResponseToApiRequest(identifiers or {}, cls._entity_path)

  @classmethod
  def Delete(cls, identifiers=None):
    if cls._entity_path is None:
      raise NotImplementedError("%s class must provide an entity path." % cls)
    return request.ResponseToApiRequest(
        identifiers or {}, cls._entity_path, method="DELETE")


class PagedListClient(BaseClient):
  """Client for `List` APIs that can only return a limited number of objects.

  Attributes:
    _list_container: the field name in the List API's response that contains the
      list of objects. None if the API returns a list directly.
    _page_field: the field name in each list element that can be used as a page
      identifier. PageListClient will take the value of this field in the last
      list item for a page, and use it as the  _start_at_param for the next
      page. None if each list element is a primitive which can be used for this
      purpose directly.
    _max_per_page: the maximum number of items that can be returned in each List
      response.
    _limit_param: the query parameter for the number of items to be returned on
      each page.
    _start_at_param: the query parameger for where in the available data the
      response should begin.
  """

  _list_container = None
  _page_field = None
  _max_per_page = 1000
  _limit_param = "count"
  _start_at_param = "startKey"

  @classmethod
  def List(cls, identifiers=None, start_at_param=None, extra_params=None):
    if start_at_param is None:
      start_at_param = cls._start_at_param
    params = {cls._limit_param: cls._max_per_page}
    if extra_params:
      params.update(extra_params)
    while True:
      result_chunk = super(PagedListClient, cls).List(identifiers, params)
      if not result_chunk and start_at_param not in params:
        # First request returned no rows; entire dataset is empty.
        return

      if cls._list_container is not None:
        # This API is expected to return a dictionary with a list inside it.
        # Extract that list out of the dictionary.
        try:
          result_chunk = result_chunk[cls._list_container]
        except KeyError:
          failure_info = (cls, cls._list_container, result_chunk)
          raise AssertionError(
              "%s specifies a _list_container %r that's not present in API "
              "responses.\nResponse: %r" % failure_info)
        except (IndexError, TypeError):
          error = ("%s specifies a _list_container, implying that the API "
                   "response should be a JSON object, but received something "
                   "else instead: %r") % (cls, result_chunk)
          raise AssertionError(error)
      # Don't include the last item in a full page; it will be included as the
      # first item of the next page instead.
      for item in result_chunk[:cls._max_per_page - 1]:
        yield item

      if len(result_chunk) < cls._max_per_page:
        # Server didn't have enough values to fill the page, so all results have
        # been received.
        break

      last_item_on_page = result_chunk[-1]
      if cls._page_field is not None:
        last_item_on_page = last_item_on_page[cls._page_field]

      params[start_at_param] = last_item_on_page
