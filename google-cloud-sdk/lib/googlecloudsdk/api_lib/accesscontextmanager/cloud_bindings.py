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
"""API library for access context manager cloud-bindings."""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.accesscontextmanager import util


class Client(object):
  """Client for Access Context Manager Access cloud-bindings service."""

  def __init__(self, client=None, messages=None, version=None):
    self.client = client or util.GetClient(version=version)
    self.messages = messages or self.client.MESSAGES_MODULE

  def Patch(self, binding_ref, level_ref):
    """Patch a gcpUserAccessBinding.

    Args:
      binding_ref: resources.Resource, reference to the binding to patch
      level_ref: resources.Resource, reference to the updating access level

    Returns:
      GcpUserAccessBinding, the updated gcpUserAccessBinding
    """
    update_mask = ['access_levels']

    m = self.messages
    request_type = m.AccesscontextmanagerOrganizationsGcpUserAccessBindingsPatchRequest
    request = request_type(
        gcpUserAccessBinding=m.GcpUserAccessBinding(
            accessLevels=[level_ref.RelativeName()]),
        name=binding_ref.RelativeName(),
        updateMask=','.join(update_mask),
    )
    operation = self.client.organizations_gcpUserAccessBindings.Patch(request)
    return operation.response

  def Create(self, org_ref, group_key, level_ref):
    """Create a gcpUserAccessBinding.

    Args:
      org_ref: resources.Resource, reference to the parent organization
      group_key: str, Google Group id whose members are subject to the
        restrictions of this binding.
      level_ref: resources.Resource, reference to the access level.

    Returns:
      GcpUserAccessBinding, the created gcpUserAccessBinding
    """

    m = self.messages
    request_type = m.AccesscontextmanagerOrganizationsGcpUserAccessBindingsCreateRequest
    request = request_type(
        gcpUserAccessBinding=m.GcpUserAccessBinding(
            groupKey=group_key, accessLevels=[level_ref.RelativeName()]),
        parent=org_ref.RelativeName())
    operation = self.client.organizations_gcpUserAccessBindings.Create(request)
    return operation.response
