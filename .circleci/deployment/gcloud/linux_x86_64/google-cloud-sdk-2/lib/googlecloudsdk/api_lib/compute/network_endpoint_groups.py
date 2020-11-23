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
"""Network endpoint group api client."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import utils as api_utils
from googlecloudsdk.api_lib.compute.operations import poller
from googlecloudsdk.api_lib.util import waiter
from googlecloudsdk.command_lib.util.apis import arg_utils


class NetworkEndpointGroupsClient(object):
  """Client for network endpoint groups service in the GCE API."""

  def __init__(self, client, messages, resources):
    self.client = client
    self.messages = messages
    self.resources = resources
    self._zonal_service = self.client.apitools_client.networkEndpointGroups
    if hasattr(self.client.apitools_client, 'globalNetworkEndpointGroups'):
      self._global_service = self.client.apitools_client.globalNetworkEndpointGroups
    if hasattr(self.client.apitools_client, 'regionNetworkEndpointGroups'):
      self._region_service = self.client.apitools_client.regionNetworkEndpointGroups

  def Create(self,
             neg_ref,
             network_endpoint_type,
             default_port=None,
             network=None,
             subnet=None,
             cloud_run_service=None,
             cloud_run_tag=None,
             cloud_run_url_mask=None,
             app_engine_app=False,
             app_engine_service=None,
             app_engine_version=None,
             app_engine_url_mask=None,
             cloud_function_name=None,
             cloud_function_url_mask=None):
    """Creates a network endpoint group."""
    is_zonal = hasattr(neg_ref, 'zone')
    is_regional = hasattr(neg_ref, 'region')

    network_uri = None
    if network and is_zonal:
      network_ref = self.resources.Parse(network, {'project': neg_ref.project},
                                         collection='compute.networks')
      network_uri = network_ref.SelfLink()
    subnet_uri = None
    if subnet and is_zonal:
      region = api_utils.ZoneNameToRegionName(neg_ref.zone)
      subnet_ref = self.resources.Parse(
          subnet,
          {'project': neg_ref.project, 'region': region},
          collection='compute.subnetworks')
      subnet_uri = subnet_ref.SelfLink()

    cloud_run = None
    if cloud_run_service or cloud_run_tag or cloud_run_url_mask:
      cloud_run = self.messages.NetworkEndpointGroupCloudRun(
          service=cloud_run_service,
          tag=cloud_run_tag,
          urlMask=cloud_run_url_mask)
    app_engine = None
    if (app_engine_app or app_engine_service or app_engine_version or
        app_engine_url_mask):
      app_engine = self.messages.NetworkEndpointGroupAppEngine(
          service=app_engine_service,
          version=app_engine_version,
          urlMask=app_engine_url_mask)
    cloud_function = None
    if cloud_function_name or cloud_function_url_mask:
      cloud_function = self.messages.NetworkEndpointGroupCloudFunction(
          function=cloud_function_name, urlMask=cloud_function_url_mask)

    endpoint_type_enum = (self.messages.NetworkEndpointGroup
                          .NetworkEndpointTypeValueValuesEnum)

    # TODO(b/137663401): remove the check below after all Serverless flags go
    # to GA.
    if is_regional:
      network_endpoint_group = self.messages.NetworkEndpointGroup(
          name=neg_ref.Name(),
          networkEndpointType=arg_utils.ChoiceToEnum(network_endpoint_type,
                                                     endpoint_type_enum),
          defaultPort=default_port,
          network=network_uri,
          subnetwork=subnet_uri,
          cloudRun=cloud_run,
          appEngine=app_engine,
          cloudFunction=cloud_function)
    else:
      network_endpoint_group = self.messages.NetworkEndpointGroup(
          name=neg_ref.Name(),
          networkEndpointType=arg_utils.ChoiceToEnum(network_endpoint_type,
                                                     endpoint_type_enum),
          defaultPort=default_port,
          network=network_uri,
          subnetwork=subnet_uri)

    if is_zonal:
      request = self.messages.ComputeNetworkEndpointGroupsInsertRequest(
          networkEndpointGroup=network_endpoint_group,
          project=neg_ref.project,
          zone=neg_ref.zone)
      return self.client.MakeRequests([(self._zonal_service, 'Insert', request)
                                      ])[0]
    elif is_regional:
      request = self.messages.ComputeRegionNetworkEndpointGroupsInsertRequest(
          networkEndpointGroup=network_endpoint_group,
          project=neg_ref.project,
          region=neg_ref.region)
      return self.client.MakeRequests([(self._region_service, 'Insert', request)
                                      ])[0]
    else:
      request = self.messages.ComputeGlobalNetworkEndpointGroupsInsertRequest(
          networkEndpointGroup=network_endpoint_group, project=neg_ref.project)
      return self.client.MakeRequests([(self._global_service, 'Insert', request)
                                      ])[0]

  def _AttachZonalEndpoints(self, neg_ref, endpoints):
    """Attaches network endpoints to a zonal network endpoint group."""
    request_class = (
        self.messages.ComputeNetworkEndpointGroupsAttachNetworkEndpointsRequest)
    nested_request_class = (
        self.messages.NetworkEndpointGroupsAttachEndpointsRequest)
    request = request_class(
        networkEndpointGroup=neg_ref.Name(),
        project=neg_ref.project,
        zone=neg_ref.zone,
        networkEndpointGroupsAttachEndpointsRequest=nested_request_class(
            networkEndpoints=self._GetEndpointMessageList(endpoints)))
    return self._zonal_service.AttachNetworkEndpoints(request)

  def _DetachZonalEndpoints(self, neg_ref, endpoints):
    """Detaches network endpoints from a zonal network endpoint group."""
    request_class = (
        self.messages.ComputeNetworkEndpointGroupsDetachNetworkEndpointsRequest)
    nested_request_class = (
        self.messages.NetworkEndpointGroupsDetachEndpointsRequest)
    request = request_class(
        networkEndpointGroup=neg_ref.Name(),
        project=neg_ref.project,
        zone=neg_ref.zone,
        networkEndpointGroupsDetachEndpointsRequest=nested_request_class(
            networkEndpoints=self._GetEndpointMessageList(endpoints)))
    return self._zonal_service.DetachNetworkEndpoints(request)

  def _AttachGlobalEndpoints(self, neg_ref, endpoints):
    """Attaches network endpoints to a global network endpoint group."""
    request_class = (
        self.messages
        .ComputeGlobalNetworkEndpointGroupsAttachNetworkEndpointsRequest)
    nested_request_class = (
        self.messages.GlobalNetworkEndpointGroupsAttachEndpointsRequest)
    request = request_class(
        networkEndpointGroup=neg_ref.Name(),
        project=neg_ref.project,
        globalNetworkEndpointGroupsAttachEndpointsRequest=nested_request_class(
            networkEndpoints=self._GetEndpointMessageList(endpoints)))
    return self._global_service.AttachNetworkEndpoints(request)

  def _DetachGlobalEndpoints(self, neg_ref, endpoints):
    """Detaches network endpoints from a global network endpoint group."""
    request_class = (
        self.messages
        .ComputeGlobalNetworkEndpointGroupsDetachNetworkEndpointsRequest)
    nested_request_class = (
        self.messages.GlobalNetworkEndpointGroupsDetachEndpointsRequest)
    request = request_class(
        networkEndpointGroup=neg_ref.Name(),
        project=neg_ref.project,
        globalNetworkEndpointGroupsDetachEndpointsRequest=nested_request_class(
            networkEndpoints=self._GetEndpointMessageList(endpoints)))
    return self._global_service.DetachNetworkEndpoints(request)

  def _GetEndpointMessageList(self, endpoints):
    """Convert endpoints to a list which can be passed in a request."""
    output_list = []
    for arg_endpoint in endpoints:
      message_endpoint = self.messages.NetworkEndpoint()
      if 'instance' in arg_endpoint:
        message_endpoint.instance = arg_endpoint.get('instance')
      if 'ip' in arg_endpoint:
        message_endpoint.ipAddress = arg_endpoint.get('ip')
      if 'port' in arg_endpoint:
        message_endpoint.port = arg_endpoint.get('port')
      if 'fqdn' in arg_endpoint:
        message_endpoint.fqdn = arg_endpoint.get('fqdn')
      output_list.append(message_endpoint)

    return output_list

  def _GetOperationsRef(self, operation):
    return self.resources.Parse(operation.selfLink,
                                collection='compute.zoneOperations')

  def _GetGlobalOperationsRef(self, operation):
    return self.resources.Parse(
        operation.selfLink, collection='compute.globalOperations')

  def _WaitForResult(self, operation_poller, operation_ref, message):
    if operation_ref:
      return waiter.WaitFor(operation_poller, operation_ref, message)
    return None

  def Update(self, neg_ref, add_endpoints=None, remove_endpoints=None):
    """Updates a Compute Network Endpoint Group."""
    attach_endpoints_ref = None
    detach_endpoints_ref = None
    operation_poller = None

    if hasattr(neg_ref, 'zone'):
      operation_poller = poller.Poller(self._zonal_service)
      if add_endpoints:
        operation = self._AttachZonalEndpoints(neg_ref, add_endpoints)
        attach_endpoints_ref = self._GetOperationsRef(operation)
      if remove_endpoints:
        operation = self._DetachZonalEndpoints(neg_ref, remove_endpoints)
        detach_endpoints_ref = self._GetOperationsRef(operation)
    else:
      operation_poller = poller.Poller(self._global_service)
      if add_endpoints:
        operation = self._AttachGlobalEndpoints(neg_ref, add_endpoints)
        attach_endpoints_ref = self._GetGlobalOperationsRef(operation)
      if remove_endpoints:
        operation = self._DetachGlobalEndpoints(neg_ref, remove_endpoints)
        detach_endpoints_ref = self._GetGlobalOperationsRef(operation)

    neg_name = neg_ref.Name()
    result = None
    result = self._WaitForResult(
        operation_poller, attach_endpoints_ref,
        'Attaching {0} endpoints to [{1}].'.format(
            len(add_endpoints) if add_endpoints else 0, neg_name)) or result
    result = self._WaitForResult(
        operation_poller, detach_endpoints_ref,
        'Detaching {0} endpoints from [{1}].'.format(
            len(remove_endpoints) if remove_endpoints else 0, neg_name)
    ) or result

    return result
