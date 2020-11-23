# Lint as: python3
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
"""Hooks for Privateca surface."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.privateca import base
from googlecloudsdk.api_lib.privateca import request_utils
from googlecloudsdk.command_lib.privateca import resource_args
from googlecloudsdk.core.util import times


def CheckResponseSubordinateTypeHook(response, unused_args):
  """Raises an exception if the response is not a subordinate ca."""
  resource_args.CheckExpectedCAType(
      base.GetMessagesModule().CertificateAuthority.TypeValueValuesEnum
      .SUBORDINATE, response)
  return response


def CheckResponseRootTypeHook(response, unused_args):
  """Raises an exception if the response is not a root ca."""
  resource_args.CheckExpectedCAType(
      base.GetMessagesModule().CertificateAuthority.TypeValueValuesEnum
      .SELF_SIGNED, response)
  return response


def _CheckRequestTypeHook(resource_ref, expected_type):
  """Do a get on a CA resource and check its type against expected_type."""
  client = base.GetClientInstance()
  messages = base.GetMessagesModule()
  certificate_authority = client.projects_locations_certificateAuthorities.Get(
      messages.PrivatecaProjectsLocationsCertificateAuthoritiesGetRequest(
          name=resource_ref.RelativeName()))

  resource_args.CheckExpectedCAType(expected_type, certificate_authority)


def CheckRequestRootTypeHook(resource_ref, unused_args, request):
  """Raises an exception if the request is not for a root ca."""
  _CheckRequestTypeHook(
      resource_ref,
      base.GetMessagesModule().CertificateAuthority.TypeValueValuesEnum
      .SELF_SIGNED)

  return request


def CheckRequestSubordinateTypeHook(resource_ref, unused_args, request):
  """Raises an exception if the request is not for a subordinate ca."""
  _CheckRequestTypeHook(
      resource_ref,
      base.GetMessagesModule().CertificateAuthority.TypeValueValuesEnum
      .SUBORDINATE)
  return request


def AddRequestIdHook(unused_ref, unused_args, request):
  """Fills a unique identifier for a request with a requestId field."""
  request.requestId = request_utils.GenerateRequestId()
  return request


def _ConvertProtoToIsoDuration(proto_duration_str):
  """Convert a given 'proto duration' string to an ISO8601 duration string."""
  return times.FormatDuration(times.ParseDuration(proto_duration_str, True))


def ConvertCertificateLifetimeToIso8601(response, unused_args):
  """Converts certificate lifetimes from proto duration format to ISO8601."""

  # These fields could be None if the user specifies a filter that omits them.
  if response.lifetime:
    response.lifetime = _ConvertProtoToIsoDuration(response.lifetime)
  if (response.certificateDescription and
      response.certificateDescription.subjectDescription and
      response.certificateDescription.subjectDescription.lifetime):
    response.certificateDescription.subjectDescription.lifetime = _ConvertProtoToIsoDuration(
        response.certificateDescription.subjectDescription.lifetime)

  return response
