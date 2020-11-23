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

"""A module to get a credentialed transport object for making API calls."""


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from googlecloudsdk.core.credentials import http
from googlecloudsdk.core.credentials import requests


def GetApitoolsTransport(timeout='unset',
                         enable_resource_quota=True,
                         force_resource_quota=False,
                         response_encoding=None,
                         ca_certs=None,
                         allow_account_impersonation=True,
                         use_google_auth=None):
  """Get an transport client for use with apitools.

  Args:
    timeout: double, The timeout in seconds to pass to httplib2.  This is the
        socket level timeout.  If timeout is None, timeout is infinite.  If
        default argument 'unset' is given, a sensible default is selected.
    enable_resource_quota: bool, By default, we are going to tell APIs to use
        the quota of the project being operated on. For some APIs we want to use
        gcloud's quota, so you can explicitly disable that behavior by passing
        False here.
    force_resource_quota: bool, If true resource project quota will be used by
      this client regardless of the settings in gcloud. This should be used for
      newer APIs that cannot work with legacy project quota.
    response_encoding: str, the encoding to use to decode the response.
    ca_certs: str, absolute filename of a ca_certs file that overrides the
        default
    allow_account_impersonation: bool, True to allow use of impersonated service
      account credentials for calls made with this client. If False, the active
      user credentials will always be used.
    use_google_auth: bool, True if the calling command indicates to use
      google-auth library for authentication. If False, authentication will
      fallback to using the oauth2client library.

  Returns:
    1. A httplib2.Http-like object backed by httplib2 or requests.
  """
  if base.UseRequests():
    session = requests.GetSession(
        timeout=timeout,
        enable_resource_quota=enable_resource_quota,
        force_resource_quota=force_resource_quota,
        response_encoding=response_encoding,
        ca_certs=ca_certs,
        allow_account_impersonation=allow_account_impersonation)

    return requests.GetApitoolsRequests(session)

  return http.Http(timeout=timeout,
                   enable_resource_quota=enable_resource_quota,
                   force_resource_quota=force_resource_quota,
                   response_encoding=response_encoding,
                   ca_certs=ca_certs,
                   allow_account_impersonation=allow_account_impersonation,
                   use_google_auth=use_google_auth)
