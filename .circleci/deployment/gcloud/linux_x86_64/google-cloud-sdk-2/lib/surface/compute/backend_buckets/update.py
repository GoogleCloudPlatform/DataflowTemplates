# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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
"""Commands for updating backend buckets."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.compute import cdn_flags_utils as cdn_flags
from googlecloudsdk.command_lib.compute import signed_url_flags
from googlecloudsdk.command_lib.compute.backend_buckets import backend_buckets_utils
from googlecloudsdk.command_lib.compute.backend_buckets import flags as backend_buckets_flags
from googlecloudsdk.core import log


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Update(base.UpdateCommand):
  """Update a backend bucket.

  *{command}* is used to update backend buckets.
  """

  BACKEND_BUCKET_ARG = None
  _support_flexible_cache_step_one = False

  @classmethod
  def Args(cls, parser):
    """Set up arguments for this command."""
    backend_buckets_flags.AddUpdatableArgs(cls, parser, 'update')
    backend_buckets_flags.GCS_BUCKET_ARG.AddArgument(parser)
    signed_url_flags.AddSignedUrlCacheMaxAge(
        parser, required=False, unspecified_help='')

    if cls._support_flexible_cache_step_one:
      cdn_flags.AddFlexibleCacheStepOne(
          parser, 'backend bucket', update_command=True)

  def AnyArgsSpecified(self, args):
    """Returns true if any args for updating backend bucket were specified."""
    return (args.IsSpecified('description') or
            args.IsSpecified('gcs_bucket_name') or
            args.IsSpecified('enable_cdn'))

  def AnyFlexibleCacheArgsSpecified(self, args):
    """Returns true if any Flexible Cache args for updating backend bucket were specified."""
    return self._support_flexible_cache_step_one and any(
        (args.IsSpecified('cache_mode'), args.IsSpecified('client_ttl'),
         args.IsSpecified('no_client_ttl'), args.IsSpecified('default_ttl'),
         args.IsSpecified('no_default_ttl'), args.IsSpecified('max_ttl'),
         args.IsSpecified('no_max_ttl'), args.IsSpecified('negative_caching'),
         args.IsSpecified('negative_caching_policy'),
         args.IsSpecified('no_negative_caching_policies'),
         args.IsSpecified('custom_response_header'),
         args.IsSpecified('no_custom_response_headers')))

  def GetGetRequest(self, client, backend_bucket_ref):
    """Returns a request to retrieve the backend bucket."""
    return (
        client.apitools_client.backendBuckets,
        'Get',
        client.messages.ComputeBackendBucketsGetRequest(
            project=backend_bucket_ref.project,
            backendBucket=backend_bucket_ref.Name()))

  def GetSetRequest(self, client, backend_bucket_ref, replacement):
    """Returns a request to update the backend bucket."""
    return (
        client.apitools_client.backendBuckets,
        'Patch',
        client.messages.ComputeBackendBucketsPatchRequest(
            project=backend_bucket_ref.project,
            backendBucket=backend_bucket_ref.Name(),
            backendBucketResource=replacement))

  def Modify(self, args, existing):
    """Modifies and returns the updated backend bucket."""
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client
    replacement = encoding.CopyProtoMessage(existing)
    cleared_fields = []

    if args.IsSpecified('description'):
      replacement.description = args.description

    if args.gcs_bucket_name:
      replacement.bucketName = args.gcs_bucket_name

    if args.enable_cdn is not None:
      replacement.enableCdn = args.enable_cdn

    backend_buckets_utils.ApplyCdnPolicyArgs(
        client,
        args,
        replacement,
        is_update=True,
        cleared_fields=cleared_fields,
        support_flexible_cache_step_one=self._support_flexible_cache_step_one)

    if self._support_flexible_cache_step_one:
      if args.custom_response_header is not None:
        replacement.customResponseHeaders = args.custom_response_header
      if args.no_custom_response_headers:
        replacement.customResponseHeaders = []
      if not replacement.customResponseHeaders:
        cleared_fields.append('customResponseHeaders')
      if (replacement.cdnPolicy is not None and
          replacement.cdnPolicy.cacheMode and args.enable_cdn is not False):  # pylint: disable=g-bool-id-comparison
        replacement.enableCdn = True

    if not replacement.description:
      cleared_fields.append('description')
    return replacement, cleared_fields

  def MakeRequests(self, args):
    """Makes the requests for updating the backend bucket."""
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    backend_bucket_ref = self.BACKEND_BUCKET_ARG.ResolveAsResource(
        args, holder.resources)
    get_request = self.GetGetRequest(client, backend_bucket_ref)

    objects = client.MakeRequests([get_request])

    new_object, cleared_fields = self.Modify(args, objects[0])

    # If existing object is equal to the proposed object or if
    # Modify() returns None, then there is no work to be done, so we
    # print the resource and return.
    if objects[0] == new_object:
      log.status.Print(
          'No change requested; skipping update for [{0}].'.format(
              objects[0].name))
      return objects

    with client.apitools_client.IncludeFields(cleared_fields):
      return client.MakeRequests(
          [self.GetSetRequest(client, backend_bucket_ref, new_object)])

  def Run(self, args):
    """Issues the request necessary for updating a backend bucket."""
    if not self.AnyArgsSpecified(args)\
        and not args.IsSpecified('signed_url_cache_max_age')\
        and not self.AnyFlexibleCacheArgsSpecified(args):
      raise exceptions.ToolException('At least one property must be modified.')
    return self.MakeRequests(args)


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.ALPHA)
class UpdateAlphaBeta(Update):
  """Update a backend bucket.

  *{command}* is used to update backend buckets.
  """
  _support_flexible_cache_step_one = True
