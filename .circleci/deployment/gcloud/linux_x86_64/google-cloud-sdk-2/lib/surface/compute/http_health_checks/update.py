# -*- coding: utf-8 -*- #
# Copyright 2014 Google LLC. All Rights Reserved.
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
"""Command for updating HTTP health checks."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.compute.http_health_checks import flags
from googlecloudsdk.core import log

THRESHOLD_UPPER_BOUND = 10
THRESHOLD_LOWER_BOUND = 1
TIMEOUT_UPPER_BOUND_SEC = 300
TIMEOUT_LOWER_BOUND_SEC = 1
CHECK_INTERVAL_UPPER_BOUND_SEC = 300
CHECK_INTERVAL_LOWER_BOUND_SEC = 1


class Update(base.UpdateCommand):
  """Update a legacy HTTP health check.

  *{command}* is used to update an existing legacy HTTP health check. Only
  arguments passed in will be updated on the health check. Other
  attributes will remain unaffected.
  """

  HTTP_HEALTH_CHECKS_ARG = None

  @classmethod
  def Args(cls, parser):
    cls.HTTP_HEALTH_CHECKS_ARG = flags.HttpHealthCheckArgument()
    cls.HTTP_HEALTH_CHECKS_ARG.AddArgument(parser, operation_type='update')

    parser.add_argument(
        '--host',
        help="""\
        The value of the host header used in this HTTP health check request.
        By default, this is empty and Compute Engine automatically sets
        the host header in health requests to the same external IP address as
        the forwarding rule associated with the target pool. Setting this to
        an empty string will clear any existing host value.
        """)

    parser.add_argument(
        '--port',
        type=int,
        help="""\
        The TCP port number that this health check monitors.
        """)

    parser.add_argument(
        '--request-path',
        help="""\
        The request path that this health check monitors. For example,
        ``/healthcheck''.
        """)

    parser.add_argument(
        '--check-interval',
        type=arg_parsers.Duration(),
        help="""\
        How often to perform a health check for an instance. For example,
        specifying ``10s'' will run the check every 10 seconds.
        See $ gcloud topic datetimes for information on duration formats.
        """)

    parser.add_argument(
        '--timeout',
        type=arg_parsers.Duration(),
        help="""\
        If Compute Engine doesn't receive an HTTP 200 response from the
        instance by the time specified by the value of this flag, the health
        check request is considered a failure. For example, specifying ``10s''
        will cause the check to wait for 10 seconds before considering the
        request a failure.  Valid units for this flag are ``s'' for seconds and
        ``m'' for minutes.
        """)

    parser.add_argument(
        '--unhealthy-threshold',
        type=int,
        help="""\
        The number of consecutive health check failures before a healthy
        instance is marked as unhealthy.
        """)

    parser.add_argument(
        '--healthy-threshold',
        type=int,
        help="""\
        The number of consecutive successful health checks before an
        unhealthy instance is marked as healthy.
        """)

    parser.add_argument(
        '--description',
        help=('A textual description for the HTTP health check. Pass in an '
              'empty string to unset.'))

  @property
  def service(self):
    return self.compute.httpHealthChecks

  @property
  def resource_type(self):
    return 'httpHealthChecks'

  def CreateReference(self, resources, args):
    return self.HTTP_HEALTH_CHECKS_ARG.ResolveAsResource(
        args, resources)

  def GetGetRequest(self, client, http_health_check_ref):
    """Returns a request for fetching the existing HTTP health check."""
    return (client.apitools_client.httpHealthChecks,
            'Get',
            client.messages.ComputeHttpHealthChecksGetRequest(
                httpHealthCheck=http_health_check_ref.Name(),
                project=http_health_check_ref.project))

  def GetSetRequest(self, client, http_health_check_ref, replacement):
    """Returns a request for updated the HTTP health check."""
    return (client.apitools_client.httpHealthChecks,
            'Update',
            client.messages.ComputeHttpHealthChecksUpdateRequest(
                httpHealthCheck=http_health_check_ref.Name(),
                httpHealthCheckResource=replacement,
                project=http_health_check_ref.project))

  def Modify(self, client, args, existing_check):
    """Returns a modified HttpHealthCheck message."""
    # Description and Host are the only attributes that can be cleared by
    # passing in an empty string (but we don't want to set it to an empty
    # string).
    if args.description:
      description = args.description
    elif args.description is None:
      description = existing_check.description
    else:
      description = None

    if args.host:
      host = args.host
    elif args.host is None:
      host = existing_check.host
    else:
      host = None

    new_health_check = client.messages.HttpHealthCheck(
        name=existing_check.name,
        host=host,
        port=args.port or existing_check.port,
        description=description,
        requestPath=args.request_path or existing_check.requestPath,
        checkIntervalSec=(args.check_interval or
                          existing_check.checkIntervalSec),
        timeoutSec=args.timeout or existing_check.timeoutSec,
        healthyThreshold=(args.healthy_threshold or
                          existing_check.healthyThreshold),
        unhealthyThreshold=(args.unhealthy_threshold or
                            existing_check.unhealthyThreshold),
    )
    return new_health_check

  def Run(self, args):
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    client = holder.client

    if (args.check_interval is not None
        and (args.check_interval < CHECK_INTERVAL_LOWER_BOUND_SEC
             or args.check_interval > CHECK_INTERVAL_UPPER_BOUND_SEC)):
      raise exceptions.ToolException(
          '[--check-interval] must not be less than {0} second or greater '
          'than {1} seconds; received [{2}] seconds.'.format(
              CHECK_INTERVAL_LOWER_BOUND_SEC, CHECK_INTERVAL_UPPER_BOUND_SEC,
              args.check_interval))

    if (args.timeout is not None
        and (args.timeout < TIMEOUT_LOWER_BOUND_SEC
             or args.timeout > TIMEOUT_UPPER_BOUND_SEC)):
      raise exceptions.ToolException(
          '[--timeout] must not be less than {0} second or greater than {1} '
          'seconds; received: [{2}] seconds.'.format(
              TIMEOUT_LOWER_BOUND_SEC, TIMEOUT_UPPER_BOUND_SEC, args.timeout))

    if (args.healthy_threshold is not None
        and (args.healthy_threshold < THRESHOLD_LOWER_BOUND
             or args.healthy_threshold > THRESHOLD_UPPER_BOUND)):
      raise exceptions.ToolException(
          '[--healthy-threshold] must be an integer between {0} and {1}, '
          'inclusive; received: [{2}].'.format(THRESHOLD_LOWER_BOUND,
                                               THRESHOLD_UPPER_BOUND,
                                               args.healthy_threshold))

    if (args.unhealthy_threshold is not None
        and (args.unhealthy_threshold < THRESHOLD_LOWER_BOUND
             or args.unhealthy_threshold > THRESHOLD_UPPER_BOUND)):
      raise exceptions.ToolException(
          '[--unhealthy-threshold] must be an integer between {0} and {1}, '
          'inclusive; received [{2}].'.format(THRESHOLD_LOWER_BOUND,
                                              THRESHOLD_UPPER_BOUND,
                                              args.unhealthy_threshold))

    args_unset = not (args.port
                      or args.request_path
                      or args.check_interval
                      or args.timeout
                      or args.healthy_threshold
                      or args.unhealthy_threshold)
    if args.description is None and args.host is None and args_unset:
      raise exceptions.ToolException('At least one property must be modified.')

    http_health_check_ref = self.CreateReference(holder.resources, args)
    get_request = self.GetGetRequest(client, http_health_check_ref)

    objects = client.MakeRequests([get_request])

    new_object = self.Modify(client, args, objects[0])

    # If existing object is equal to the proposed object or if
    # Modify() returns None, then there is no work to be done, so we
    # print the resource and return.
    if objects[0] == new_object:
      log.status.Print(
          'No change requested; skipping update for [{0}].'.format(
              objects[0].name))
      return objects

    return client.MakeRequests(
        [self.GetSetRequest(client, http_health_check_ref, new_object)])
