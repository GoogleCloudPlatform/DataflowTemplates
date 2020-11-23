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
"""Common functions and classes for dealing with managed instances groups."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import json
import random
import re
import string
import sys
from apitools.base.py import encoding
from apitools.base.py import list_pager

from googlecloudsdk.api_lib.compute import exceptions
from googlecloudsdk.api_lib.compute import instance_utils
from googlecloudsdk.api_lib.compute import lister
from googlecloudsdk.api_lib.compute import path_simplifier
from googlecloudsdk.api_lib.compute import request_helper
from googlecloudsdk.api_lib.compute import utils
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions as calliope_exceptions
from googlecloudsdk.command_lib.compute.managed_instance_groups import auto_healing_utils
from googlecloudsdk.command_lib.util.apis import arg_utils
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.console import console_io
import six
from six.moves import range  # pylint: disable=redefined-builtin


_ALLOWED_UTILIZATION_TARGET_TYPES = [
    'DELTA_PER_MINUTE', 'DELTA_PER_SECOND', 'GAUGE']

_ALLOWED_UTILIZATION_TARGET_TYPES_LOWER = [
    'delta-per-minute', 'delta-per-second', 'gauge']

ARGS_CONFLICTING_WITH_AUTOSCALING_FILE_BETA = [
    'max_num_replicas', 'cool_down_period', 'custom_metric_utilization',
    'description', 'min_num_replicas',
    'scale_based_on_cpu', 'scale_based_on_load_balancing',
    'target_cpu_utilization', 'target_load_balancing_utilization'
]

ARGS_CONFLICTING_WITH_AUTOSCALING_FILE_ALPHA = (
    ARGS_CONFLICTING_WITH_AUTOSCALING_FILE_BETA)


_MAX_AUTOSCALER_NAME_LENGTH = 63
# 4 character chosen from between lowercase letters and numbers give >1.6M
# possibilities with no more than 100 Autoscalers in one Zone and Project
# so probability that adding an autoscaler will fail because of name conflict
# is about 6e-5.
_NUM_RANDOM_CHARACTERS_IN_AS_NAME = 4


# TODO(b/110191362): resign from passing whole args to functions in this file


class Error(exceptions.Error):
  """Base exception for managed instance group exceptions."""


class ResourceNotFoundException(Error):
  """The user tries to get/use/update resource which does not exist."""


class InvalidArgumentError(exceptions.Error):
  """The user provides invalid arguments."""


class ResourceAlreadyExistsException(Error):
  """The user tries to create resource which already exists."""


class ResourceCannotBeResolvedException(Error):
  """The user uses invalid / partial name to resolve URI for the resource."""


def AddAutoscalerArgs(
    parser,
    autoscaling_file_enabled=False,
    stackdriver_metrics_flags=False,
    predictive=False,
    scheduled=False,
    patch_args=False):
  """Adds commandline arguments to parser."""
  parser.add_argument(
      '--cool-down-period',
      type=arg_parsers.Duration(),
      help=('The time period that the autoscaler should wait before it starts '
            'collecting information from a new instance. This prevents the '
            'autoscaler from collecting information when the instance is '
            'initializing, during which the collected usage would not be '
            'reliable. The default is 60s. See $ gcloud topic datetimes '
            'for information on duration formats.'))
  parser.add_argument('--description', help='Notes about Autoscaler.')
  AddMinMaxControl(parser, max_required=not autoscaling_file_enabled)
  parser.add_argument('--scale-based-on-cpu',
                      action='store_true',
                      help='Autoscaler will be based on CPU utilization.')
  parser.add_argument('--scale-based-on-load-balancing',
                      action='store_true',
                      help=('Use autoscaling based on load balancing '
                            'utilization.'))
  parser.add_argument('--target-cpu-utilization',
                      type=arg_parsers.BoundedFloat(0.0, 1.0),
                      help='Autoscaler will aim to maintain CPU utilization at '
                      'target level (0.0 to 1.0).')
  parser.add_argument('--target-load-balancing-utilization',
                      type=arg_parsers.BoundedFloat(0.0, None),
                      help='Autoscaler will aim to maintain the load balancing '
                      'utilization level (greater than 0.0).')
  custom_metric_utilization_help = """\
Adds a target metric value for the Autoscaler to use.

*metric*::: Protocol-free URL of a Google Cloud Monitoring metric.

*utilization-target*::: Value of the metric Autoscaler will aim to
  maintain (greater than 0.0).

*utilization-target-type*::: How target is expressed. Valid values: {0}.
""".format(', '.join(_ALLOWED_UTILIZATION_TARGET_TYPES))
  if stackdriver_metrics_flags:
    custom_metric_utilization_help += """
Mutually exclusive with `--update-stackdriver-metric`.
"""
  parser.add_argument(
      '--custom-metric-utilization',
      type=arg_parsers.ArgDict(
          spec={
              'metric': str,
              'utilization-target': float,
              'utilization-target-type': str,
          },
      ),
      action='append',
      help=custom_metric_utilization_help,
  )

  if autoscaling_file_enabled:
    parser.add_argument(
        '--autoscaling-file',
        metavar='PATH',
        help=('Path of the file from which autoscaling configuration will be '
              'loaded. This flag allows you to atomically setup complex '
              'autoscalers.'))
  if stackdriver_metrics_flags:
    parser.add_argument(
        '--remove-stackdriver-metric',
        metavar='METRIC',
        help=('Stackdriver metric to remove from autoscaling configuration. '
              'If the metric is the only input used for autoscaling the '
              'command will fail.'))
    parser.add_argument(
        '--update-stackdriver-metric',
        metavar='METRIC',
        help=('Stackdriver metric to use as an input for autoscaling. '
              'When using this flag you must also specify target value of the '
              'metric by specifying '
              '`--stackdriver-metric-single-instance-assignment` or '
              '`--stackdriver-metric-utilization-target` and '
              '`--stackdriver-metric-utilization-target-type`. '
              'Mutually exclusive with `--custom-metric-utilization`.'))
    parser.add_argument(
        '--stackdriver-metric-filter',
        metavar='FILTER',
        help=('Expression for filtering samples used to autoscale, see '
              'https://cloud.google.com/monitoring/api/v3/filters.'))
    parser.add_argument(
        '--stackdriver-metric-utilization-target',
        metavar='TARGET',
        type=float,
        help=('Value of the metric Autoscaler will aim to maintain. When '
              'specifying this flag you must also provide '
              '`--stackdriver-metric-utilization-target-type`. Mutually '
              'exclusive with '
              '`--stackdriver-metric-single-instance-assignment` and '
              '`--custom-metric-utilization`.'))

    parser.add_argument(
        '--stackdriver-metric-utilization-target-type',
        metavar='TARGET_TYPE',
        choices=_ALLOWED_UTILIZATION_TARGET_TYPES_LOWER,
        help=('Value of the metric Autoscaler will aim to maintain. When '
              'specifying this flag you must also provide '
              '`--stackdriver-metric-utilization-target`. Mutually '
              'exclusive with '
              '`--stackdriver-metric-single-instance-assignment` and '
              '`--custom-metric-utilization`.'))
    parser.add_argument(
        '--stackdriver-metric-single-instance-assignment',
        metavar='ASSIGNMENT',
        type=float,
        help=('Autoscaler will aim to maintain value of metric divided by '
              'number of instances at this level. Mutually '
              'exclusive with '
              '`-stackdriver-metric-utilization-target-type`, '
              '`-stackdriver-metric-utilization-target-type`, and '
              '`--custom-metric-utilization`.'))

  GetModeFlag().AddToParser(parser)

  AddScaleInControlFlag(parser)

  if predictive:
    AddPredictiveAutoscaling(parser)

  if scheduled:
    AddScheduledAutoscaling(parser, patch_args)


def AddMinMaxControl(parser, max_required=True):
  """Adds min and max num replicas controls to a given parser."""
  parser.add_argument('--min-num-replicas',
                      type=arg_parsers.BoundedInt(0, sys.maxsize),
                      help='Minimum number of replicas Autoscaler will set.')
  parser.add_argument('--max-num-replicas',
                      type=arg_parsers.BoundedInt(0, sys.maxsize),
                      required=max_required,
                      help='Maximum number of replicas Autoscaler will set.')


def GetModeFlag():
  # Can't use a ChoiceEnumMapper because we don't have access to the "messages"
  # module for the right API version.
  return base.ChoiceArgument(
      '--mode',
      {
          'on': ('Permits autoscaling to scale out and in (default for '
                 'new autoscalers).'),
          'only-scale-out': ('Permits autoscaling to scale only out and '
                             'not in.'),
          'only-up': ("""
              (DEPRECATED) Permits autoscaling to scale only out and not in.

              Value `only-up` is deprecated. Use `--mode only-scale-out`
              instead.
          """),
          'off': ('Turns off autoscaling, while keeping the new '
                  'configuration.')
      },
      help_str="""\
          Set the mode of an autoscaler for a managed instance group.

          You can turn off or restrict MIG autoscaler activities without
          affecting your autoscaler configuration. The autoscaler configuration
          persists while the activities are turned off or restricted, and the
          activities resume when the autoscaler is turned on again or when the
          restrictions are lifted.
      """)


def AddScaleInControlFlag(parser, include_clear=False):
  """Adds scale-in-control flags to the given parser."""
  arg_group = parser
  # Convert to arg group if --clear-scale-in-control flag is included.
  if include_clear:
    arg_group = parser.add_group(mutex=True)
    arg_group.add_argument(
        '--clear-scale-in-control',
        action='store_true',
        help="""\
          If specified, the scale-in-control field will be cleared. Using this
          flag will remove any configuration set by `--scale-in-control` flag.
        """)
  arg_group.add_argument(
      '--scale-in-control',
      type=arg_parsers.ArgDict(
          spec={
              'max-scaled-in-replicas': str,
              'max-scaled-in-replicas-percent': str,
              'time-window': int,
          },),
      help="""\
        Configuration that allows slower scale in so that even if Autoscaler
        recommends an abrupt scale in of a managed instance group, it will be
        throttled as specified by the parameters.

        *max-scaled-in-replicas*::: Maximum allowed number of VMs that can be
        deducted from the peak recommendation during the window. Possibly all
        these VMs can be deleted at once so the application needs to be prepared
        to lose that many VMs in one step. Mutually exclusive with
        'max-scaled-in-replicas-percent'.

        *max-scaled-in-replicas-percent*::: Maximum allowed percent of VMs
        that can be deducted from the peak recommendation during the window.
        Possibly all these VMs can be deleted at once so the application needs
        to be prepared to lose that many VMs in one step. Mutually exclusive
        with  'max-scaled-in-replicas'.

        *time-window*::: How long back autoscaling should look when computing
        recommendations. The autoscaler will not resize below the maximum
        allowed deduction subtracted from the peak size observed in this
        period. Measured in seconds.
        """)


def AddPredictiveAutoscaling(parser):
  parser.add_argument(
      '--cpu-utilization-predictive-method',
      choices={
          'none':
              ('(Default) No predictions are made based on the scaling metric '
               'when calculating the number of VM instances.'),
          'standard':
              ('Standard predictive autoscaling  predicts the future values of '
               'the scaling metric and then scales a MIG to ensure that new VM '
               'instances are ready in time to cover the predicted peak.'),
          'optimize-availability':
              ('Predictive autoscaling predicts the future values of the '
               'scaling metric and scales a MIG in advance to ensure that new '
               'VM instances are ready in time to cover the predicted peak.')
      },
      help='Indicates which method of prediction is used for CPU utilization metric, if any.'
  )


def AddScheduledAutoscaling(parser, patch_args):
  """Add parameters controlling scheduled autoscaling."""
  if patch_args:
    arg_group = parser.add_group(mutex=True)
    arg_group_config = parser.add_group()
    arg_group.add_argument(
        '--set-schedule',
        help='A unique name of the scaling schedule to be configured.')
    arg_group.add_argument(
        '--update-schedule',
        help='A unique name of the scaling schedule to be updated.')
    arg_group.add_argument(
        '--remove-schedule',
        help="""\
          A unique name of the scaling schedule to be removed.

          Be careful with this action as scaling schedule deletion cannot be
          undone.

          You can delete any schedule regardless of its status. If you delete
          a scaling schedule that is currently active, the deleted scaling
          schedule will stop being effective immediately after it is deleted.
          If this results in removing instances from MIG, the autoscaler will
          respect the 10-minute stabilization period and if configured also
          scale-in controls. This ensures you don't accidentally lose capacity
          immediately after the scaling schedule ends.
          """)
    arg_group.add_argument(
        '--enable-schedule',
        help='A unique name of the scaling schedule to be enabled.')
    arg_group.add_argument(
        '--disable-schedule',
        help="""\
          A unique name of the scaling schedule to be disabled.

          When the scaling schedule is disabled its configuration persists but
          the scaling schedule itself never becomes active. If you disable a
          scaling schedule that is currently active the deleted scaling
          schedule will stop being effective immediately after it moves into
          DISABLED state. If this results in removing instances from
          MIG, the autoscaler will respect the 10-minute stabilization period
          and if configured also scale-in controls. This ensures you don't
          accidentally lose capacity immediately after the scaling schedule
          ends.
          """)
    AddScheduledAutoscalingConfigurationArguments(arg_group_config)
  else:
    arg_group = parser
    parser.add_argument(
        '--set-schedule',
        help='A unique name of the scaling schedule.')
    AddScheduledAutoscalingConfigurationArguments(parser.add_group())


def AddScheduledAutoscalingConfigurationArguments(arg_group):
  """Add arguments that are common to adding or modifying a scaling schedule."""
  arg_group.add_argument(
      '--schedule-cron',
      help="""\
        Start time of the scaling schedule in cron format.

        This is when an autoscaler will start creating new VMs if MIG size is
        lower than the minimum number of instances. Set the start time to allow
        enough time for new VMs to boot and initialize. For example if your
        workload takes 10 minutes from VM creation to start serving then set
        start time 10 minutes earlier than the time you need VMs to be ready.
        """)
  arg_group.add_argument(
      '--schedule-duration-sec',
      type=arg_parsers.BoundedInt(300, sys.maxsize),
      help="""\
        How long should the scaling schedule be active, measured in seconds.

        Minimum duration is 5 minutes. Scaling schedule is active from its start
        time and for configured duration. During this time autoscaler will scale
        MIG to have at least as many VMs as defined by the minimum number of
        instances. After specified duration if there is no need to maintain
        capacity, the autoscaler will start removing instances after a 10
        minutes stabilization period and (if configured) following scale-in
        controls.
        """)
  arg_group.add_argument(
      '--schedule-min-required-replicas',
      type=arg_parsers.BoundedInt(0, sys.maxsize),
      help="""\
        How many VMs autoscaler should ensure for the duration of this scaling
        schedule.

        Autoscaler will provide at least this number of instances when the
        scaling schedule is active. MIG can have more VMs if there are other
        scaling schedules active with higher instance count or if autoscaling
        policy (for example on CPU) requires more instances to meet its target.

        This configuration does not change autoscaling minimum and maximum
        instance limits which are always in effect. Autoscaler will not create
        more than the maximum number of instances configured for MIG.
        """)
  arg_group.add_argument(
      '--schedule-time-zone',
      help="""\
        Location of the scaling schedule's start time.

        It should be provided provided as a location from the IANA tz database
        (for example Europe/Paris). It will consider daylight saving time (DST)
        adjustments. If no time zone is provided UTC is used as a default.

        See https://en.wikipedia.org/wiki/List_of_tz_database_time_zones for
        the list of valid timezones.
        """)
  arg_group.add_argument(
      '--schedule-description',
      help='A verbose description of the scaling schedule.')


def ValidateConflictsWithAutoscalingFile(args, conflicting_args):
  if (hasattr(args, 'autoscaling_file') and
      args.IsSpecified('autoscaling_file')):
    for arg in conflicting_args:
      if args.IsSpecified(arg):
        conflicting_flags = [
            '--' + a.replace('_', '-')
            for a in conflicting_args
        ]
        raise calliope_exceptions.ConflictingArgumentsException(
            *(['--autoscaling-file'] + conflicting_flags))


def _ValidateCustomMetricUtilizationVsUpdateStackdriverMetric(args):
  if (args.IsSpecified('custom_metric_utilization') and
      args.IsSpecified('update_stackdriver_metric')):
    raise calliope_exceptions.ConflictingArgumentsException(
        '--custom-metric-utilization', '--update-stackdriver-metric')


def _ValidateRemoveStackdriverMetricVsUpdateStackdriverMetric(args):
  if (args.IsSpecified('update_stackdriver_metric') and
      args.IsSpecified('remove_stackdriver_metric') and
      args.update_stackdriver_metric == args.remove_stackdriver_metric):
    raise calliope_exceptions.InvalidArgumentException(
        '--update-stackdriver-metric',
        'You can not remove Stackdriver metric you are updating with '
        '[--update-stackdriver-metric] flag.')


def _ValidateRequiringUpdateStackdriverMetric(args):  # pylint:disable=missing-docstring
  if not args.IsSpecified('update_stackdriver_metric'):
    requiring_flags = [
        'stackdriver_metric_filter',
        'stackdriver_metric_single_instance_assignment',
        'stackdriver_metric_utilization_target',
        'stackdriver_metric_utilization_target_type',
    ]
    for f in requiring_flags:
      if args.IsSpecified(f):
        raise calliope_exceptions.RequiredArgumentException(
            '--' + f.replace('_', '-'),
            '[--update-stackdriver-metric] required to use this flag.')


def _ValidateRequiredByUpdateStackdriverMetric(args):  # pylint:disable=missing-docstring
  if args.IsSpecified('update_stackdriver_metric'):
    one_of_required = [
        'stackdriver_metric_single_instance_assignment',
        'stackdriver_metric_utilization_target',]
    if not any([args.IsSpecified(f) for f in one_of_required]):
      flags = [
          '[--{}]'.format(f.replace('_', '--')) for f in one_of_required]
      msg = ('You must provide one of {} with '
             '[--update-stackdriver-metric].'.format(', '.join(flags)))
      raise calliope_exceptions.RequiredArgumentException(
          '--update-stackdriver-metric', msg)


def _ValidateSingleInstanceAssignmentVsUtilizationTarget(args):  # pylint:disable=missing-docstring
  if args.IsSpecified('stackdriver_metric_single_instance_assignment'):
    potential_conflicting = [
        'stackdriver_metric_utilization_target',
        'stackdriver_metric_utilization_target_type',
    ]
    conflicting = [f for f in potential_conflicting if args.IsSpecified(f)]
    if any(conflicting):
      assignment_flag = '--stackdriver-metric-single-instance-assignment'
      conflicting_flags = [
          '[--{}]'.format(f.replace('_', '-')) for f in conflicting]
      raise calliope_exceptions.ConflictingArgumentsException(
          assignment_flag,
          'You cannot use any of {} with `{}`'.format(
              conflicting_flags, assignment_flag))


def _ValidateUtilizationTargetHasType(args):
  if (args.IsSpecified('stackdriver_metric_utilization_target') and
      not args.IsSpecified('stackdriver_metric_utilization_target_type')):
    raise calliope_exceptions.RequiredArgumentException(
        '--stackdriver-metric-utilization-target-type',
        'Required with [--stackdriver-metric-utilization-target].')


def ValidateStackdriverMetricsFlags(args):
  """Perform validations related to .*stackdriver-metric.* flags."""
  _ValidateCustomMetricUtilizationVsUpdateStackdriverMetric(args)
  _ValidateRemoveStackdriverMetricVsUpdateStackdriverMetric(args)
  _ValidateRequiringUpdateStackdriverMetric(args)
  _ValidateRequiredByUpdateStackdriverMetric(args)
  _ValidateSingleInstanceAssignmentVsUtilizationTarget(args)
  _ValidateUtilizationTargetHasType(args)


def ValidateGeneratedAutoscalerIsValid(args, autoscaler):
  if (args.IsSpecified('remove_stackdriver_metric') and
      not autoscaler.autoscalingPolicy.customMetricUtilizations and
      not autoscaler.autoscalingPolicy.cpuUtilization and
      not autoscaler.autoscalingPolicy.loadBalancingUtilization):
    raise calliope_exceptions.InvalidArgumentException(
        '--remove-stackdriver-metric',
        'This would remove the only signal used for autoscaling. If you want '
        'to stop autoscaling the Managed Instance Group use `stop-autoscaling` '
        'command instead.')


def ValidateAutoscalerArgs(args):
  """Validates args."""
  if args.min_num_replicas and args.max_num_replicas:
    if args.min_num_replicas > args.max_num_replicas:
      raise calliope_exceptions.InvalidArgumentException(
          '--max-num-replicas', 'can\'t be less than min num replicas.')

  if args.custom_metric_utilization:
    for custom_metric_utilization in args.custom_metric_utilization:
      for field in ('utilization-target', 'metric', 'utilization-target-type'):
        if field not in custom_metric_utilization:
          raise calliope_exceptions.InvalidArgumentException(
              '--custom-metric-utilization', field + ' not present.')
      if custom_metric_utilization['utilization-target'] < 0:
        raise calliope_exceptions.InvalidArgumentException(
            '--custom-metric-utilization utilization-target', 'less than 0.')


def GetInstanceGroupManagerOrThrow(igm_ref, client):
  """Retrieves the given Instance Group Manager if possible.

  Args:
    igm_ref: reference to the Instance Group Manager.
    client: The compute client.
  Returns:
    Instance Group Manager object.
  """
  if hasattr(igm_ref, 'region'):
    service = client.apitools_client.regionInstanceGroupManagers
    request_type = service.GetRequestType('Get')
  if hasattr(igm_ref, 'zone'):
    service = client.apitools_client.instanceGroupManagers
    request_type = service.GetRequestType('Get')
  request = request_type(**igm_ref.AsDict())

  errors = []
  # Run through the generator to actually make the requests and get potential
  # errors.
  igm_details = client.MakeRequests([(service, 'Get', request)],
                                    errors_to_collect=errors)

  if errors or len(igm_details) != 1:
    utils.RaiseException(errors, ResourceNotFoundException,
                         error_message='Could not fetch resource:')
  return igm_details[0]


def CreateZoneRef(resources, data):
  """Create zone reference from object with project and zone fields."""
  return resources.Parse(
      None,
      params={'project': data.project,
              'zone': data.zone},
      collection='compute.zones')


def CreateRegionRef(resources, data):
  """Create region reference from object with project and region fields."""
  return resources.Parse(
      None,
      params={'project': data.project,
              'region': data.region},
      collection='compute.regions')


def GroupByProject(locations):
  """Group locations by project field."""
  result = {}
  for location in locations or []:
    if location.project not in result:
      result[location.project] = []
    result[location.project].append(location)
  return result


def AutoscalersForLocations(zones, regions, client,
                            fail_when_api_not_supported=True):
  """Finds all Autoscalers defined for a given project and locations.

  Args:
    zones: iterable of target zone references
    regions: iterable of target region references
    client: The compute client.
    fail_when_api_not_supported: If true, raise tool exception if API does not
        support autoscaling.
  Returns:
    A list of Autoscaler objects.
  """
  # Errors is passed through library calls and modified with
  # (ERROR_CODE, ERROR_MESSAGE) tuples.
  errors = []

  # Explicit list() is required to unwind the generator and make sure errors
  # are detected at this level.
  requests = []
  for project, zones in six.iteritems(GroupByProject(zones)):
    requests += lister.FormatListRequests(
        service=client.apitools_client.autoscalers,
        project=project,
        scopes=sorted(set([zone_ref.zone for zone_ref in zones])),
        scope_name='zone',
        filter_expr=None)

  if regions:
    if hasattr(client.apitools_client, 'regionAutoscalers'):
      for project, regions in six.iteritems(GroupByProject(regions)):
        requests += lister.FormatListRequests(
            service=client.apitools_client.regionAutoscalers,
            project=project,
            scopes=sorted(set([region_ref.region for region_ref in regions])),
            scope_name='region',
            filter_expr=None)
    else:
      if fail_when_api_not_supported:
        errors.append((None, 'API does not support regional autoscaling'))

  autoscalers = client.MakeRequests(
      requests=requests,
      errors_to_collect=errors)

  if errors:
    utils.RaiseToolException(
        errors,
        error_message='Could not check if the Managed Instance Group is '
        'Autoscaled.')

  return autoscalers


def AutoscalersForMigs(migs, autoscalers):
  """Finds Autoscalers with target amongst given IGMs.

  Args:
    migs: List of triples (IGM name, scope type, location reference).
    autoscalers: A list of Autoscalers to search among.
  Returns:
    A list of all Autoscalers with target on mig_names list.
  """
  igm_url_regexes = []
  for (name, scope_type, location) in migs:
    igm_url_regexes.append(
        '/projects/{project}/{scopeType}/{scopeName}/'
        'instanceGroupManagers/{name}$'
        .format(project=location.project,
                scopeType=(scope_type + 's'),
                scopeName=getattr(location, scope_type),
                name=name))
  igm_url_regex = re.compile('(' + ')|('.join(igm_url_regexes) + ')')
  result = [
      autoscaler for autoscaler in autoscalers
      if igm_url_regex.search(autoscaler.target)
  ]
  return result


def AutoscalerForMigByRef(client, resources, igm_ref):
  """Returns autoscaler targeting given instance group manager.

  Args:
    client: a GCE client
    resources: a GCE resource registry
    igm_ref: reference to instance group manager
  Returns:
    Autoscaler message with autoscaler targeting the IGM refferenced by
    igm_ref or None if there isn't one.
  Raises:
    ValueError: if instance group manager collection path is unknown
  """
  if igm_ref.Collection() == 'compute.instanceGroupManagers':
    scope_type = 'zone'
    location = CreateZoneRef(resources, igm_ref)
    zones, regions = [location], None
  elif igm_ref.Collection() == 'compute.regionInstanceGroupManagers':
    scope_type = 'region'
    location = CreateRegionRef(resources, igm_ref)
    zones, regions = None, [location]
  else:
    raise ValueError('Unknown reference type {0}'.format(igm_ref.Collection()))

  autoscalers = AutoscalersForLocations(
      regions=regions,
      zones=zones,
      client=client)

  return AutoscalerForMig(
      mig_name=igm_ref.Name(),
      autoscalers=autoscalers,
      location=location,
      scope_type=scope_type)


def AutoscalerForMig(mig_name, autoscalers, location, scope_type):
  """Finds Autoscaler targeting given IGM.

  Args:
    mig_name: Name of MIG targeted by Autoscaler.
    autoscalers: A list of Autoscalers to search among.
    location: Target location reference.
    scope_type: Target scope type.
  Returns:
    Autoscaler object for autoscaling the given Instance Group Manager or None
    when such Autoscaler does not exist.
  """
  autoscalers = AutoscalersForMigs(
      [(mig_name, scope_type, location)], autoscalers)
  if autoscalers:
    # For each Instance Group Manager there can be at most one Autoscaler having
    # the Manager as a target, so when one is found it can be returned as it is
    # the only one.
    if len(autoscalers) == 1:
      return autoscalers[0]
    else:
      raise calliope_exceptions.ToolException(
          'More than one Autoscaler with given target.')
  return None


def AddAutoscalersToMigs(migs_iterator,
                         client,
                         resources,
                         fail_when_api_not_supported=True):
  """Add Autoscaler to each IGM object if autoscaling is enabled for it."""
  def ParseZone(zone_link):
    return resources.Parse(
        zone_link,
        params={'project': properties.VALUES.core.project.GetOrFail},
        collection='compute.zones')

  def ParseRegion(region_link):
    return resources.Parse(
        region_link,
        params={'project': properties.VALUES.core.project.GetOrFail},
        collection='compute.regions')

  migs = list(migs_iterator)
  zones = set([ParseZone(mig['zone']) for mig in migs if 'zone' in mig])
  regions = set(
      [ParseRegion(mig['region']) for mig in migs if 'region' in mig])
  autoscalers = {}
  all_autoscalers = AutoscalersForLocations(
      zones=zones,
      regions=regions,
      client=client,
      fail_when_api_not_supported=fail_when_api_not_supported)

  for location in list(zones) + list(regions):
    autoscalers[location.Name()] = []

  for autoscaler in all_autoscalers:
    autoscaler_scope = None
    if autoscaler.zone is not None:
      autoscaler_scope = ParseZone(autoscaler.zone)
    if hasattr(autoscaler, 'region') and autoscaler.region is not None:
      autoscaler_scope = ParseRegion(autoscaler.region)
    if autoscaler_scope is not None:
      autoscalers.setdefault(autoscaler_scope.Name(), [])
      autoscalers[autoscaler_scope.Name()].append(autoscaler)

  for mig in migs:
    location = None
    scope_type = None
    if 'region' in mig:
      location = ParseRegion(mig['region'])
      scope_type = 'region'
    elif 'zone' in mig:
      location = ParseZone(mig['zone'])
      scope_type = 'zone'

    autoscaler = None
    if location and scope_type:
      autoscaler = AutoscalerForMig(
          mig_name=mig['name'],
          autoscalers=autoscalers[location.Name()],
          location=location,
          scope_type=scope_type)
    if autoscaler:
      mig['autoscaler'] = autoscaler
    yield mig


def _BuildCpuUtilization(args, messages, predictive=False):
  """Builds the CPU Utilization message given relevant arguments."""
  flags_to_check = ['target_cpu_utilization', 'scale_based_on_cpu']
  if predictive:
    flags_to_check.append('cpu_utilization_predictive_method')

  if instance_utils.IsAnySpecified(args, *flags_to_check):
    cpu_message = messages.AutoscalingPolicyCpuUtilization()
    if args.target_cpu_utilization:
      cpu_message.utilizationTarget = args.target_cpu_utilization
    if predictive and args.cpu_utilization_predictive_method:
      cpu_predictive_enum = messages.AutoscalingPolicyCpuUtilization.PredictiveMethodValueValuesEnum
      cpu_message.predictiveMethod = arg_utils.ChoiceToEnum(
          args.cpu_utilization_predictive_method, cpu_predictive_enum)
    return cpu_message
  return None


def _BuildCustomMetricUtilizationsFromCustomMetricUtilizationFlag(
    flag, messages):
  """Translate --custom-metric-utilization flag to API message."""
  result = []
  for custom_metric_utilization in flag:
    result.append(
        messages.AutoscalingPolicyCustomMetricUtilization(
            utilizationTarget=custom_metric_utilization[
                'utilization-target'],
            metric=custom_metric_utilization['metric'],
            utilizationTargetType=(
                messages
                .AutoscalingPolicyCustomMetricUtilization
                .UtilizationTargetTypeValueValuesEnum(
                    custom_metric_utilization['utilization-target-type'],
                )
            ),
        )
    )
  return result


def _RemoveMetricFromList(metrics, to_remove):
  for i, metric in enumerate(metrics):
    if metric.metric == to_remove:
      del metrics[i]
      return


def _UpdateCustomMetricUtilizationsFromStackoverflowFlags(
    args, messages, original):
  """Take apply stackdriver flags to customMetricUtilizations."""
  if original:
    result = original.autoscalingPolicy.customMetricUtilizations
  else:
    result = []
  if args.remove_stackdriver_metric:
    _RemoveMetricFromList(result, args.remove_stackdriver_metric)
  if args.update_stackdriver_metric:
    _RemoveMetricFromList(result, args.update_stackdriver_metric)
    if args.stackdriver_metric_utilization_target_type:
      target_type = (
          messages.AutoscalingPolicyCustomMetricUtilization
          .UtilizationTargetTypeValueValuesEnum(
              args.stackdriver_metric_utilization_target_type.upper().
              replace('-', '_'),
          )
      )
    else:
      target_type = None

    if args.stackdriver_metric_filter and "'" in args.stackdriver_metric_filter:
      log.warning(
          "The provided filter contains a single quote character ('). While "
          "valid as a metric/resource label value, it's not a control "
          "character that is part of the filtering language; if you meant "
          "to use it to quote a string value, you need to use a double quote "
          "character (\") instead.")
    result.append(
        messages.AutoscalingPolicyCustomMetricUtilization(
            utilizationTarget=args.stackdriver_metric_utilization_target,
            metric=args.update_stackdriver_metric,
            utilizationTargetType=target_type,
            singleInstanceAssignment=(
                args.stackdriver_metric_single_instance_assignment
            ),
            filter=args.stackdriver_metric_filter,
        )
    )
  return result


def _BuildCustomMetricUtilizations(args, messages, original):
  """Builds custom metric utilization policy list from args.

  Args:
    args: command line arguments.
    messages: module containing message classes.
    original: original autoscaler message.
  Returns:
    AutoscalingPolicyCustomMetricUtilization list.
  """
  if args.custom_metric_utilization:
    return _BuildCustomMetricUtilizationsFromCustomMetricUtilizationFlag(
        args.custom_metric_utilization, messages)
  if hasattr(args, 'stackdriver_metric_filter'):
    return _UpdateCustomMetricUtilizationsFromStackoverflowFlags(
        args, messages, original)


def _BuildLoadBalancingUtilization(args, messages):
  if args.target_load_balancing_utilization:
    return messages.AutoscalingPolicyLoadBalancingUtilization(
        utilizationTarget=args.target_load_balancing_utilization,
    )
  if args.scale_based_on_load_balancing:
    return messages.AutoscalingPolicyLoadBalancingUtilization()
  return None


def ParseModeString(mode, messages):
  return messages.AutoscalingPolicy.ModeValueValuesEnum(
      mode.upper().replace('-', '_'))


def _BuildMode(args, messages, original):
  if not args.mode:
    return original.autoscalingPolicy.mode if original else None
  return ParseModeString(args.mode, messages)


def BuildScaleIn(args, messages):
  """Builds AutoscalingPolicyScaleInControl.

  Args:
    args: command line arguments.
    messages: module containing message classes.
  Returns:
    AutoscalingPolicyScaleInControl message object.
  Raises:
    InvalidArgumentError:  if both max-scaled-in-replicas and
      max-scaled-in-replicas-percent are specified.
  """
  if args.IsSpecified('scale_in_control'):
    replicas_arg = args.scale_in_control.get('max-scaled-in-replicas')
    replicas_arg_percent = args.scale_in_control.get(
        'max-scaled-in-replicas-percent')
    if replicas_arg and replicas_arg_percent:
      raise InvalidArgumentError(
          'max-scaled-in-replicas and max-scaled-in-replicas-percent'
          'are mutually exclusive, you can\'t specify both')
    elif replicas_arg_percent:
      max_replicas = messages.FixedOrPercent(percent=int(replicas_arg_percent))
    else:
      max_replicas = messages.FixedOrPercent(fixed=int(replicas_arg))

    return messages.AutoscalingPolicyScaleInControl(
        maxScaledInReplicas=max_replicas,
        timeWindowSec=args.scale_in_control.get('time-window'))


def _RemoveScheduleEncoder(message, unused_encoder=None):
  """Encoder for use when removing a schedule.

  It works around issues with proto encoding of AdditionalProperties with null
  values by directly encoding a dict of keys with None values into json,
  skipping proto-based encoding.

  Args:
    message: an instance of AutoscalingPolicy.ScalingSchedulesValue.
  Returns:
    Schedule removal request JSON dumped to string.
  """
  py_object = {property.key: None for property in message.additionalProperties}
  return json.dumps(py_object)


def _RemoveScheduleDecoder(unused_data, unused_decoder=None):
  """Dummy decoder for schedule removal message.

  It's passed when registering message codec, but it will never be used as
  removing schedules is a write-only operation.
  """
  raise NotImplementedError("This should never be called, it's a write-only"
                            " operation.")


def BuildScheduled(args, messages):
  """Builds AutoscalingPolicyScalingSchedules.

  Args:
    args: command line arguments.
    messages: module containing message classes.
  Returns:
    Dict containing an AutoscalingPolicyScalingSchedule message object.
  Raises:
    InvalidArgumentError:  if more than one of --scaling-schedule,
    --update-schedule, --remove-schedule,
    --enable-schedule, --disable-schedule is specified.
  """
  # The following method uses getattr insead of args.IsSpecified because the
  # latter doesn't work before the parameter is available in GA, raising
  # googlecloudsdk.calliope.parser_errors.UnknownDestinationException.
  mutex_group = {'set_schedule', 'update_schedule', 'remove_schedule',
                 'enable_schedule', 'disable_schedule'}
  count = 0
  for possitble_argument in mutex_group:
    if getattr(args, possitble_argument, None) is not None:
      count += 1
  if count == 0:
    return None
  if count > 1:
    raise InvalidArgumentError(
        '--set-schedule, --update-schedule, --remove-schedule, '
        '--enable-schedule, --disable-schedule are mutually exclusive, only '
        'one can be specified.')

  scaling_schedule_wrapper = (messages.AutoscalingPolicy.
                              ScalingSchedulesValue.AdditionalProperty)
  field_mapping = {
      'schedule_cron': 'schedule',
      'schedule_duration_sec': 'durationSec',
      'schedule_min_required_replicas': 'minRequiredReplicas',
      'schedule_time_zone': 'timeZone',
      'schedule_description': 'description',
  }

  if getattr(args, 'enable_schedule', None) is not None:
    return messages.AutoscalingPolicy.ScalingSchedulesValue(
        additionalProperties=[
            scaling_schedule_wrapper(
                key=args.enable_schedule,
                value=messages.AutoscalingPolicyScalingSchedule(
                    disabled=False))])
  if getattr(args, 'disable_schedule', None) is not None:
    return messages.AutoscalingPolicy.ScalingSchedulesValue(
        additionalProperties=[
            scaling_schedule_wrapper(
                key=args.disable_schedule,
                value=messages.AutoscalingPolicyScalingSchedule(
                    disabled=True))])
  if getattr(args, 'remove_schedule', None) is not None:
    # Register a custom message codec to properly encode AdditionalProperty's
    # None value into nulls.
    encoding.RegisterCustomMessageCodec(
        encoder=_RemoveScheduleEncoder,
        decoder=_RemoveScheduleDecoder)(
            messages.AutoscalingPolicy.ScalingSchedulesValue)
    return messages.AutoscalingPolicy.ScalingSchedulesValue(
        additionalProperties=[
            scaling_schedule_wrapper(
                key=args.remove_schedule,
                value=None)])
  if getattr(args, 'set_schedule', None) is not None:
    policy_name = args.set_schedule
    required = {'schedule_cron', 'schedule_duration_sec',
                'schedule_min_required_replicas'}
    # Set-shedule should clear pre-existing fields, so we set them to None.
    scaling_schedule = {field: None for field in field_mapping.values()}
  else:
    policy_name = args.update_schedule
    required = set()
    scaling_schedule = {}

  for arg_attr, field in six.iteritems(field_mapping):
    arg = getattr(args, arg_attr, None)
    if arg is not None:
      scaling_schedule[field] = arg
    elif arg_attr in required:
      raise InvalidArgumentError(
          '--set-schedule argument requires --schedule-duration-sec, '
          '--schedule-cron, and --schedule-min-required-replicas to be '
          'specified.')

  return messages.AutoscalingPolicy.ScalingSchedulesValue(
      additionalProperties=[
          scaling_schedule_wrapper(
              key=policy_name,
              value=messages.AutoscalingPolicyScalingSchedule(
                  **scaling_schedule))])


def _BuildAutoscalerPolicy(args, messages, original, predictive=False,
                           scheduled=False):
  """Builds AutoscalingPolicy from args.

  Args:
    args: command line arguments.
    messages: module containing message classes.
    original: original autoscaler message.
    predictive: bool, whether to inclue the
      `autoscalingPolicy.cpuUtilization.predictiveMethod` field in the message.
    scheduled: bool, whether to include the
      `autoscalingPolicy.scalingSchedules` field in the message.
  Returns:
    AutoscalingPolicy message object.
  """
  policy_dict = {
      'coolDownPeriodSec': args.cool_down_period,
      'cpuUtilization': _BuildCpuUtilization(args, messages, predictive),
      'customMetricUtilizations': _BuildCustomMetricUtilizations(
          args, messages, original),
      'loadBalancingUtilization': _BuildLoadBalancingUtilization(
          args, messages),
      'maxNumReplicas': args.max_num_replicas,
      'minNumReplicas': args.min_num_replicas,
  }
  policy_dict['mode'] = _BuildMode(args, messages, original)
  policy_dict['scaleInControl'] = BuildScaleIn(args, messages)
  if scheduled:
    policy_dict['scalingSchedules'] = BuildScheduled(args, messages)

  return messages.AutoscalingPolicy(
      **dict((key, value) for key, value in six.iteritems(policy_dict)
             if value is not None))  # Filter out None values.


def AdjustAutoscalerNameForCreation(autoscaler_resource, igm_ref):
  """Set name of autoscaler o be created.

  If autoscaler name is not None it wNone ill be used as a prefix of name of the
  autoscaler to be created. Prefix may be shortened so that the name fits below
  length limit. Name prefix is followed by '-' character and four
  random letters.

  Args:
    autoscaler_resource: Autoscaler resource to be created.
    igm_ref: reference to Instance Group Manager targeted by the Autoscaler.
  """
  if autoscaler_resource.name is None:
    autoscaler_resource.name = igm_ref.Name()
  trimmed_name = autoscaler_resource.name[
      0:(_MAX_AUTOSCALER_NAME_LENGTH - _NUM_RANDOM_CHARACTERS_IN_AS_NAME - 1)]
  random_characters = [
      random.choice(string.ascii_lowercase + string.digits)
      for _ in range(_NUM_RANDOM_CHARACTERS_IN_AS_NAME)
  ]
  random_suffix = ''.join(random_characters)
  new_name = '{0}-{1}'.format(trimmed_name, random_suffix)
  autoscaler_resource.name = new_name


def BuildAutoscaler(args,
                    messages,
                    igm_ref,
                    name,
                    original,
                    predictive=False,
                    scheduled=False):
  """Builds autoscaler message protocol buffer."""
  autoscaler = messages.Autoscaler(
      autoscalingPolicy=_BuildAutoscalerPolicy(
          args,
          messages,
          original,
          predictive=predictive,
          scheduled=scheduled),
      description=args.description,
      name=name,
      target=igm_ref.SelfLink(),
  )
  return autoscaler


def CreateAutohealingPolicies(messages, health_check, initial_delay):
  """Creates autohealing policy list from args."""
  if health_check is None and initial_delay is None:
    return []
  policy = messages.InstanceGroupManagerAutoHealingPolicy()
  if health_check:
    policy.healthCheck = health_check
  if initial_delay:
    policy.initialDelaySec = initial_delay
  return [policy]


def ModifyAutohealingPolicies(current_policies, messages, args,
                              health_check_url):
  """Modifies existing autohealing policy from args."""
  if args.clear_autohealing:
    return [messages.InstanceGroupManagerAutoHealingPolicy()]
  if not health_check_url and not args.initial_delay:
    # No modifications of AutoHealingPolicies.
    return None
  current_policy = current_policies[0] if current_policies else None
  policy = current_policy or messages.InstanceGroupManagerAutoHealingPolicy()
  if health_check_url:
    policy.healthCheck = health_check_url
  if args.initial_delay:
    policy.initialDelaySec = args.initial_delay
  return [policy]


def ValidateAutohealingPolicies(auto_healing_policies):
  """Validates autohealing policies.

  Args:
    auto_healing_policies: list of AutoHealingPolicies to validate
  """
  if not auto_healing_policies:
    return
  # Only a single auto_healing_policy is allowed. Displaying warnings for any
  # additional entries is unnecessary as an error will be returned.
  policy = auto_healing_policies[0]
  if not policy.healthCheck and policy.initialDelaySec:
    message = ('WARNING: Health check should be provided when specifying '
               'initial delay.')
    console_io.PromptContinue(message=message, cancel_on_no=True)


def _GetInstanceTemplatesSet(*versions_lists):
  versions_set = set()
  for versions_list in versions_lists:
    versions_set.update(versions_list)
  return versions_set


def ValidateVersions(igm_info, new_versions, resources, force=False):
  """Validates whether versions provided by user are consistent.

  Args:
    igm_info: instance group manager resource.
    new_versions: list of new versions.
    force: if true, we allow any combination of instance templates, as long as
    they are different. If false, only the following transitions are allowed: X
      -> Y, X -> (X, Y), (X, Y) -> X, (X, Y) -> Y, (X, Y) -> (X, Y)
  """
  if (len(new_versions) == 2 and
      new_versions[0].instanceTemplate == new_versions[1].instanceTemplate):
    raise calliope_exceptions.ToolException(
        'Provided instance templates must be different.')
  if force:
    return

  # Only X -> Y, X -> (X, Y), (X, Y) -> X, (X, Y) -> Y, (X, Y) -> (X, Y)
  # are allowed in gcloud (unless --force)
  # Equivalently, at most two versions in old and new versions set union
  if igm_info.versions:
    igm_templates = [
        resources.ParseURL(version.instanceTemplate).RelativeName()
        for version in igm_info.versions
    ]
  elif igm_info.instanceTemplate:
    igm_templates = [
        resources.ParseURL(igm_info.instanceTemplate).RelativeName()
    ]
  else:
    raise calliope_exceptions.ToolException(
        'Either versions or instance template must be specified for '
        'managed instance group.')

  new_templates = [
      resources.ParseURL(version.instanceTemplate).RelativeName()
      for version in new_versions
  ]

  version_count = len(_GetInstanceTemplatesSet(igm_templates, new_templates))
  if version_count > 2:
    raise calliope_exceptions.ToolException(
        'Update inconsistent with current state. '
        'The only allowed transitions between versions are: '
        'X -> Y, X -> (X, Y), (X, Y) -> X, (X, Y) -> Y, (X, Y) -> (X, Y). '
        'Please check versions templates or use --force.')


def AddAutoscaledPropertyToMigs(migs, client, resources):
  """Add Autoscaler information if Autoscaler is defined for the MIGs.

  Issue additional queries to detect if any given Instange Group Manager is
  a target of some autoscaler and add this information to in 'autoscaled'
  property.

  Args:
    migs: list of dicts, List of IGM resources converted to dictionaries
    client: a GCE client
    resources: a GCE resource registry

  Returns:
    Pair of:
    - boolean - True iff any autoscaler has an error
    - Copy of migs list with additional property 'autoscaled' set to 'No'/'Yes'/
    'Yes (*)' for each MIG depending on look-up result.
  """

  augmented_migs = []
  had_errors = False
  for mig in AddAutoscalersToMigs(
      migs_iterator=_ComputeInstanceGroupSize(migs, client, resources),
      client=client,
      resources=resources,
      fail_when_api_not_supported=False):
    if 'autoscaler' in mig and mig['autoscaler'] is not None:
      # status is present in autoscaler iff Autoscaler message has embedded
      # StatusValueValuesEnum defined.
      if (getattr(mig['autoscaler'], 'status', False) and mig['autoscaler']
          .status == client.messages.Autoscaler.StatusValueValuesEnum.ERROR):
        mig['autoscaled'] = 'yes (*)'
        had_errors = True
      else:
        mig['autoscaled'] = 'yes'
    else:
      mig['autoscaled'] = 'no'
    augmented_migs.append(mig)
  return (had_errors, augmented_migs)


def _ComputeInstanceGroupSize(items, client, resources):
  """Add information about Instance Group size."""
  errors = []
  zone_refs = [
      resources.Parse(
          mig['zone'],
          params={'project': properties.VALUES.core.project.GetOrFail},
          collection='compute.zones') for mig in items if 'zone' in mig
  ]
  region_refs = [
      resources.Parse(
          mig['region'],
          params={'project': properties.VALUES.core.project.GetOrFail},
          collection='compute.regions') for mig in items if 'region' in mig
  ]

  zonal_instance_groups = []
  for project, zone_refs in six.iteritems(GroupByProject(zone_refs)):
    zonal_instance_groups.extend(
        lister.GetZonalResources(
            service=client.apitools_client.instanceGroups,
            project=project,
            requested_zones=set([zone.zone for zone in zone_refs]),
            filter_expr=None,
            http=client.apitools_client.http,
            batch_url=client.batch_url,
            errors=errors))

  regional_instance_groups = []
  if getattr(client.apitools_client, 'regionInstanceGroups', None):
    for project, region_refs in six.iteritems(GroupByProject(region_refs)):
      regional_instance_groups.extend(
          lister.GetRegionalResources(
              service=client.apitools_client.regionInstanceGroups,
              project=project,
              requested_regions=set([region.region for region in region_refs]),
              filter_expr=None,
              http=client.apitools_client.http,
              batch_url=client.batch_url,
              errors=errors))

  instance_groups = zonal_instance_groups + regional_instance_groups
  instance_group_uri_to_size = {ig.selfLink: ig.size for ig in instance_groups}

  if errors:
    utils.RaiseToolException(errors)

  for item in items:
    self_link = item['selfLink']
    gm_self_link = self_link.replace('/instanceGroupManagers/',
                                     '/instanceGroups/')

    item['size'] = str(instance_group_uri_to_size.get(gm_self_link, ''))
    yield item


def GetHealthCheckUri(resources, args):
  """Creates health check reference from args."""
  if args.health_check:
    ref = auto_healing_utils.HEALTH_CHECK_ARG.ResolveAsResource(args, resources)
    return ref.SelfLink()
  if args.http_health_check:
    return resources.Parse(
        args.http_health_check,
        params={'project': properties.VALUES.core.project.GetOrFail},
        collection='compute.httpHealthChecks').SelfLink()
  if args.https_health_check:
    return resources.Parse(
        args.https_health_check,
        params={'project': properties.VALUES.core.project.GetOrFail},
        collection='compute.httpsHealthChecks').SelfLink()


# TODO(b/70203649): improve/fix method (no silent errors, add optimizations)
def CreateInstanceReferences(holder, igm_ref, instance_names):
  """Creates references to instances in instance group (zonal or regional)."""
  if igm_ref.Collection() == 'compute.instanceGroupManagers':
    instance_refs = []
    for instance in instance_names:
      instance_refs.append(
          holder.resources.Parse(
              instance,
              params={
                  'project': igm_ref.project,
                  'zone': igm_ref.zone,
              },
              collection='compute.instances'))
    return instance_refs
  elif igm_ref.Collection() == 'compute.regionInstanceGroupManagers':
    messages = holder.client.messages
    request = (
        messages.ComputeRegionInstanceGroupManagersListManagedInstancesRequest)(
            instanceGroupManager=igm_ref.Name(),
            region=igm_ref.region,
            project=igm_ref.project)
    managed_instances = list_pager.YieldFromList(
        service=holder.client.apitools_client.regionInstanceGroupManagers,
        batch_size=500,
        request=request,
        method='ListManagedInstances',
        field='managedInstances',
    )
    instances_to_return = []
    for instance_ref in managed_instances:
      if path_simplifier.Name(
          instance_ref.instance
      ) in instance_names or instance_ref.instance in instance_names:
        instances_to_return.append(instance_ref.instance)
    return instances_to_return
  else:
    raise ValueError('Unknown reference type {0}'.format(igm_ref.Collection()))


def GetDeviceNamesFromStatefulPolicy(stateful_policy):
  """Returns a list of device names from given StatefulPolicy message."""
  if not stateful_policy or not stateful_policy.preservedState \
      or not stateful_policy.preservedState.disks:
    return []
  return [disk.key
          for disk in stateful_policy.preservedState.disks.additionalProperties]


def IsAutoscalerNew(autoscaler):
  return getattr(autoscaler, 'name', None) is None


def ApplyInstanceRedistributionTypeToUpdatePolicy(
    client, instance_redistribution_type, update_policy):
  if instance_redistribution_type:
    if update_policy is None:
      update_policy = client.messages.InstanceGroupManagerUpdatePolicy()
    update_policy.instanceRedistributionType = (
        client.messages.InstanceGroupManagerUpdatePolicy.
        InstanceRedistributionTypeValueValuesEnum)(
            instance_redistribution_type)
  return update_policy


def ListPerInstanceConfigs(client, igm_ref):
  """Lists per-instance-configs for a given IGM."""
  if not hasattr(client.messages,
                 'ComputeInstanceGroupManagersListPerInstanceConfigsRequest'):
    return []
  if igm_ref.Collection() == 'compute.instanceGroupManagers':
    service = client.apitools_client.instanceGroupManagers
    request = (client.messages
               .ComputeInstanceGroupManagersListPerInstanceConfigsRequest)(
                   instanceGroupManager=igm_ref.Name(),
                   project=igm_ref.project,
                   zone=igm_ref.zone,
               )
  elif igm_ref.Collection() == 'compute.regionInstanceGroupManagers':
    service = client.apitools_client.regionInstanceGroupManagers
    request = (client.messages.
               ComputeRegionInstanceGroupManagersListPerInstanceConfigsRequest)(
                   instanceGroupManager=igm_ref.Name(),
                   project=igm_ref.project,
                   region=igm_ref.region,
               )
  else:
    raise ValueError('Unknown reference type {0}'.format(igm_ref.Collection()))

  errors = []
  results = list(
      request_helper.MakeRequests(
          requests=[(service, 'ListPerInstanceConfigs', request)],
          http=client.apitools_client.http,
          batch_url=client.batch_url,
          errors=errors))

  if not results:
    return []
  return results[0].items


def IsStateful(client, igm_info, igm_ref):
  """For a given IGM, returns if it is stateful."""
  # TODO(b/129752312): use igm.is_stateful field when launched
  pics = ListPerInstanceConfigs(client, igm_ref)
  has_policy = (
      hasattr(igm_info, 'statefulPolicy') and
      igm_info.statefulPolicy is not None)

  return has_policy or pics


def ValidateIgmReadyForStatefulness(igm_resource, client):
  """Throws exception if IGM is in state not ready for adding statefulness."""
  if not igm_resource.updatePolicy:
    return

  client_update_policy = client.messages.InstanceGroupManagerUpdatePolicy
  type_is_proactive = (
      igm_resource.updatePolicy.type == (
          client_update_policy.TypeValueValuesEnum.PROACTIVE))
  replacement_method_is_substitute = (
      igm_resource.updatePolicy.replacementMethod == (
          client_update_policy.ReplacementMethodValueValuesEnum.SUBSTITUTE))
  instance_redistribution_type_is_proactive = (
      igm_resource.updatePolicy.instanceRedistributionType == (
          client_update_policy.InstanceRedistributionTypeValueValuesEnum
          .PROACTIVE))

  if type_is_proactive and replacement_method_is_substitute:
    raise exceptions.Error(
        'Stateful IGMs cannot use SUBSTITUTE replacement method. '
        'Try `gcloud alpha compute instance-groups managed '
        'rolling-update stop-proactive-update')
  if instance_redistribution_type_is_proactive:
    raise exceptions.Error(
        'Stateful regional IGMs cannot use proactive instance redistribution. '
        'Try `gcloud alpha compute instance-groups managed '
        'update --instance-redistribution-type=NONE')


def ValueOrNone(message):
  """Return message if message is a proto with one or more fields set or None.

  If message is None or is the default proto, it returns None. In all other
  cases, it returns the message.

  Args:
    message: An generated proto message object.

  Returns:
    message if message is initialized or None
  """
  if message is None:
    return message
  default_object = message.__class__()
  return message if message != default_object else None
