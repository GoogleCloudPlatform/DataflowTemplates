# -*- coding: utf-8 -*- #
# Copyright 2017 Google LLC. All Rights Reserved.
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
"""Common utility functions for sql instance commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import getpass

from googlecloudsdk.api_lib.sql import constants
from googlecloudsdk.api_lib.sql import instance_prop_reducers as reducers
from googlecloudsdk.api_lib.sql import validate
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib import info_holder
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.core import execution_utils
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.console import console_io

DEFAULT_RELEASE_TRACK = base.ReleaseTrack.GA

# PD = Persistent Disk. This is prefixed to all storage type payloads.
STORAGE_TYPE_PREFIX = 'PD_'


def GetInstanceRef(args, client):
  """Validates and returns the instance reference."""
  validate.ValidateInstanceName(args.instance)
  return client.resource_parser.Parse(
      args.instance,
      params={'project': properties.VALUES.core.project.GetOrFail},
      collection='sql.instances')


def GetDatabaseArgs(args, flags):
  """Gets the args for specifying a database during instance connection."""
  command_line_args = []
  if args.IsSpecified('database'):
    try:
      command_line_args.extend([flags['database'], args.database])
    except KeyError:
      raise exceptions.InvalidArgumentException(
          '--database', 'This instance does not support the database argument.')
  return command_line_args


def ConnectToInstance(cmd_args, sql_user):
  """Connects to the instance using the relevant CLI."""
  try:
    log.status.write(
        'Connecting to database with SQL user [{0}].'.format(sql_user))
    execution_utils.Exec(cmd_args)
  except OSError:
    log.error('Failed to execute command "{0}"'.format(' '.join(cmd_args)))
    log.Print(info_holder.InfoHolder())


def _GetAndValidateCmekKeyName(args, is_master):
  """Parses the CMEK resource arg, makes sure the key format was correct."""
  kms_ref = args.CONCEPTS.kms_key.Parse()
  if kms_ref:
    # Since CMEK is required for replicas of CMEK masters, this prompt is only
    # actionable for master instances.
    if is_master:
      _ShowCmekPrompt()
    return kms_ref.RelativeName()
  else:
    # Check for partially specified disk-encryption-key.
    for keyword in [
        'disk-encryption-key', 'disk-encryption-key-keyring',
        'disk-encryption-key-location', 'disk-encryption-key-project'
    ]:
      if getattr(args, keyword.replace('-', '_'), None):
        raise exceptions.InvalidArgumentException('--disk-encryption-key',
                                                  'not fully specified.')


def _GetZone(args):
  return args.zone or args.gce_zone


def _IsAlpha(release_track):
  return release_track == base.ReleaseTrack.ALPHA


def _IsBetaOrNewer(release_track):
  return release_track == base.ReleaseTrack.BETA or _IsAlpha(release_track)


def _ParseActivationPolicy(sql_messages, policy):
  if policy:
    return sql_messages.Settings.ActivationPolicyValueValuesEnum.lookup_by_name(
        policy.replace('-', '_').upper())
  return None


def _ParseAvailabilityType(sql_messages, availability_type):
  if availability_type:
    return sql_messages.Settings.AvailabilityTypeValueValuesEnum.lookup_by_name(
        availability_type.upper())
  return None


def _ParseDatabaseVersion(sql_messages, database_version):
  if database_version:
    return sql_messages.DatabaseInstance.DatabaseVersionValueValuesEnum.lookup_by_name(
        database_version.upper())
  return None


def _ParsePricingPlan(sql_messages, pricing_plan):
  if pricing_plan:
    return sql_messages.Settings.PricingPlanValueValuesEnum.lookup_by_name(
        pricing_plan.upper())
  return None


def _ParseReplicationType(sql_messages, replication):
  if replication:
    return sql_messages.Settings.ReplicationTypeValueValuesEnum.lookup_by_name(
        replication.upper())
  return None


def _ParseStorageType(sql_messages, storage_type):
  if storage_type:
    return sql_messages.Settings.DataDiskTypeValueValuesEnum.lookup_by_name(
        storage_type.upper())
  return None


# TODO(b/122660263): Remove when V1 instances are no longer supported.
def ShowV1DeprecationWarning(plural=False):
  message = (
      'Upgrade your First Generation instance{} to Second Generation before we '
      'auto-upgrade {} on March 4, 2020, ahead of the full decommission of '
      'First Generation on March 25, 2020.')
  if plural:
    log.warning(message.format('s', 'them'))
  else:
    log.warning(message.format('', 'it'))


def ShowZoneDeprecationWarnings(args):
  """Show warnings if both region and zone are specified or neither is.

  Args:
      args: argparse.Namespace, The arguments that the command was invoked with.
  """

  region_specified = args.IsSpecified('region')
  zone_specified = args.IsSpecified('gce_zone') or args.IsSpecified('zone')

  # TODO(b/73362371): Remove this check; user must specify a location flag.
  if not (region_specified or zone_specified):
    log.warning('Starting with release 233.0.0, you will need to specify '
                'either a region or a zone to create an instance.')


def ShowCmekWarning(resource_type_label, instance_type_label=None):
  if instance_type_label is None:
    log.warning(
        'Your {} will be encrypted with a customer-managed key. If anyone '
        'destroys this key, all data encrypted with it will be permanently '
        'lost.'.format(resource_type_label))
  else:
    log.warning(
        'Your {} will be encrypted with {}\'s customer-managed encryption key. '
        'If anyone destroys this key, all data encrypted with it will be '
        'permanently lost.'.format(resource_type_label, instance_type_label))


def _ShowCmekPrompt():
  log.warning(
      'You are creating a Cloud SQL instance encrypted with a customer-managed '
      'key. If anyone destroys a customer-managed key, all data encrypted with '
      'it will be permanently lost.\n')
  console_io.PromptContinue(cancel_on_no=True)


class _BaseInstances(object):
  """Common utility functions for sql instance commands."""\

  @classmethod
  def _ConstructBaseSettingsFromArgs(cls,
                                     sql_messages,
                                     args,
                                     instance=None,
                                     release_track=DEFAULT_RELEASE_TRACK):
    """Constructs instance settings from the command line arguments.

    Args:
      sql_messages: module, The messages module that should be used.
      args: argparse.Namespace, The arguments that this command was invoked
        with.
      instance: sql_messages.DatabaseInstance, The original instance, for
        settings that depend on the previous state.
      release_track: base.ReleaseTrack, the release track that this was run
        under.

    Returns:
      A settings object representing the instance settings.

    Raises:
      ToolException: An error other than http error occurred while executing the
          command.
    """

    # This code is shared by create and patch, but these args don't exist in
    # create anymore, so insert them here to avoid regressions below.
    if 'authorized_gae_apps' not in args:
      args.authorized_gae_apps = None
    if 'follow_gae_app' not in args:
      args.follow_gae_app = None
    if 'pricing_plan' not in args:
      args.pricing_plan = 'PER_USE'

    settings = sql_messages.Settings(
        kind='sql#settings',
        tier=reducers.MachineType(instance, args.tier, args.memory, args.cpu),
        pricingPlan=_ParsePricingPlan(sql_messages, args.pricing_plan),
        replicationType=_ParseReplicationType(sql_messages, args.replication),
        activationPolicy=_ParseActivationPolicy(sql_messages,
                                                args.activation_policy))

    if args.authorized_gae_apps:
      settings.authorizedGaeApplications = args.authorized_gae_apps

    if any([
        args.assign_ip is not None, args.require_ssl is not None,
        args.authorized_networks
    ]):
      settings.ipConfiguration = sql_messages.IpConfiguration()
      if args.assign_ip is not None:
        cls.SetIpConfigurationEnabled(settings, args.assign_ip)

      if args.authorized_networks:
        cls.SetAuthorizedNetworks(settings, args.authorized_networks,
                                  sql_messages.AclEntry)

      if args.require_ssl is not None:
        settings.ipConfiguration.requireSsl = args.require_ssl

    if any([args.follow_gae_app, _GetZone(args)]):
      settings.locationPreference = sql_messages.LocationPreference(
          kind='sql#locationPreference',
          followGaeApplication=args.follow_gae_app,
          zone=_GetZone(args))

    if args.storage_size:
      settings.dataDiskSizeGb = int(args.storage_size / constants.BYTES_TO_GB)

    if args.storage_auto_increase is not None:
      settings.storageAutoResize = args.storage_auto_increase

    if args.IsSpecified('availability_type'):
      settings.availabilityType = _ParseAvailabilityType(
          sql_messages, args.availability_type)

    # BETA args.
    if _IsBetaOrNewer(release_track):
      if args.IsSpecified('storage_auto_increase_limit'):
        # Resize limit should be settable if the original instance has resize
        # turned on, or if the instance to be created has resize flag.
        if (instance and instance.settings.storageAutoResize) or (
            args.storage_auto_increase):
          # If the limit is set to None, we want it to be set to 0. This is a
          # backend requirement.
          settings.storageAutoResizeLimit = (
              args.storage_auto_increase_limit or 0)
        else:
          raise exceptions.RequiredArgumentException(
              '--storage-auto-increase', 'To set the storage capacity limit '
              'using [--storage-auto-increase-limit], '
              '[--storage-auto-increase] must be enabled.')

      if args.IsSpecified('network'):
        if not settings.ipConfiguration:
          settings.ipConfiguration = sql_messages.IpConfiguration()
        settings.ipConfiguration.privateNetwork = reducers.PrivateNetworkUrl(
            args.network)

    return settings

  @classmethod
  def _ConstructCreateSettingsFromArgs(cls,
                                       sql_messages,
                                       args,
                                       instance=None,
                                       release_track=DEFAULT_RELEASE_TRACK):
    """Constructs create settings object from base settings and args."""
    original_settings = instance.settings if instance else None
    settings = cls._ConstructBaseSettingsFromArgs(sql_messages, args, instance,
                                                  release_track)

    backup_configuration = (
        reducers.BackupConfiguration(
            sql_messages,
            instance,
            backup_enabled=args.backup,
            backup_location=args.backup_location,
            backup_start_time=args.backup_start_time,
            enable_bin_log=args.enable_bin_log,
            enable_point_in_time_recovery=args.enable_point_in_time_recovery,
            retained_backups_count=args.retained_backups_count,
            retained_transaction_log_days=args.retained_transaction_log_days))
    if backup_configuration:
      cls.AddBackupConfigToSettings(settings, backup_configuration)

    settings.databaseFlags = (
        reducers.DatabaseFlags(
            sql_messages, original_settings,
            database_flags=args.database_flags))

    settings.maintenanceWindow = (
        reducers.MaintenanceWindow(
            sql_messages,
            instance,
            maintenance_release_channel=args.maintenance_release_channel,
            maintenance_window_day=args.maintenance_window_day,
            maintenance_window_hour=args.maintenance_window_hour))

    if args.deny_maintenance_period_start_date and args.deny_maintenance_period_end_date:
      settings.denyMaintenancePeriods = []
      settings.denyMaintenancePeriods.append(
          reducers.DenyMaintenancePeriod(
              sql_messages,
              instance,
              deny_maintenance_period_start_date=args
              .deny_maintenance_period_start_date,
              deny_maintenance_period_end_date=args
              .deny_maintenance_period_end_date,
              deny_maintenance_period_time=args.deny_maintenance_period_time))

    settings.insightsConfig = (
        reducers.InsightsConfig(
            sql_messages,
            insights_config_query_insights_enabled=args
            .insights_config_query_insights_enabled,
            insights_config_query_string_length=args
            .insights_config_query_string_length,
            insights_config_record_application_tags=args
            .insights_config_record_application_tags,
            insights_config_record_client_address=args
            .insights_config_record_client_address))

    if args.storage_type:
      settings.dataDiskType = _ParseStorageType(
          sql_messages, STORAGE_TYPE_PREFIX + args.storage_type)

    # BETA args.
    if _IsBetaOrNewer(release_track):
      settings.userLabels = labels_util.ParseCreateArgs(
          args, sql_messages.Settings.UserLabelsValue)

    # ALPHA args.
    if _IsAlpha(release_track):
      if args.active_directory_domain is not None:
        settings.activeDirectoryConfig = (
            reducers.ActiveDirectoryConfig(sql_messages,
                                           args.active_directory_domain))

    return settings

  @classmethod
  def _ConstructPatchSettingsFromArgs(cls,
                                      sql_messages,
                                      args,
                                      instance,
                                      release_track=DEFAULT_RELEASE_TRACK):
    """Constructs patch settings object from base settings and args."""
    original_settings = instance.settings
    settings = cls._ConstructBaseSettingsFromArgs(sql_messages, args, instance,
                                                  release_track)

    if args.clear_gae_apps:
      settings.authorizedGaeApplications = []

    if any([args.follow_gae_app, _GetZone(args)]):
      settings.locationPreference = sql_messages.LocationPreference(
          kind='sql#locationPreference',
          followGaeApplication=args.follow_gae_app,
          zone=_GetZone(args))

    if args.clear_authorized_networks:
      if not settings.ipConfiguration:
        settings.ipConfiguration = sql_messages.IpConfiguration()
      settings.ipConfiguration.authorizedNetworks = []

    if args.enable_database_replication is not None:
      settings.databaseReplicationEnabled = args.enable_database_replication

    backup_configuration = (
        reducers.BackupConfiguration(
            sql_messages,
            instance,
            backup_enabled=not args.no_backup,
            backup_location=args.backup_location,
            backup_start_time=args.backup_start_time,
            enable_bin_log=args.enable_bin_log,
            enable_point_in_time_recovery=args.enable_point_in_time_recovery,
            retained_backups_count=args.retained_backups_count,
            retained_transaction_log_days=args.retained_transaction_log_days))

    if backup_configuration:
      cls.AddBackupConfigToSettings(settings, backup_configuration)

    settings.databaseFlags = (
        reducers.DatabaseFlags(
            sql_messages,
            original_settings,
            database_flags=args.database_flags,
            clear_database_flags=args.clear_database_flags))

    settings.maintenanceWindow = (
        reducers.MaintenanceWindow(
            sql_messages,
            instance,
            maintenance_release_channel=args.maintenance_release_channel,
            maintenance_window_day=args.maintenance_window_day,
            maintenance_window_hour=args.maintenance_window_hour))

    if args.remove_deny_maintenance_period:
      settings.denyMaintenancePeriods = []

    if (args.deny_maintenance_period_start_date or
        args.deny_maintenance_period_end_date or
        args.deny_maintenance_period_time):
      settings.denyMaintenancePeriods = []
      settings.denyMaintenancePeriods.append(
          reducers.DenyMaintenancePeriod(
              sql_messages,
              instance,
              deny_maintenance_period_start_date=args
              .deny_maintenance_period_start_date,
              deny_maintenance_period_end_date=args
              .deny_maintenance_period_end_date,
              deny_maintenance_period_time=args.deny_maintenance_period_time))

    settings.insightsConfig = (
        reducers.InsightsConfig(
            sql_messages,
            insights_config_query_insights_enabled=args
            .insights_config_query_insights_enabled,
            insights_config_query_string_length=args
            .insights_config_query_string_length,
            insights_config_record_application_tags=args
            .insights_config_record_application_tags,
            insights_config_record_client_address=args
            .insights_config_record_client_address))

    # BETA args.
    if _IsBetaOrNewer(release_track):
      labels_diff = labels_util.ExplicitNullificationDiff.FromUpdateArgs(args)
      labels_update = labels_diff.Apply(sql_messages.Settings.UserLabelsValue,
                                        instance.settings.userLabels)
      if labels_update.needs_update:
        settings.userLabels = labels_update.labels

    # ALPHA args.
    if _IsAlpha(release_track):
      if args.active_directory_domain is not None:
        settings.activeDirectoryConfig = (
            reducers.ActiveDirectoryConfig(sql_messages,
                                           args.active_directory_domain))

    return settings

  @classmethod
  def _ConstructBaseInstanceFromArgs(cls,
                                     sql_messages,
                                     args,
                                     original=None,
                                     instance_ref=None,
                                     release_track=DEFAULT_RELEASE_TRACK):
    """Construct a Cloud SQL instance from command line args.

    Args:
      sql_messages: module, The messages module that should be used.
      args: argparse.Namespace, The CLI arg namespace.
      original: sql_messages.DatabaseInstance, The original instance, if some of
        it might be used to fill fields in the new one.
      instance_ref: reference to DatabaseInstance object, used to fill project
        and instance information.
      release_track: base.ReleaseTrack, the release track that this was run
        under.

    Returns:
      sql_messages.DatabaseInstance, The constructed (and possibly partial)
      database instance.

    Raises:
      ToolException: An error other than http error occurred while executing the
          command.
    """
    del args, original, release_track  # Currently unused in base function.
    instance_resource = sql_messages.DatabaseInstance(kind='sql#instance')

    if instance_ref:
      cls.SetProjectAndInstanceFromRef(instance_resource, instance_ref)

    return instance_resource

  @classmethod
  def ConstructCreateInstanceFromArgs(cls,
                                      sql_messages,
                                      args,
                                      original=None,
                                      instance_ref=None,
                                      release_track=DEFAULT_RELEASE_TRACK):
    """Constructs Instance for create request from base instance and args."""
    ShowZoneDeprecationWarnings(args)
    instance_resource = cls._ConstructBaseInstanceFromArgs(
        sql_messages, args, original, instance_ref)

    instance_resource.region = reducers.Region(args.region, _GetZone(args))
    instance_resource.databaseVersion = _ParseDatabaseVersion(
        sql_messages, args.database_version)
    instance_resource.masterInstanceName = args.master_instance_name
    instance_resource.rootPassword = args.root_password

    # BETA: Set the host port and return early if external master instance.
    if _IsBetaOrNewer(release_track) and args.IsSpecified('source_ip_address'):
      on_premises_configuration = reducers.OnPremisesConfiguration(
          sql_messages, args.source_ip_address, args.source_port)
      instance_resource.onPremisesConfiguration = on_premises_configuration
      return instance_resource

    instance_resource.settings = cls._ConstructCreateSettingsFromArgs(
        sql_messages, args, original, release_track)

    if args.master_instance_name:
      replication = sql_messages.Settings.ReplicationTypeValueValuesEnum.ASYNCHRONOUS
      if args.replica_type == 'FAILOVER':
        instance_resource.replicaConfiguration = (
            sql_messages.ReplicaConfiguration(
                kind='sql#demoteMasterMysqlReplicaConfiguration',
                failoverTarget=True))
    else:
      replication = sql_messages.Settings.ReplicationTypeValueValuesEnum.SYNCHRONOUS
    if not args.replication:
      instance_resource.settings.replicationType = replication

    if args.failover_replica_name:
      instance_resource.failoverReplica = (
          sql_messages.DatabaseInstance.FailoverReplicaValue(
              name=args.failover_replica_name))

    if args.collation:
      instance_resource.settings.collation = args.collation

    # BETA: Config for creating a replica of an external master instance.
    if _IsBetaOrNewer(release_track) and args.IsSpecified('master_username'):
      # Ensure that the master instance name is specified.
      if not args.IsSpecified('master_instance_name'):
        raise exceptions.RequiredArgumentException(
            '--master-instance-name', 'To create a read replica of an external '
            'master instance, [--master-instance-name] must be specified')

      # TODO(b/78648703): Remove when mutex required status is fixed.
      # Ensure that the master replication user password is specified.
      if not (args.IsSpecified('master_password') or
              args.IsSpecified('prompt_for_master_password')):
        raise exceptions.RequiredArgumentException(
            '--master-password', 'To create a read replica of an external '
            'master instance, [--master-password] or '
            '[--prompt-for-master-password] must be specified')

      # Get password if not specified on command line.
      if args.prompt_for_master_password:
        args.master_password = getpass.getpass('Master Instance Password: ')

      instance_resource.replicaConfiguration = reducers.ReplicaConfiguration(
          sql_messages, args.master_username, args.master_password,
          args.master_dump_file_path, args.master_ca_certificate_path,
          args.client_certificate_path, args.client_key_path)

    is_master = instance_resource.masterInstanceName is None
    key_name = _GetAndValidateCmekKeyName(args, is_master)
    if key_name:
      config = sql_messages.DiskEncryptionConfiguration(
          kind='sql#diskEncryptionConfiguration', kmsKeyName=key_name)
      instance_resource.diskEncryptionConfiguration = config

    return instance_resource

  @classmethod
  def ConstructPatchInstanceFromArgs(cls,
                                     sql_messages,
                                     args,
                                     original,
                                     instance_ref=None,
                                     release_track=DEFAULT_RELEASE_TRACK):
    """Constructs Instance for patch request from base instance and args."""
    instance_resource = cls._ConstructBaseInstanceFromArgs(
        sql_messages, args, original, instance_ref)

    instance_resource.settings = cls._ConstructPatchSettingsFromArgs(
        sql_messages, args, original, release_track)

    return instance_resource


class InstancesV1Beta4(_BaseInstances):
  """Common utility functions for sql instances V1Beta4."""

  @staticmethod
  def SetProjectAndInstanceFromRef(instance_resource, instance_ref):
    instance_resource.project = instance_ref.project
    instance_resource.name = instance_ref.instance

  @staticmethod
  def AddBackupConfigToSettings(settings, backup_config):
    settings.backupConfiguration = backup_config

  @staticmethod
  def SetIpConfigurationEnabled(settings, assign_ip):
    settings.ipConfiguration.ipv4Enabled = assign_ip

  @staticmethod
  def SetAuthorizedNetworks(settings, authorized_networks, acl_entry_value):
    settings.ipConfiguration.authorizedNetworks = [
        acl_entry_value(kind='sql#aclEntry', value=n)
        for n in authorized_networks
    ]
