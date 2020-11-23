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
"""Convenience functions for dealing with instances and instance templates."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import collections
import re

from googlecloudsdk.api_lib.compute import constants
from googlecloudsdk.api_lib.compute import containers_utils
from googlecloudsdk.api_lib.compute import csek_utils
from googlecloudsdk.api_lib.compute import metadata_utils
from googlecloudsdk.api_lib.compute import utils
from googlecloudsdk.api_lib.compute import zone_utils
from googlecloudsdk.calliope import exceptions as calliope_exceptions
from googlecloudsdk.command_lib.compute import flags as compute_flags
from googlecloudsdk.command_lib.compute import scope as compute_scopes
from googlecloudsdk.command_lib.compute.instances import flags
from googlecloudsdk.command_lib.compute.sole_tenancy import util as sole_tenancy_util
from googlecloudsdk.core import log
from googlecloudsdk.core import resources as cloud_resources
import six

EMAIL_REGEX = re.compile(r'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)')

_DEFAULT_DEVICE_NAME_CONTAINER_WARNING = (
    'Default device-name for disk name [{0}] will be [{0}] because it is being '
    'mounted to a container with [`--container-mount-disk`]')


def GetCpuRamVmFamilyFromCustomName(name):
  """Gets the CPU and memory specs from the custom machine type name.

  Args:
    name: the custom machine type name for the 'instance create' call

  Returns:
    A three-tuple with the vm family, number of cpu and amount of memory for the
    custom machine type.
    custom_family, the name of the VM family
    custom_cpu, the number of cpu desired for the custom machine type instance
    custom_memory_mib, the amount of ram desired in MiB for the custom machine
      type instance
    None for both variables otherwise
  """
  check_custom = re.search('([a-zA-Z0-9]+)-custom-([0-9]+)-([0-9]+)', name)
  if check_custom:
    custom_family = check_custom.group(1)
    custom_cpu = check_custom.group(2)
    custom_memory_mib = check_custom.group(3)
    return custom_family, custom_cpu, custom_memory_mib
  return None, None, None


def GetNameForCustom(custom_cpu, custom_memory_mib, ext=False, vm_type=False):
  """Creates a custom machine type name from the desired CPU and memory specs.

  Args:
    custom_cpu: the number of cpu desired for the custom machine type
    custom_memory_mib: the amount of ram desired in MiB for the custom machine
      type instance
    ext: extended custom machine type should be used if true
    vm_type: VM instance generation

  Returns:
    The custom machine type name for the 'instance create' call
  """
  if vm_type:
    machine_type = '{0}-custom-{1}-{2}'.format(vm_type, custom_cpu,
                                               custom_memory_mib)
  else:
    machine_type = 'custom-{0}-{1}'.format(custom_cpu, custom_memory_mib)
  if ext:
    machine_type += '-ext'
  return machine_type


def InterpretMachineType(machine_type,
                         custom_cpu,
                         custom_memory,
                         ext=True,
                         vm_type=False,
                         confidential_vm=False):
  """Interprets the machine type for the instance.

  Args:
    machine_type: name of existing machine type, eg. n1-standard
    custom_cpu: number of CPU cores for custom machine type,
    custom_memory: amount of RAM memory in bytes for custom machine type,
    ext: extended custom machine type should be used if true,
    vm_type:  VM instance generation
    confidential_vm: If True, default machine type is different for confidential
      VMs.

  Returns:
    A string representing the URL naming a machine-type.

  Raises:
    calliope_exceptions.RequiredArgumentException when only one of the two
      custom machine type flags are used.
    calliope_exceptions.InvalidArgumentException when both the machine type and
      custom machine type flags are used to generate a new instance.
  """
  # Setting the machine type
  if machine_type:
    machine_type_name = machine_type
  elif confidential_vm:
    machine_type_name = constants.DEFAULT_MACHINE_TYPE_FOR_CONFIDENTIAL_VMS
  else:
    machine_type_name = constants.DEFAULT_MACHINE_TYPE

  # Setting the specs for the custom machine.
  if custom_cpu or custom_memory or ext:
    if not custom_cpu:
      raise calliope_exceptions.RequiredArgumentException(
          '--custom-cpu', 'Both [--custom-cpu] and [--custom-memory] must be '
          'set to create a custom machine type instance.')
    if not custom_memory:
      raise calliope_exceptions.RequiredArgumentException(
          '--custom-memory', 'Both [--custom-cpu] and [--custom-memory] must '
          'be set to create a custom machine type instance.')
    if machine_type:
      raise calliope_exceptions.InvalidArgumentException(
          '--machine-type', 'Cannot set both [--machine-type] and '
          '[--custom-cpu]/[--custom-memory] for the same instance.')
    custom_type_string = GetNameForCustom(
        custom_cpu,
        # converting from B to MiB.
        custom_memory // (2**20),
        ext,
        vm_type)

    # Updating the machine type that is set for the URIs
    machine_type_name = custom_type_string
  return machine_type_name


def CheckCustomCpuRamRatio(compute_client, project, zone, machine_type_name):
  """Checks that the CPU and memory ratio is a supported custom instance type.

  Args:
    compute_client: GCE API client,
    project: a project,
    zone: the zone of the instance(s) being created,
    machine_type_name: The machine type of the instance being created.

  Returns:
    Nothing. Function acts as a bound checker, and will raise an exception from
      within the function if needed.

  Raises:
    utils.RaiseToolException if a custom machine type ratio is out of bounds.
  """
  messages = compute_client.messages
  compute = compute_client.apitools_client
  if 'custom' in machine_type_name:
    mt_get_pb = messages.ComputeMachineTypesGetRequest(
        machineType=machine_type_name, project=project, zone=zone)
    mt_get_reqs = [(compute.machineTypes, 'Get', mt_get_pb)]
    errors = []

    # Makes a 'machine-types describe' request to check the bounds
    _ = list(
        compute_client.MakeRequests(
            requests=mt_get_reqs, errors_to_collect=errors))

    if errors:
      utils.RaiseToolException(
          errors, error_message='Could not fetch machine type:')


def CreateServiceAccountMessages(messages, scopes, service_account):
  """Returns a list of ServiceAccount messages corresponding to scopes."""
  if scopes is None:
    scopes = constants.DEFAULT_SCOPES
  if service_account is None:
    service_account = 'default'

  accounts_to_scopes = collections.defaultdict(list)
  for scope in scopes:
    parts = scope.split('=')
    if len(parts) == 1:
      account = service_account
      scope_uri = scope
    elif len(parts) == 2:
      # TODO(b/33688878) Remove exception for this deprecated format
      raise calliope_exceptions.InvalidArgumentException(
          '--scopes',
          'Flag format --scopes [ACCOUNT=]SCOPE,[[ACCOUNT=]SCOPE, ...] is '
          'removed. Use --scopes [SCOPE,...] --service-account ACCOUNT '
          'instead.')
    else:
      raise calliope_exceptions.ToolException(
          '[{0}] is an illegal value for [--scopes]. Values must be of the '
          'form [SCOPE].'.format(scope))

    if service_account != 'default' and not EMAIL_REGEX.match(service_account):
      raise calliope_exceptions.InvalidArgumentException(
          '--service-account',
          'Invalid format: expected default or user@domain.com, received ' +
          service_account)

    # Expands the scope if the user provided an alias like
    # "compute-rw".
    scope_uri = constants.SCOPES.get(scope_uri, [scope_uri])
    accounts_to_scopes[account].extend(scope_uri)

  if not scopes and service_account != 'default':
    return [messages.ServiceAccount(email=service_account, scopes=[])]
  res = []
  for account, scopes in sorted(six.iteritems(accounts_to_scopes)):
    res.append(messages.ServiceAccount(email=account, scopes=sorted(scopes)))
  return res


def CreateOnHostMaintenanceMessage(messages, maintenance_policy):
  """Create on-host-maintenance message for VM."""
  if maintenance_policy:
    on_host_maintenance = messages.Scheduling.OnHostMaintenanceValueValuesEnum(
        maintenance_policy)
  else:
    on_host_maintenance = None
  return on_host_maintenance


def CreateSchedulingMessage(messages,
                            maintenance_policy,
                            preemptible,
                            restart_on_failure,
                            node_affinities=None,
                            min_node_cpu=None,
                            location_hint=None,
                            maintenance_freeze_duration=None,
                            maintenance_interval=None):
  """Create scheduling message for VM."""
  # Note: We always specify automaticRestart=False for preemptible VMs. This
  # makes sense, since no-restart-on-failure is defined as "store-true", and
  # thus can't be given an explicit value. Hence it either has its default
  # value (in which case we override it for convenience's sake to the only
  # setting that makes sense for preemptible VMs), or the user actually
  # specified no-restart-on-failure, the only usable setting.
  on_host_maintenance = CreateOnHostMaintenanceMessage(messages,
                                                       maintenance_policy)
  if preemptible:
    scheduling = messages.Scheduling(
        automaticRestart=False,
        onHostMaintenance=on_host_maintenance,
        preemptible=True)
  else:
    scheduling = messages.Scheduling(
        automaticRestart=restart_on_failure,
        onHostMaintenance=on_host_maintenance)
  if node_affinities:
    scheduling.nodeAffinities = node_affinities

  if min_node_cpu is not None:
    scheduling.minNodeCpus = int(min_node_cpu)

  if location_hint:
    scheduling.locationHint = location_hint

  if maintenance_freeze_duration:
    scheduling.maintenanceFreezeDurationHours = \
      maintenance_freeze_duration // 3600  # sec to hour

  if maintenance_interval:
    scheduling.maintenanceInterval = messages.\
      Scheduling.MaintenanceIntervalValueValuesEnum(maintenance_interval)
  return scheduling


def CreateShieldedInstanceConfigMessage(messages, enable_secure_boot,
                                        enable_vtpm,
                                        enable_integrity_monitoring):
  """Create shieldedInstanceConfig message for VM."""

  shielded_instance_config = messages.ShieldedInstanceConfig(
      enableSecureBoot=enable_secure_boot,
      enableVtpm=enable_vtpm,
      enableIntegrityMonitoring=enable_integrity_monitoring)

  return shielded_instance_config


def CreateShieldedInstanceIntegrityPolicyMessage(messages,
                                                 update_auto_learn_policy=True):
  """Creates shieldedInstanceIntegrityPolicy message for VM."""

  shielded_instance_integrity_policy = messages.ShieldedInstanceIntegrityPolicy(
      updateAutoLearnPolicy=update_auto_learn_policy)

  return shielded_instance_integrity_policy


def CreateConfidentialInstanceMessage(messages, enable_confidential_compute):
  """Create confidentialInstanceConfig message for VM."""
  confidential_instance_config = messages.ConfidentialInstanceConfig(
      enableConfidentialCompute=enable_confidential_compute)

  return confidential_instance_config


def CreateAdvancedMachineFeaturesMessage(messages,
                                         enable_nested_virtualization=None):
  """Create AdvancedMachineFeatures message for an Instance."""
  return messages.AdvancedMachineFeatures(
      enableNestedVirtualization=enable_nested_virtualization)


def ParseDiskResource(resources, name, project, zone, type_):
  """Parses disk resources.

  Project and zone are ignored if a fully-qualified resource name is given, i.e.
    - https://compute.googleapis.com/compute/v1/projects/my-project
          /zones/us-central1-a/disks/disk-1
    - projects/my-project/zones/us-central1-a/disks/disk-1

  If project and zone cannot be parsed, we will use the given project and zone
  as fallbacks.

  Args:
    resources: resources.Registry, The resource registry
    name: str, name of the disk.
    project: str, project of the disk.
    zone: str, zone of the disk.
    type_: ScopeEnum, type of the disk.

  Returns:
    A disk resource.
  """
  if type_ == compute_scopes.ScopeEnum.REGION:
    return resources.Parse(
        name,
        collection='compute.regionDisks',
        params={
            'project': project,
            'region': utils.ZoneNameToRegionName(zone)
        })
  else:
    return resources.Parse(
        name,
        collection='compute.disks',
        params={
            'project': project,
            'zone': zone
        })


def ParseDiskResourceFromAttachedDisk(resources, attached_disk):
  """Parses the source disk resource of an AttachedDisk.

  The source of an AttachedDisk is either a partial or fully specified URL
  referencing either a regional or zonal disk.

  Args:
    resources: resources.Registry, The resource registry
    attached_disk: AttachedDisk

  Returns:
    A disk resource.

  Raises:
    InvalidResourceException: If the attached disk source cannot be parsed as a
        regional or zonal disk.
  """
  try:
    disk = resources.Parse(attached_disk.source,
                           collection='compute.regionDisks')
    if disk:
      return disk
  except (cloud_resources.WrongResourceCollectionException,
          cloud_resources.RequiredFieldOmittedException):
    pass

  try:
    disk = resources.Parse(attached_disk.source,
                           collection='compute.disks')
    if disk:
      return disk
  except (cloud_resources.WrongResourceCollectionException,
          cloud_resources.RequiredFieldOmittedException):
    pass

  raise cloud_resources.InvalidResourceException('Unable to parse [{}]'.format(
      attached_disk.source))


def GetDiskDeviceName(disk, name, container_mount_disk):
  """Helper method to get device-name for a disk message."""
  if (container_mount_disk and filter(
      bool, [d.get('name', name) == name for d in container_mount_disk])):
    # device-name must be the same as name if it is being mounted to a
    # container.
    if not disk.get('device-name'):
      log.warning(_DEFAULT_DEVICE_NAME_CONTAINER_WARNING.format(name))
      return name
    # This is defensive only; should be validated before this method is called.
    elif disk.get('device-name') != name:
      raise calliope_exceptions.InvalidArgumentException(
          '--container-mount-disk',
          'Attempting to mount disk named [{}] with device-name [{}]. If '
          'being mounted to container, disk name must match device-name.'
          .format(name, disk.get('device-name')))
  return disk.get('device-name')


def ParseDiskType(resources, disk_type, project, location, scope):
  """Parses disk type reference based on location scope."""
  if scope == compute_scopes.ScopeEnum.ZONE:
    collection = 'compute.diskTypes'
    params = {'project': project, 'zone': location}
  elif scope == compute_scopes.ScopeEnum.REGION:
    collection = 'compute.regionDiskTypes'
    params = {'project': project, 'region': location}
  disk_type_ref = resources.Parse(
      disk_type,
      collection=collection,
      params=params)
  return disk_type_ref


def UseExistingBootDisk(disks):
  """Returns True if the user has specified an existing boot disk."""
  return any(disk.get('boot') == 'yes' for disk in disks)


def IsAnySpecified(args, *dests):
  return any([args.IsSpecified(dest) for dest in dests])


def GetSourceInstanceTemplate(args, resources, source_instance_template_arg):
  if not args.IsSpecified('source_instance_template'):
    return None
  ref = source_instance_template_arg.ResolveAsResource(args, resources)
  return ref.SelfLink()


def GetSkipDefaults(source_instance_template):
  # gcloud creates default values for some fields in Instance resource
  # when no value was specified on command line.
  # When --source-instance-template was specified, defaults are taken from
  # Instance Template and gcloud flags are used to override them - by default
  # fields should not be initialized.
  return source_instance_template is not None


def GetScheduling(args,
                  client,
                  skip_defaults,
                  support_node_affinity=False,
                  support_min_node_cpu=True,
                  support_location_hint=False):
  """Generate a Scheduling Message or None based on specified args."""
  node_affinities = None
  if support_node_affinity:
    node_affinities = sole_tenancy_util.GetSchedulingNodeAffinityListFromArgs(
        args, client.messages)
  min_node_cpu = None
  if support_min_node_cpu:
    min_node_cpu = args.min_node_cpu
  location_hint = None
  if support_location_hint:
    location_hint = args.location_hint
  if (skip_defaults and not IsAnySpecified(
      args, 'maintenance_policy', 'preemptible', 'restart_on_failure') and
      not node_affinities):
    return None
  freeze_duration = None
  if hasattr(args, 'maintenance_freeze_duration') and args.IsSpecified(
      'maintenance_freeze_duration'):
    freeze_duration = args.maintenance_freeze_duration
  maintenance_interval = None
  if hasattr(args, 'maintenance_interval') and args.IsSpecified(
      'maintenance_interval'):
    maintenance_interval = args.maintenance_interval
  return CreateSchedulingMessage(
      messages=client.messages,
      maintenance_policy=args.maintenance_policy,
      preemptible=args.preemptible,
      restart_on_failure=args.restart_on_failure,
      node_affinities=node_affinities,
      min_node_cpu=min_node_cpu,
      location_hint=location_hint,
      maintenance_freeze_duration=freeze_duration,
      maintenance_interval=maintenance_interval)


def GetServiceAccounts(args, client, skip_defaults):
  if args.no_service_account:
    service_account = None
  else:
    service_account = args.service_account
  if (skip_defaults and not IsAnySpecified(
      args, 'scopes', 'no_scopes', 'service_account', 'no_service_account')):
    return []
  return CreateServiceAccountMessages(
      messages=client.messages,
      scopes=[] if args.no_scopes else args.scopes,
      service_account=service_account)


def GetValidatedMetadata(args, client):
  user_metadata = metadata_utils.ConstructMetadataMessage(
      client.messages,
      metadata=args.metadata,
      metadata_from_file=args.metadata_from_file)
  containers_utils.ValidateUserMetadata(user_metadata)
  return user_metadata


def GetMetadata(args, client, skip_defaults):
  if (skip_defaults and
      not IsAnySpecified(args, 'metadata', 'metadata_from_file')):
    return None
  else:
    return metadata_utils.ConstructMetadataMessage(
        client.messages,
        metadata=args.metadata,
        metadata_from_file=args.metadata_from_file)


def GetBootDiskSizeGb(args):
  boot_disk_size_gb = utils.BytesToGb(args.boot_disk_size)
  utils.WarnIfDiskSizeIsTooSmall(boot_disk_size_gb, args.boot_disk_type)
  return boot_disk_size_gb


def GetInstanceRefs(args, client, holder):
  instance_refs = flags.INSTANCES_ARG.ResolveAsResource(
      args,
      holder.resources,
      scope_lister=compute_flags.GetDefaultScopeLister(client))
  # Check if the zone is deprecated or has maintenance coming.
  zone_resource_fetcher = zone_utils.ZoneResourceFetcher(client)
  zone_resource_fetcher.WarnForZonalCreation(instance_refs)
  return instance_refs


def GetSourceMachineImageKey(args, source_image, compute_client, holder):
  machine_image_ref = source_image.ResolveAsResource(args, holder.resources)
  csek_keys = csek_utils.CsekKeyStore.FromFile(
      args.source_machine_image_csek_key_file, allow_rsa_encrypted=False)
  disk_key_or_none = csek_utils.MaybeLookupKeyMessage(
      csek_keys, machine_image_ref, compute_client.apitools_client)
  return disk_key_or_none


def CheckSpecifiedMachineTypeArgs(args, skip_defaults):
  return (not skip_defaults or
          IsAnySpecified(args, 'machine_type', 'custom_cpu', 'custom_memory'))


def CreateMachineTypeUri(args, compute_client, resource_parser, project,
                         location, scope, confidential_vm=False):
  """Create a machine type URI for given args and instance reference."""

  machine_type = args.machine_type
  custom_cpu = args.custom_cpu
  custom_memory = args.custom_memory
  vm_type = getattr(args, 'custom_vm_type', None)
  ext = getattr(args, 'custom_extensions', None)

  # Setting the machine type
  machine_type_name = InterpretMachineType(
      machine_type=machine_type,
      custom_cpu=custom_cpu,
      custom_memory=custom_memory,
      ext=ext,
      vm_type=vm_type,
      confidential_vm=confidential_vm)

  # Check to see if the custom machine type ratio is supported
  CheckCustomCpuRamRatio(compute_client, project, location, machine_type_name)

  machine_type_uri = ParseMachineType(resource_parser, machine_type_name,
                                      project, location, scope)
  return machine_type_uri


def ParseMachineType(resource_parser, machine_type_name, project, location,
                     scope):
  """Returns the location-specific machine type uri."""
  if scope == compute_scopes.ScopeEnum.ZONE:
    collection = 'compute.machineTypes'
    params = {'project': project, 'zone': location}
  elif scope == compute_scopes.ScopeEnum.REGION:
    collection = 'compute.regionMachineTypes'
    params = {'project': project, 'region': location}
  machine_type_uri = resource_parser.Parse(
      machine_type_name, collection=collection, params=params).SelfLink()
  return machine_type_uri


def GetCanIpForward(args, skip_defaults):
  if skip_defaults and not args.IsSpecified('can_ip_forward'):
    return None
  return args.can_ip_forward


def GetTags(args, client):
  if args.tags:
    return client.messages.Tags(items=args.tags)
  return None


def GetLabels(args, client, instance_properties=False):
  """Gets labels for the instance message."""
  labels_value = client.messages.Instance.LabelsValue
  if instance_properties:
    labels_value = client.messages.InstanceProperties.LabelsValue
  if args.labels:
    return labels_value(additionalProperties=[
        labels_value.AdditionalProperty(
            key=key, value=value)
        for key, value in sorted(six.iteritems(args.labels))
    ])
  return None


def ParseAcceleratorType(accelerator_type_name, resource_parser, project,
                         location, scope):
  """Returns accelerator type ref based on location scope."""
  if scope == compute_scopes.ScopeEnum.ZONE:
    collection = 'compute.acceleratorTypes'
    params = {'project': project, 'zone': location}
  elif scope == compute_scopes.ScopeEnum.REGION:
    collection = 'compute.regionAcceleratorTypes'
    params = {'project': project, 'region': location}
  accelerator_type = resource_parser.Parse(
      accelerator_type_name, collection=collection, params=params).SelfLink()
  return accelerator_type


def ResolveSnapshotURI(user_project, snapshot, resource_parser):
  if user_project and snapshot and resource_parser:
    snapshot_ref = resource_parser.Parse(
        snapshot,
        collection='compute.snapshots',
        params={'project': user_project})
    return snapshot_ref.SelfLink()
  return None


def GetReservationAffinity(args, client):
  """Returns the message of reservation affinity for the instance."""
  if args.IsSpecified('reservation_affinity'):
    type_msgs = (
        client.messages.ReservationAffinity
        .ConsumeReservationTypeValueValuesEnum)

    reservation_key = None
    reservation_values = []

    if args.reservation_affinity == 'none':
      reservation_type = type_msgs.NO_RESERVATION
    elif args.reservation_affinity == 'specific':
      reservation_type = type_msgs.SPECIFIC_RESERVATION
      # Currently, the key is fixed and the value is the name of the
      # reservation.
      # The value being a repeated field is reserved for future use when user
      # can specify more than one reservation names from which the VM can take
      # capacity from.
      reservation_key = _RESERVATION_AFFINITY_KEY
      reservation_values = [args.reservation]
    else:
      reservation_type = type_msgs.ANY_RESERVATION

    return client.messages.ReservationAffinity(
        consumeReservationType=reservation_type,
        key=reservation_key or None,
        values=reservation_values)

  return None


_RESERVATION_AFFINITY_KEY = 'compute.googleapis.com/reservation-name'
