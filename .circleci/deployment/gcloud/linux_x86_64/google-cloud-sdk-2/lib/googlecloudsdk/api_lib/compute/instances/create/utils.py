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
"""Convenience functions for dealing with instances create."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import ipaddress

from googlecloudsdk.api_lib.compute import alias_ip_range_utils
from googlecloudsdk.api_lib.compute import constants
from googlecloudsdk.api_lib.compute import csek_utils
from googlecloudsdk.api_lib.compute import image_utils
from googlecloudsdk.api_lib.compute import instance_utils
from googlecloudsdk.api_lib.compute import kms_utils
from googlecloudsdk.api_lib.compute import utils
from googlecloudsdk.command_lib.compute import scope as compute_scopes
from googlecloudsdk.command_lib.compute.instances import flags as instances_flags
from googlecloudsdk.core import log
import six


def CheckSpecifiedDiskArgs(args,
                           support_disks=True,
                           skip_defaults=False,
                           support_kms=False,
                           support_nvdimm=False):
  """Checks if relevant disk arguments have been specified."""
  flags_to_check = [
      'local_ssd',
      'boot_disk_type',
      'boot_disk_device_name',
      'boot_disk_auto_delete',
  ]

  if support_disks:
    flags_to_check.extend([
        'disk',
        'require_csek_key_create',
    ])
  if support_kms:
    flags_to_check.extend([
        'create_disk',
        'boot_disk_kms_key',
        'boot_disk_kms_project',
        'boot_disk_kms_location',
        'boot_disk_kms_keyring',
    ])
  if support_nvdimm:
    flags_to_check.extend(['local_nvdimm'])

  if (skip_defaults and
      not instance_utils.IsAnySpecified(args, *flags_to_check)):
    return False
  return True


def CreateDiskMessages(args,
                       project,
                       location,
                       scope,
                       compute_client,
                       resource_parser,
                       image_uri,
                       holder=None,
                       boot_disk_size_gb=None,
                       instance_name=None,
                       create_boot_disk=False,
                       csek_keys=None,
                       support_kms=False,
                       support_nvdimm=False,
                       support_disk_resource_policy=False,
                       support_source_snapshot_csek=False,
                       support_boot_snapshot_uri=False,
                       support_image_csek=False,
                       support_match_container_mount_disks=False,
                       support_create_disk_snapshots=False,
                       support_persistent_attached_disks=True,
                       support_replica_zones=False,
                       use_disk_type_uri=True):
  """Creates disk messages for a single instance."""

  container_mount_disk = []
  if support_match_container_mount_disks:
    container_mount_disk = args.container_mount_disk

  persistent_disks = []
  if support_persistent_attached_disks:
    persistent_disks = (
        CreatePersistentAttachedDiskMessages(
            resources=resource_parser,
            compute_client=compute_client,
            csek_keys=csek_keys,
            disks=args.disk or [],
            project=project,
            location=location,
            scope=scope,
            container_mount_disk=container_mount_disk))

  persistent_create_disks = (
      CreatePersistentCreateDiskMessages(
          compute_client=compute_client,
          resources=resource_parser,
          csek_keys=csek_keys,
          create_disks=getattr(args, 'create_disk', []),
          project=project,
          location=location,
          scope=scope,
          holder=holder,
          enable_kms=support_kms,
          enable_snapshots=support_create_disk_snapshots,
          container_mount_disk=container_mount_disk,
          resource_policy=support_disk_resource_policy,
          enable_source_snapshot_csek=support_source_snapshot_csek,
          enable_image_csek=support_image_csek,
          support_replica_zones=support_replica_zones,
          use_disk_type_uri=use_disk_type_uri))

  local_nvdimms = []
  if support_nvdimm:
    local_nvdimms = CreateLocalNvdimmMessages(args, resource_parser,
                                              compute_client.messages, location,
                                              scope, project)

  local_ssds = CreateLocalSsdMessages(args, resource_parser,
                                      compute_client.messages, location, scope,
                                      project, use_disk_type_uri)
  if create_boot_disk:
    boot_snapshot_uri = None
    if support_boot_snapshot_uri:
      boot_snapshot_uri = instance_utils.ResolveSnapshotURI(
          user_project=project,
          snapshot=args.source_snapshot,
          resource_parser=resource_parser)

    boot_disk = CreateDefaultBootAttachedDiskMessage(
        compute_client=compute_client,
        resources=resource_parser,
        disk_type=args.boot_disk_type,
        disk_device_name=args.boot_disk_device_name,
        disk_auto_delete=args.boot_disk_auto_delete,
        disk_size_gb=boot_disk_size_gb,
        require_csek_key_create=(args.require_csek_key_create
                                 if csek_keys else None),
        image_uri=image_uri,
        instance_name=instance_name,
        project=project,
        location=location,
        scope=scope,
        enable_kms=support_kms,
        csek_keys=csek_keys,
        kms_args=args,
        snapshot_uri=boot_snapshot_uri)
    persistent_disks = [boot_disk] + persistent_disks

  return persistent_disks + persistent_create_disks + local_nvdimms + local_ssds


def CreatePersistentAttachedDiskMessages(resources,
                                         compute_client,
                                         csek_keys,
                                         disks,
                                         project,
                                         location,
                                         scope,
                                         container_mount_disk=None):
  """Returns a list of AttachedDisk messages and the boot disk's reference."""
  disks_messages = []

  messages = compute_client.messages
  compute = compute_client.apitools_client
  for disk in disks:
    name = disk.get('name')

    # Resolves the mode.
    mode_value = disk.get('mode', 'rw')
    if mode_value == 'rw':
      mode = messages.AttachedDisk.ModeValueValuesEnum.READ_WRITE
    else:
      mode = messages.AttachedDisk.ModeValueValuesEnum.READ_ONLY

    boot = disk.get('boot') == 'yes'
    auto_delete = disk.get('auto-delete') == 'yes'

    if 'scope' in disk and disk['scope'] == 'regional':
      scope = compute_scopes.ScopeEnum.REGION
    else:
      scope = compute_scopes.ScopeEnum.ZONE
    disk_ref = instance_utils.ParseDiskResource(resources, name, project,
                                                location, scope)

    # TODO(b/36051031) drop test after CSEK goes GA
    if csek_keys:
      disk_key_or_none = csek_utils.MaybeLookupKeyMessage(
          csek_keys, disk_ref, compute)
      kwargs = {'diskEncryptionKey': disk_key_or_none}
    else:
      kwargs = {}

    device_name = instance_utils.GetDiskDeviceName(disk, name,
                                                   container_mount_disk)

    attached_disk = messages.AttachedDisk(
        autoDelete=auto_delete,
        boot=boot,
        deviceName=device_name,
        mode=mode,
        source=disk_ref.SelfLink(),
        type=messages.AttachedDisk.TypeValueValuesEnum.PERSISTENT,
        **kwargs)

    # The boot disk must end up at index 0.
    if boot:
      disks_messages = [attached_disk] + disks_messages
    else:
      disks_messages.append(attached_disk)

  return disks_messages


def CreatePersistentCreateDiskMessages(compute_client,
                                       resources,
                                       csek_keys,
                                       create_disks,
                                       project,
                                       location,
                                       scope,
                                       holder,
                                       enable_kms=False,
                                       enable_snapshots=False,
                                       container_mount_disk=None,
                                       resource_policy=False,
                                       enable_source_snapshot_csek=False,
                                       enable_image_csek=False,
                                       support_replica_zones=False,
                                       use_disk_type_uri=True):
  """Returns a list of AttachedDisk messages for newly creating disks.

  Args:
    compute_client: creates resources,
    resources: parser of resources,
    csek_keys: customer suplied encryption keys,
    create_disks: disk objects - contains following properties * name - the name
      of disk, * description - an optional description for the disk, * mode -
      'rw' (R/W), 'ro' (R/O) access mode, * disk-size - the size of the disk, *
      disk-type - the type of the disk (HDD or SSD), * image - the name of the
      image to initialize from, * image-csek-required - the name of the CSK
      protected image, * image-family - the image family name, * image-project -
      the project name that has the image, * auto-delete - whether disks is
      deleted when VM is deleted, * device-name - device name on VM, *
      source-snapshot - the snapshot to initialize from, *
      source-snapshot-csek-required - CSK protected snapshot, *
      disk-resource-policy - resource policies applied to disk. *
      enable_source_snapshot_csek - CSK file for snapshot, * enable_image_csek -
      CSK file for image
    project: Project of instance that will own the new disks.
    location: Location of the instance that will own the new disks.
    scope: Location type of the instance that will own the new disks.
    holder: Convenience class to hold lazy initialized client and resources.
    enable_kms: True if KMS keys are supported for the disk.
    enable_snapshots: True if snapshot initialization is supported for the disk.
    container_mount_disk: list of disks to be mounted to container, if any.
    resource_policy: True if resource-policies are enabled
    enable_source_snapshot_csek: True if snapshot CSK files are enabled
    enable_image_csek: True if image CSK files are enabled
    support_replica_zones: True if we allow creation of regional disks
    use_disk_type_uri: True to use disk type URI, False if naked type.

  Returns:
    list of API messages for attached disks
  """
  disks_messages = []

  messages = compute_client.messages
  compute = compute_client.apitools_client
  for disk in create_disks or []:
    name = disk.get('name')

    # Resolves the mode.
    mode_value = disk.get('mode', 'rw')
    if mode_value == 'rw':
      mode = messages.AttachedDisk.ModeValueValuesEnum.READ_WRITE
    else:
      mode = messages.AttachedDisk.ModeValueValuesEnum.READ_ONLY

    auto_delete_value = disk.get('auto-delete', 'yes')
    auto_delete = auto_delete_value == 'yes'

    disk_size_gb = utils.BytesToGb(disk.get('size'))
    disk_type = disk.get('type')
    if disk_type:
      if use_disk_type_uri:
        disk_type_ref = instance_utils.ParseDiskType(resources, disk_type,
                                                     project, location, scope)
        disk_type = disk_type_ref.SelfLink()
    else:
      disk_type = None

    img = disk.get('image')
    img_family = disk.get('image-family')
    img_project = disk.get('image-project')

    image_uri = None
    if img or img_family:
      image_expander = image_utils.ImageExpander(compute_client, resources)
      image_uri, _ = image_expander.ExpandImageFlag(
          user_project=project,
          image=img,
          image_family=img_family,
          image_project=img_project,
          return_image_resource=False)

    image_key = None
    disk_key = None
    if csek_keys:
      image_key = csek_utils.MaybeLookupKeyMessagesByUri(
          csek_keys, resources, [image_uri], compute)
      if name:
        disk_ref = resources.Parse(
            name, collection='compute.disks', params={'zone': location})
        disk_key = csek_utils.MaybeLookupKeyMessage(csek_keys, disk_ref,
                                                    compute)

    if enable_kms:
      disk_key = kms_utils.MaybeGetKmsKeyFromDict(disk, messages, disk_key)

    initialize_params = messages.AttachedDiskInitializeParams(
        diskName=name,
        description=disk.get('description'),
        sourceImage=image_uri,
        diskSizeGb=disk_size_gb,
        diskType=disk_type,
        sourceImageEncryptionKey=image_key)

    replica_zones = disk.get('replica-zones')
    if support_replica_zones and replica_zones:
      normalized_zones = []
      for zone in replica_zones:
        zone_ref = holder.resources.Parse(
            zone, collection='compute.zones', params={'project': project})
        normalized_zones.append(zone_ref.SelfLink())
      initialize_params.replicaZones = normalized_zones

    if enable_snapshots:
      snapshot_name = disk.get('source-snapshot')
      attached_snapshot_uri = instance_utils.ResolveSnapshotURI(
          snapshot=snapshot_name,
          user_project=project,
          resource_parser=resources)
      if attached_snapshot_uri:
        initialize_params.sourceImage = None
        initialize_params.sourceSnapshot = attached_snapshot_uri

    if resource_policy:
      policies = disk.get('disk-resource-policy')
      if policies:
        initialize_params.resourcePolicies = policies

    if enable_image_csek:
      image_key_file = disk.get('image_csek')
      if image_key_file:
        initialize_params.imageKeyFile = image_key_file

    if enable_source_snapshot_csek:
      snapshot_key_file = disk.get('source_snapshot_csek')
      if snapshot_key_file:
        initialize_params.snapshotKeyFile = snapshot_key_file
    boot = disk.get('boot') == 'yes'
    multi_writer = disk.get('multi-writer') == 'yes'
    if multi_writer:
      initialize_params.multiWriter = True
    device_name = instance_utils.GetDiskDeviceName(disk, name,
                                                   container_mount_disk)
    create_disk = messages.AttachedDisk(
        autoDelete=auto_delete,
        boot=boot,
        deviceName=device_name,
        initializeParams=initialize_params,
        mode=mode,
        type=messages.AttachedDisk.TypeValueValuesEnum.PERSISTENT,
        diskEncryptionKey=disk_key)
    disks_messages.append(create_disk)

  return disks_messages


def CreateDefaultBootAttachedDiskMessage(compute_client,
                                         resources,
                                         disk_type,
                                         disk_device_name,
                                         disk_auto_delete,
                                         disk_size_gb,
                                         require_csek_key_create,
                                         image_uri,
                                         instance_name,
                                         project,
                                         location,
                                         scope,
                                         csek_keys=None,
                                         kms_args=None,
                                         enable_kms=False,
                                         snapshot_uri=None,
                                         use_disk_type_uri=True):
  """Returns an AttachedDisk message for creating a new boot disk."""
  messages = compute_client.messages
  compute = compute_client.apitools_client

  if disk_type and use_disk_type_uri:
    disk_type_ref = instance_utils.ParseDiskType(resources, disk_type, project,
                                                 location, scope)
    disk_type_uri = disk_type_ref.SelfLink()
  else:
    disk_type_uri = None

  if csek_keys:
    # If we're going to encrypt the boot disk make sure that we select
    # a name predictably, instead of letting the API deal with name
    # conflicts automatically.
    #
    # Note that when csek keys are being used we *always* want force this
    # even if we don't have any encryption key for default disk name.
    #
    # Consider the case where the user's key file has a key for disk `foo-1`
    # and no other disk.  Assume she runs
    #   gcloud compute instances create foo --csek-key-file f \
    #       --no-require-csek-key-create
    # and gcloud doesn't force the disk name to be `foo`.  The API might
    # select name `foo-1` for the new disk, but has no way of knowing
    # that the user has a key file mapping for that disk name.  That
    # behavior violates the principle of least surprise.
    #
    # Instead it's better for gcloud to force a specific disk name in the
    # instance create, and fail if that name isn't available.

    effective_boot_disk_name = (disk_device_name or instance_name)

    disk_ref = resources.Parse(
        effective_boot_disk_name,
        collection='compute.disks',
        params={
            'project': project,
            'zone': location
        })
    disk_key_or_none = csek_utils.MaybeToMessage(
        csek_keys.LookupKey(disk_ref, require_csek_key_create), compute)
    [image_key_or_none
    ] = csek_utils.MaybeLookupKeyMessagesByUri(csek_keys, resources,
                                               [image_uri], compute)
    kwargs_init_parms = {'sourceImageEncryptionKey': image_key_or_none}
    kwargs_disk = {'diskEncryptionKey': disk_key_or_none}
  else:
    kwargs_disk = {}
    kwargs_init_parms = {}
    effective_boot_disk_name = disk_device_name

  if enable_kms:
    kms_key = kms_utils.MaybeGetKmsKey(
        kms_args,
        messages,
        kwargs_disk.get('diskEncryptionKey', None),
        boot_disk_prefix=True)
    if kms_key:
      kwargs_disk = {'diskEncryptionKey': kms_key}

  initialize_params = messages.AttachedDiskInitializeParams(
      sourceImage=image_uri,
      diskSizeGb=disk_size_gb,
      diskType=disk_type_uri,
      **kwargs_init_parms)

  if snapshot_uri:
    initialize_params.sourceImage = None
    initialize_params.sourceSnapshot = snapshot_uri

  return messages.AttachedDisk(
      autoDelete=disk_auto_delete,
      boot=True,
      deviceName=effective_boot_disk_name,
      initializeParams=initialize_params,
      mode=messages.AttachedDisk.ModeValueValuesEnum.READ_WRITE,
      type=messages.AttachedDisk.TypeValueValuesEnum.PERSISTENT,
      **kwargs_disk)


# TODO(b/116515070) Replace `aep-nvdimm` with `local-nvdimm`
NVDIMM_DISK_TYPE = 'aep-nvdimm'


def CreateLocalNvdimmMessages(args,
                              resources,
                              messages,
                              location=None,
                              scope=None,
                              project=None):
  """Create messages representing local NVDIMMs."""
  local_nvdimms = []
  for local_nvdimm_disk in getattr(args, 'local_nvdimm', []) or []:
    local_nvdimm = _CreateLocalNvdimmMessage(resources, messages,
                                             local_nvdimm_disk.get('size'),
                                             location, scope, project)
    local_nvdimms.append(local_nvdimm)
  return local_nvdimms


def _CreateLocalNvdimmMessage(resources,
                              messages,
                              size_bytes=None,
                              location=None,
                              scope=None,
                              project=None):
  """Create a message representing a local NVDIMM."""

  if location:
    disk_type_ref = instance_utils.ParseDiskType(resources, NVDIMM_DISK_TYPE,
                                                 project, location, scope)
    disk_type = disk_type_ref.SelfLink()
  else:
    disk_type = NVDIMM_DISK_TYPE

  local_nvdimm = messages.AttachedDisk(
      type=messages.AttachedDisk.TypeValueValuesEnum.SCRATCH,
      autoDelete=True,
      interface=messages.AttachedDisk.InterfaceValueValuesEnum.NVDIMM,
      mode=messages.AttachedDisk.ModeValueValuesEnum.READ_WRITE,
      initializeParams=messages.AttachedDiskInitializeParams(
          diskType=disk_type),
  )

  if size_bytes is not None:
    local_nvdimm.diskSizeGb = utils.BytesToGb(size_bytes)

  return local_nvdimm


def CreateLocalSsdMessages(args,
                           resources,
                           messages,
                           location=None,
                           scope=None,
                           project=None,
                           use_disk_type_uri=True):
  """Create messages representing local ssds."""
  local_ssds = []
  for local_ssd_disk in getattr(args, 'local_ssd', []) or []:
    local_ssd = _CreateLocalSsdMessage(resources, messages,
                                       local_ssd_disk.get('device-name'),
                                       local_ssd_disk.get('interface'),
                                       local_ssd_disk.get('size'), location,
                                       scope, project, use_disk_type_uri)
    local_ssds.append(local_ssd)
  return local_ssds


def _CreateLocalSsdMessage(resources,
                           messages,
                           device_name,
                           interface,
                           size_bytes=None,
                           location=None,
                           scope=None,
                           project=None,
                           use_disk_type_uri=True):
  """Create a message representing a local ssd."""

  if location and use_disk_type_uri:
    disk_type_ref = instance_utils.ParseDiskType(resources, 'local-ssd',
                                                 project, location, scope)
    disk_type = disk_type_ref.SelfLink()
  else:
    disk_type = 'local-ssd'

  maybe_interface_enum = (
      messages.AttachedDisk.InterfaceValueValuesEnum(interface)
      if interface else None)

  local_ssd = messages.AttachedDisk(
      type=messages.AttachedDisk.TypeValueValuesEnum.SCRATCH,
      autoDelete=True,
      deviceName=device_name,
      interface=maybe_interface_enum,
      mode=messages.AttachedDisk.ModeValueValuesEnum.READ_WRITE,
      initializeParams=messages.AttachedDiskInitializeParams(
          diskType=disk_type),
  )

  if size_bytes is not None:
    local_ssd.diskSizeGb = utils.BytesToGb(size_bytes)

  return local_ssd


def GetBulkNetworkInterfaces(args, resource_parser, compute_client, holder,
                             project, location, scope, skip_defaults):
  if (skip_defaults and not instance_utils.IsAnySpecified(
      args, 'network_interface', 'network', 'network_tier', 'subnet',
      'no_address')):
    return []
  elif args.network_interface:
    return CreateNetworkInterfaceMessages(
        resources=resource_parser,
        compute_client=compute_client,
        network_interface_arg=args.network_interface,
        project=project,
        location=location,
        scope=scope)
  else:
    return [
        CreateNetworkInterfaceMessage(
            resources=holder.resources,
            compute_client=compute_client,
            network=args.network,
            subnet=args.subnet,
            no_address=args.no_address,
            project=project,
            location=location,
            scope=scope,
            network_tier=getattr(args, 'network_tier', None),
        )
    ]


def GetNetworkInterfaces(args, client, holder, project, location, scope,
                         skip_defaults):
  """Get network interfaces."""
  if (skip_defaults and not args.IsSpecified('network') and
      not instance_utils.IsAnySpecified(
          args,
          'address',
          'network_tier',
          'no_address',
          'no_public_ptr',
          'no_public_ptr_domain',
          'private_network_ip',
          'public_ptr',
          'public_ptr_domain',
          'subnet',
      )):
    return []
  return [
      CreateNetworkInterfaceMessage(
          resources=holder.resources,
          compute_client=client,
          network=args.network,
          subnet=args.subnet,
          no_address=args.no_address,
          address=args.address,
          project=project,
          location=location,
          scope=scope,
          no_public_ptr=args.no_public_ptr,
          public_ptr=args.public_ptr,
          no_public_ptr_domain=args.no_public_ptr_domain,
          public_ptr_domain=args.public_ptr_domain,
          private_network_ip=getattr(args, 'private_network_ip', None),
          network_tier=getattr(args, 'network_tier', None),
      )
  ]


def GetNetworkInterfacesAlpha(args, client, holder, project, location, scope,
                              skip_defaults):
  if (skip_defaults and not instance_utils.IsAnySpecified(
      args, 'network', 'subnet', 'private_network_ip', 'no_address', 'address',
      'network_tier', 'no_public_dns', 'public_dns', 'no_public_ptr',
      'public_ptr', 'no_public_ptr_domain', 'public_ptr_domain')):
    return []
  return [
      CreateNetworkInterfaceMessage(
          resources=holder.resources,
          compute_client=client,
          network=args.network,
          subnet=args.subnet,
          no_address=args.no_address,
          address=args.address,
          project=project,
          location=location,
          scope=scope,
          private_network_ip=getattr(args, 'private_network_ip', None),
          network_tier=getattr(args, 'network_tier', None),
          no_public_dns=getattr(args, 'no_public_dns', None),
          public_dns=getattr(args, 'public_dns', None),
          no_public_ptr=getattr(args, 'no_public_ptr', None),
          public_ptr=getattr(args, 'public_ptr', None),
          no_public_ptr_domain=getattr(args, 'no_public_ptr_domain', None),
          public_ptr_domain=getattr(args, 'public_ptr_domain', None))
  ]


def CreateNetworkInterfaceMessage(resources,
                                  compute_client,
                                  network,
                                  subnet,
                                  project,
                                  location,
                                  scope,
                                  nic_type=None,
                                  no_address=None,
                                  address=None,
                                  private_network_ip=None,
                                  alias_ip_ranges_string=None,
                                  network_tier=None,
                                  no_public_dns=None,
                                  public_dns=None,
                                  no_public_ptr=None,
                                  public_ptr=None,
                                  no_public_ptr_domain=None,
                                  public_ptr_domain=None):
  """Returns a new NetworkInterface message."""
  # TODO(b/30460572): instance reference should have zone name, not zone URI.
  if scope == compute_scopes.ScopeEnum.ZONE:
    region = utils.ZoneNameToRegionName(location.split('/')[-1])
  elif scope == compute_scopes.ScopeEnum.REGION:
    region = location
  messages = compute_client.messages
  network_interface = messages.NetworkInterface()
  # By default interface is attached to default network. If network or subnet
  # are specified they're used instead.
  if subnet is not None:
    subnet_ref = resources.Parse(
        subnet,
        collection='compute.subnetworks',
        params={
            'project': project,
            'region': region
        })
    network_interface.subnetwork = subnet_ref.SelfLink()
  if network is not None:
    network_ref = resources.Parse(
        network, params={
            'project': project,
        }, collection='compute.networks')
    network_interface.network = network_ref.SelfLink()
  elif subnet is None:
    network_ref = resources.Parse(
        constants.DEFAULT_NETWORK,
        params={'project': project},
        collection='compute.networks')
    network_interface.network = network_ref.SelfLink()

  if private_network_ip is not None:
    # Try interpreting the address as IPv4 or IPv6.
    try:
      # ipaddress only allows unicode input
      ipaddress.ip_address(six.text_type(private_network_ip))
      network_interface.networkIP = private_network_ip
    except ValueError:
      # ipaddress could not resolve as an IPv4 or IPv6 address.
      network_interface.networkIP = instances_flags.GetAddressRef(
          resources, private_network_ip, region).SelfLink()

  if nic_type is not None:
    network_interface.nicType = messages.NetworkInterface.NicTypeValueValuesEnum(
        nic_type)

  if alias_ip_ranges_string:
    network_interface.aliasIpRanges = (
        alias_ip_range_utils.CreateAliasIpRangeMessagesFromString(
            messages, True, alias_ip_ranges_string))

  if not no_address:
    access_config = messages.AccessConfig(
        name=constants.DEFAULT_ACCESS_CONFIG_NAME,
        type=messages.AccessConfig.TypeValueValuesEnum.ONE_TO_ONE_NAT)
    if network_tier is not None:
      access_config.networkTier = (
          messages.AccessConfig.NetworkTierValueValuesEnum(network_tier))

    # If the user provided an external IP, populate the access
    # config with it.
    address_resource = instances_flags.ExpandAddressFlag(
        resources, compute_client, address, region)
    if address_resource:
      access_config.natIP = address_resource

    if no_public_dns:
      access_config.setPublicDns = False
    elif public_dns:
      access_config.setPublicDns = True

    if no_public_ptr:
      access_config.setPublicPtr = False
    elif public_ptr:
      access_config.setPublicPtr = True

    if not no_public_ptr_domain and public_ptr_domain is not None:
      access_config.publicPtrDomainName = public_ptr_domain

    network_interface.accessConfigs = [access_config]

  return network_interface


def CreateNetworkInterfaceMessages(resources, compute_client,
                                   network_interface_arg, project, location,
                                   scope):
  """Create network interface messages.

  Args:
    resources: generates resource references.
    compute_client: creates resources.
    network_interface_arg: CLI argument specyfying network interfaces.
    project: project of the instance that will own the generated network
      interfaces.
    zone: zone of the instance that will own the generated network interfaces.

  Returns:
    list, items are NetworkInterfaceMessages.
  """
  result = []
  if network_interface_arg:
    for interface in network_interface_arg:
      address = interface.get('address', None)
      no_address = 'no-address' in interface
      network_tier = interface.get('network-tier', None)

      result.append(
          CreateNetworkInterfaceMessage(
              resources=resources,
              compute_client=compute_client,
              network=interface.get('network', None),
              subnet=interface.get('subnet', None),
              private_network_ip=interface.get('private-network-ip', None),
              nic_type=interface.get('nic-type', None),
              no_address=no_address,
              address=address,
              project=project,
              location=location,
              scope=scope,
              alias_ip_ranges_string=interface.get('aliases', None),
              network_tier=network_tier))
  return result


def GetNetworkInterfacesWithValidation(args,
                                       resource_parser,
                                       compute_client,
                                       holder,
                                       project,
                                       location,
                                       scope,
                                       skip_defaults,
                                       support_public_dns=False):
  """Validates and retrieves the network interface message."""
  if args.network_interface:
    return CreateNetworkInterfaceMessages(
        resources=resource_parser,
        compute_client=compute_client,
        network_interface_arg=args.network_interface,
        project=project,
        location=location,
        scope=scope)
  else:
    instances_flags.ValidatePublicPtrFlags(args)
    if support_public_dns:
      instances_flags.ValidatePublicDnsFlags(args)
      return GetNetworkInterfacesAlpha(args, compute_client, holder, project,
                                       location, scope, skip_defaults)
    return GetNetworkInterfaces(args, compute_client, holder, project, location,
                                scope, skip_defaults)


def GetProjectToServiceAccountMap(args, instance_refs, client, skip_defaults):
  """Creates a mapping of projects to service accounts."""
  project_to_sa = {}
  for instance_ref in instance_refs:
    if instance_ref.project not in project_to_sa:
      project_to_sa[instance_ref.project] = GetProjectServiceAccount(
          args=args,
          project=instance_ref.project,
          client=client,
          skip_defaults=skip_defaults,
          instance_name=instance_ref.Name())
  return project_to_sa


def GetProjectServiceAccount(args,
                             project,
                             client,
                             skip_defaults,
                             instance_name=None):
  """Retrieves service accounts for the specified project."""
  scopes = None
  if not args.no_scopes and not args.scopes:
    # User didn't provide any input on scopes. If project has no default
    # service account then we want to create a VM with no scopes
    request = (client.apitools_client.projects, 'Get',
               client.messages.ComputeProjectsGetRequest(project=project))
    errors = []
    result = client.MakeRequests([request], errors)
    if not errors:
      if not result[0].defaultServiceAccount:
        scopes = []
        scope_warning = 'There is no default service account for project {}.'.format(
            project)
        if instance_name:
          scope_warning += ' Instance {} will not have scopes.'.format(
              instance_name)
        log.status.Print(scope_warning)
  if scopes is None:
    scopes = [] if args.no_scopes else args.scopes

  if args.no_service_account:
    service_account = None
  else:
    service_account = args.service_account
  if (skip_defaults and not args.IsSpecified('scopes') and
      not args.IsSpecified('no_scopes') and
      not args.IsSpecified('service_account') and
      not args.IsSpecified('no_service_account')):
    service_accounts = []
  else:
    service_accounts = instance_utils.CreateServiceAccountMessages(
        messages=client.messages,
        scopes=scopes,
        service_account=service_account)
  return service_accounts


def BuildShieldedInstanceConfigMessage(messages, args):
  """Builds a shielded instance configuration message."""
  if (args.IsSpecified('shielded_vm_secure_boot') or
      args.IsSpecified('shielded_vm_vtpm') or
      args.IsSpecified('shielded_vm_integrity_monitoring')):
    return instance_utils.CreateShieldedInstanceConfigMessage(
        messages, args.shielded_vm_secure_boot, args.shielded_vm_vtpm,
        args.shielded_vm_integrity_monitoring)
  else:
    return None


def BuildConfidentialInstanceConfigMessage(messages, args):
  """Builds a confidential instance configuration message."""
  if args.IsSpecified('confidential_compute'):
    return instance_utils.CreateConfidentialInstanceMessage(
        messages, args.confidential_compute)
  else:
    return None


def GetImageUri(args,
                client,
                create_boot_disk,
                project,
                resource_parser,
                confidential_vm=False):
  """Retrieves the image uri for the specified image."""
  if create_boot_disk:
    image_expander = image_utils.ImageExpander(client, resource_parser)
    image_uri, _ = image_expander.ExpandImageFlag(
        user_project=project,
        image=args.image,
        image_family=args.image_family,
        image_project=args.image_project,
        return_image_resource=False,
        confidential_vm=confidential_vm)
    return image_uri


def GetAccelerators(args, compute_client, resource_parser, project, location,
                    scope):
  """Returns list of messages with accelerators for the instance."""
  if args.accelerator:
    accelerator_type_name = args.accelerator['type']
    accelerator_type = instance_utils.ParseAcceleratorType(
        accelerator_type_name, resource_parser, project, location, scope)
    # Accelerator count is default to 1.
    accelerator_count = int(args.accelerator.get('count', 1))
    return CreateAcceleratorConfigMessages(compute_client.messages,
                                           accelerator_type, accelerator_count)
  return []


def GetAcceleratorsForInstanceProperties(args, compute_client):
  if args.accelerator:
    accelerator_type = args.accelerator['type']
    accelerator_count = int(args.accelerator.get('count', 1))
    return CreateAcceleratorConfigMessages(compute_client.messages,
                                           accelerator_type, accelerator_count)
  return []


def CreateAcceleratorConfigMessages(msgs, accelerator_type, accelerator_count):
  """Returns a list of accelerator config messages.

  Args:
    msgs: tracked GCE API messages.
    accelerator_type: reference to the accelerator type.
    accelerator_count: number of accelerators to attach to the VM.

  Returns:
    a list of accelerator config message that specifies the type and number of
    accelerators to attach to an instance.
  """

  accelerator_config = msgs.AcceleratorConfig(
      acceleratorType=accelerator_type, acceleratorCount=accelerator_count)
  return [accelerator_config]


def CreateMachineTypeUri(args,
                         compute_client,
                         resource_parser,
                         project,
                         location,
                         scope,
                         confidential_vm=False):
  """Create a machine type URI for given args and instance reference."""

  machine_type = args.machine_type
  custom_cpu = args.custom_cpu
  custom_memory = args.custom_memory
  vm_type = getattr(args, 'custom_vm_type', None)
  ext = getattr(args, 'custom_extensions', None)

  # Setting the machine type
  machine_type_name = instance_utils.InterpretMachineType(
      machine_type=machine_type,
      custom_cpu=custom_cpu,
      custom_memory=custom_memory,
      ext=ext,
      vm_type=vm_type,
      confidential_vm=confidential_vm)

  # Check to see if the custom machine type ratio is supported
  instance_utils.CheckCustomCpuRamRatio(compute_client, project, location,
                                        machine_type_name)

  machine_type_uri = instance_utils.ParseMachineType(resource_parser,
                                                     machine_type_name, project,
                                                     location, scope)
  return machine_type_uri
