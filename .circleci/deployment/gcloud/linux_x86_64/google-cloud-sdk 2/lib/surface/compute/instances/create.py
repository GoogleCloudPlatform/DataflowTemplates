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
"""Command for creating instances."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import re

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import base_classes_resource_registry as resource_registry
from googlecloudsdk.api_lib.compute import csek_utils
from googlecloudsdk.api_lib.compute import instance_utils
from googlecloudsdk.api_lib.compute import metadata_utils
from googlecloudsdk.api_lib.compute import utils
from googlecloudsdk.api_lib.compute.instances.create import utils as create_utils
from googlecloudsdk.api_lib.compute.operations import poller
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.compute import completers
from googlecloudsdk.command_lib.compute import flags
from googlecloudsdk.command_lib.compute import scope as compute_scopes
from googlecloudsdk.command_lib.compute.instances import flags as instances_flags
from googlecloudsdk.command_lib.compute.resource_policies import flags as maintenance_flags
from googlecloudsdk.command_lib.compute.resource_policies import util as maintenance_util
from googlecloudsdk.command_lib.compute.sole_tenancy import flags as sole_tenancy_flags
from googlecloudsdk.command_lib.util.apis import arg_utils
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.core import exceptions as core_exceptions
from googlecloudsdk.core import log
import six
from six.moves import zip

DETAILED_HELP = {
    'DESCRIPTION':
        """
        *{command}* facilitates the creation of Compute Engine
        virtual machines.

        When an instance is in RUNNING state and the system begins to boot,
        the instance creation is considered finished, and the command returns
        with a list of new virtual machines.  Note that you usually cannot log
        into a new instance until it finishes booting. Check the progress of an
        instance using `gcloud compute instances get-serial-port-output`.

        For more examples, refer to the *EXAMPLES* section below.
        """,
    'EXAMPLES':
        """
        To create an instance with the latest 'Red Hat Enterprise Linux 8' image
        available, run:

          $ {command} example-instance --image-family=rhel-8 --image-project=rhel-cloud --zone=us-central1-a

        To create instances called 'example-instance-1', 'example-instance-2',
        and 'example-instance-3' in the 'us-central1-a' zone, run:

          $ {command} example-instance-1 example-instance-2 example-instance-3 --zone=us-central1-a

        To create an instance called 'instance-1' from a source snapshot called
        'instance-snapshot' in zone 'us-central2-a' and attached regional disk
        'disk-1', run:

          $ {command} instance-1 --source-snapshot=https://compute.googleapis.com/compute/v1/projects/myproject/global/snapshots/instance-snapshot --zone=central2-a --disk=name=disk1,scope=regional

        To create an instance called instance-1 as a shielded vm with
        secure boot, virtual trusted platform module (vTPM) enabled and
        integrity monitoring, run:

          $ {command} instance-1 --zone=central2-a --shielded-secure-boot --shielded-vtpm --shielded-integrity-monitoring

        To create an preemptible instance called 'instance-1', run:

          $ {command} instance-1 --machine-type=n1-standard-1 --zone=us-central1-b --preemptible --no-restart-on-failure --maintenance-policy=terminate

        """,
}


def _CommonArgs(parser,
                enable_regional=False,
                enable_kms=False,
                deprecate_maintenance_policy=False,
                enable_resource_policy=False,
                supports_location_hint=False,
                support_network_interface_nic_type=False,
                supports_erase_vss=False,
                snapshot_csek=False,
                image_csek=False,
                support_multi_writer=True,
                support_replica_zones=False):
  """Register parser args common to all tracks."""
  metadata_utils.AddMetadataArgs(parser)
  instances_flags.AddDiskArgs(parser, enable_regional, enable_kms=enable_kms)
  instances_flags.AddCreateDiskArgs(
      parser,
      enable_kms=enable_kms,
      enable_snapshots=True,
      resource_policy=enable_resource_policy,
      source_snapshot_csek=snapshot_csek,
      image_csek=image_csek,
      support_boot=True,
      support_multi_writer=support_multi_writer,
      support_replica_zones=support_replica_zones)
  instances_flags.AddCanIpForwardArgs(parser)
  instances_flags.AddAddressArgs(
      parser,
      instances=True,
      support_network_interface_nic_type=support_network_interface_nic_type)
  instances_flags.AddAcceleratorArgs(parser)
  instances_flags.AddMachineTypeArgs(parser)
  instances_flags.AddMaintenancePolicyArgs(
      parser, deprecate=deprecate_maintenance_policy)
  instances_flags.AddNoRestartOnFailureArgs(parser)
  instances_flags.AddPreemptibleVmArgs(parser)
  instances_flags.AddServiceAccountAndScopeArgs(
      parser,
      False,
      extra_scopes_help='However, if neither `--scopes` nor `--no-scopes` are '
      'specified and the project has no default service '
      'account, then the instance will be created with no '
      'scopes. Note that the level of access that a service '
      'account has is determined by a combination of access '
      'scopes and IAM roles so you must configure both '
      'access scopes and IAM roles for the service account '
      'to work properly.')
  instances_flags.AddTagsArgs(parser)
  instances_flags.AddCustomMachineTypeArgs(parser)
  instances_flags.AddNetworkArgs(parser)
  instances_flags.AddPrivateNetworkIpArgs(parser)
  instances_flags.AddHostnameArg(parser)
  instances_flags.AddImageArgs(parser, enable_snapshots=True)
  instances_flags.AddDeletionProtectionFlag(parser)
  instances_flags.AddPublicPtrArgs(parser, instance=True)
  instances_flags.AddNetworkTierArgs(parser, instance=True)
  instances_flags.AddShieldedInstanceConfigArgs(parser)
  instances_flags.AddDisplayDeviceArg(parser)
  instances_flags.AddMinNodeCpuArg(parser)

  instances_flags.AddReservationAffinityGroup(
      parser,
      group_text='Specifies the reservation for the instance.',
      affinity_text='The type of reservation for the instance.')

  maintenance_flags.AddResourcePoliciesArgs(parser, 'added to', 'instance')

  sole_tenancy_flags.AddNodeAffinityFlagToParser(parser)

  if supports_location_hint:
    instances_flags.AddLocationHintArg(parser)

  if supports_erase_vss:
    flags.AddEraseVssSignature(parser, 'source snapshots or source machine'
                               ' image')

  labels_util.AddCreateLabelsFlags(parser)

  parser.add_argument(
      '--description', help='Specifies a textual description of the instances.')

  instances_flags.INSTANCES_ARG_FOR_CREATE.AddArgument(
      parser, operation_type='create')

  csek_utils.AddCsekKeyArgs(parser)

  base.ASYNC_FLAG.AddToParser(parser)
  parser.display_info.AddFormat(
      resource_registry.RESOURCE_REGISTRY['compute.instances'].list_format)
  parser.display_info.AddCacheUpdater(completers.InstancesCompleter)


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Create(base.CreateCommand):
  """Create Compute Engine virtual machine instances."""

  _support_regional = True
  _support_kms = True
  _support_nvdimm = False
  _support_public_dns = False
  _support_disk_resource_policy = False
  _support_erase_vss = False
  _support_machine_image_key = False
  _support_location_hint = False
  _support_source_snapshot_csek = False
  _support_image_csek = False
  _support_post_key_revocation_action_type = False
  _support_rsa_encrypted = False
  _deprecate_maintenance_policy = False
  _support_create_disk_snapshots = True
  _support_boot_snapshot_uri = True
  _enable_pd_interface = False
  _support_enable_nested_virtualization = False
  _support_replica_zones = False
  _support_network_interface_nic_type = False

  @classmethod
  def Args(cls, parser):
    _CommonArgs(
        parser,
        enable_kms=cls._support_kms,
        support_multi_writer=False,
        support_replica_zones=cls._support_replica_zones,
        enable_regional=cls._support_regional)
    cls.SOURCE_INSTANCE_TEMPLATE = (
        instances_flags.MakeSourceInstanceTemplateArg())
    cls.SOURCE_INSTANCE_TEMPLATE.AddArgument(parser)
    instances_flags.AddLocalSsdArgs(parser)
    instances_flags.AddMinCpuPlatformArgs(parser, base.ReleaseTrack.GA)
    instances_flags.AddPrivateIpv6GoogleAccessArg(parser,
                                                  utils.COMPUTE_GA_API_VERSION)
    instances_flags.AddConfidentialComputeArgs(parser)

  def Collection(self):
    return 'compute.instances'

  def GetSourceInstanceTemplate(self, args, resources):
    """Get sourceInstanceTemplate value as required by API."""
    if not args.IsSpecified('source_instance_template'):
      return None
    ref = self.SOURCE_INSTANCE_TEMPLATE.ResolveAsResource(args, resources)
    return ref.SelfLink()

  def GetSourceMachineImage(self, args, resources):
    """Get sourceMachineImage value as required by API."""
    return None

  def _CreateRequests(self, args, instance_refs, project, zone, compute_client,
                      resource_parser, holder):
    # gcloud creates default values for some fields in Instance resource
    # when no value was specified on command line.
    # When --source-instance-template was specified, defaults are taken from
    # Instance Template and gcloud flags are used to override them - by default
    # fields should not be initialized.
    source_instance_template = self.GetSourceInstanceTemplate(
        args, resource_parser)
    skip_defaults = source_instance_template is not None

    source_machine_image = self.GetSourceMachineImage(args, resource_parser)
    skip_defaults = skip_defaults or source_machine_image is not None

    scheduling = instance_utils.GetScheduling(
        args,
        compute_client,
        skip_defaults,
        support_node_affinity=True,
        support_location_hint=self._support_location_hint)
    tags = instance_utils.GetTags(args, compute_client)
    labels = instance_utils.GetLabels(args, compute_client)
    metadata = instance_utils.GetMetadata(args, compute_client, skip_defaults)
    boot_disk_size_gb = instance_utils.GetBootDiskSizeGb(args)

    network_interfaces = create_utils.GetNetworkInterfacesWithValidation(
        args=args,
        resource_parser=resource_parser,
        compute_client=compute_client,
        holder=holder,
        project=project,
        location=zone,
        scope=compute_scopes.ScopeEnum.ZONE,
        skip_defaults=skip_defaults,
        support_public_dns=self._support_public_dns)

    confidential_vm = (
        args.IsSpecified('confidential_compute') and args.confidential_compute)

    create_boot_disk = not (
        instance_utils.UseExistingBootDisk((args.disk or []) +
                                           (args.create_disk or [])))
    image_uri = create_utils.GetImageUri(args, compute_client, create_boot_disk,
                                         project, resource_parser,
                                         confidential_vm)

    shielded_instance_config = create_utils.BuildShieldedInstanceConfigMessage(
        messages=compute_client.messages, args=args)

    confidential_instance_config = (
        create_utils.BuildConfidentialInstanceConfigMessage(
            messages=compute_client.messages, args=args))

    csek_keys = csek_utils.CsekKeyStore.FromArgs(args,
                                                 self._support_rsa_encrypted)

    project_to_sa = create_utils.GetProjectToServiceAccountMap(
        args, instance_refs, compute_client, skip_defaults)

    requests = []
    for instance_ref in instance_refs:

      disks = []
      if create_utils.CheckSpecifiedDiskArgs(
          args=args, skip_defaults=skip_defaults,
          support_kms=self._support_kms):
        disks = create_utils.CreateDiskMessages(
            args=args,
            instance_name=instance_ref.Name(),
            project=instance_ref.project,
            location=instance_ref.zone,
            scope=compute_scopes.ScopeEnum.ZONE,
            compute_client=compute_client,
            resource_parser=resource_parser,
            boot_disk_size_gb=boot_disk_size_gb,
            image_uri=image_uri,
            create_boot_disk=create_boot_disk,
            csek_keys=csek_keys,
            holder=holder,
            support_kms=self._support_kms,
            support_nvdimm=self._support_nvdimm,
            support_disk_resource_policy=self._support_disk_resource_policy,
            support_source_snapshot_csek=self._support_source_snapshot_csek,
            support_boot_snapshot_uri=self._support_boot_snapshot_uri,
            support_image_csek=self._support_image_csek,
            support_create_disk_snapshots=self._support_create_disk_snapshots,
            support_replica_zones=self._support_replica_zones)

      machine_type_uri = None
      if instance_utils.CheckSpecifiedMachineTypeArgs(args, skip_defaults):
        machine_type_uri = instance_utils.CreateMachineTypeUri(
            args=args,
            compute_client=compute_client,
            resource_parser=resource_parser,
            project=instance_ref.project,
            location=instance_ref.zone,
            scope=compute_scopes.ScopeEnum.ZONE,
            confidential_vm=confidential_vm)

      can_ip_forward = instance_utils.GetCanIpForward(args, skip_defaults)
      guest_accelerators = create_utils.GetAccelerators(
          args=args,
          compute_client=compute_client,
          resource_parser=resource_parser,
          project=instance_ref.project,
          location=instance_ref.zone,
          scope=compute_scopes.ScopeEnum.ZONE)

      instance = compute_client.messages.Instance(
          canIpForward=can_ip_forward,
          deletionProtection=args.deletion_protection,
          description=args.description,
          disks=disks,
          guestAccelerators=guest_accelerators,
          hostname=args.hostname,
          labels=labels,
          machineType=machine_type_uri,
          metadata=metadata,
          minCpuPlatform=args.min_cpu_platform,
          name=instance_ref.Name(),
          networkInterfaces=network_interfaces,
          serviceAccounts=project_to_sa[instance_ref.project],
          scheduling=scheduling,
          tags=tags)

      if args.private_ipv6_google_access_type is not None:
        instance.privateIpv6GoogleAccess = (
            instances_flags.GetPrivateIpv6GoogleAccessTypeFlagMapper(
                compute_client.messages).GetEnumForChoice(
                    args.private_ipv6_google_access_type))

      if (self._support_enable_nested_virtualization and
          args.enable_nested_virtualization is not None):
        instance.advancedMachineFeatures = (
            instance_utils.CreateAdvancedMachineFeaturesMessage(
                compute_client.messages,
                args.enable_nested_virtualization))

      resource_policies = getattr(args, 'resource_policies', None)
      if resource_policies:
        parsed_resource_policies = []
        for policy in resource_policies:
          resource_policy_ref = maintenance_util.ParseResourcePolicyWithZone(
              resource_parser,
              policy,
              project=instance_ref.project,
              zone=instance_ref.zone)
          parsed_resource_policies.append(resource_policy_ref.SelfLink())
        instance.resourcePolicies = parsed_resource_policies

      if shielded_instance_config:
        instance.shieldedInstanceConfig = shielded_instance_config

      if confidential_instance_config:
        instance.confidentialInstanceConfig = confidential_instance_config

      if self._support_erase_vss and \
        args.IsSpecified('erase_windows_vss_signature'):
        instance.eraseWindowsVssSignature = args.erase_windows_vss_signature

      if self._support_post_key_revocation_action_type and args.IsSpecified(
          'post_key_revocation_action_type'):
        instance.postKeyRevocationActionType = arg_utils.ChoiceToEnum(
            args.post_key_revocation_action_type, compute_client.messages
            .Instance.PostKeyRevocationActionTypeValueValuesEnum)

      request = compute_client.messages.ComputeInstancesInsertRequest(
          instance=instance,
          project=instance_ref.project,
          zone=instance_ref.zone)

      if source_instance_template:
        request.sourceInstanceTemplate = source_instance_template

      if source_machine_image:
        request.instance.sourceMachineImage = source_machine_image
        if args.IsSpecified('source_machine_image_csek_key_file'):
          key = instance_utils.GetSourceMachineImageKey(
              args, self.SOURCE_MACHINE_IMAGE, compute_client, holder)
          request.instance.sourceMachineImageEncryptionKey = key

      if self._support_machine_image_key and \
          args.IsSpecified('source_machine_image_csek_key_file'):
        if not args.IsSpecified('source_machine_image'):
          raise exceptions.RequiredArgumentException(
              '`--source-machine-image`',
              '`--source-machine-image-csek-key-file` requires '
              '`--source-machine-image` to be specified`')

      if args.IsSpecified('enable_display_device'):
        request.instance.displayDevice = compute_client.messages.DisplayDevice(
            enableDisplay=args.enable_display_device)

      request.instance.reservationAffinity = instance_utils.GetReservationAffinity(
          args, compute_client)

      requests.append(
          (compute_client.apitools_client.instances, 'Insert', request))
    return requests

  def Run(self, args):
    instances_flags.ValidateDiskFlags(
        args,
        enable_kms=self._support_kms,
        enable_snapshots=True,
        enable_source_snapshot_csek=self._support_source_snapshot_csek,
        enable_image_csek=self._support_image_csek)
    instances_flags.ValidateImageFlags(args)
    instances_flags.ValidateLocalSsdFlags(args)
    instances_flags.ValidateNicFlags(args)
    instances_flags.ValidateServiceAccountAndScopeArgs(args)
    instances_flags.ValidateAcceleratorArgs(args)
    instances_flags.ValidateNetworkTierArgs(args)
    instances_flags.ValidateReservationAffinityGroup(args)

    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    compute_client = holder.client
    resource_parser = holder.resources

    instance_refs = instance_utils.GetInstanceRefs(args, compute_client, holder)
    if len(instance_refs) > 1 and args.IsSpecified('address'):
      raise exceptions.BadArgumentException(
          '--address',
          'Multiple instances were specified for creation. --address flag can '
          'be used when creating a single instance.')

    requests = self._CreateRequests(args, instance_refs,
                                    instance_refs[0].project,
                                    instance_refs[0].zone, compute_client,
                                    resource_parser, holder)
    if not args.async_:
      # TODO(b/63664449): Replace this with poller + progress tracker.
      try:
        # Using legacy MakeRequests (which also does polling) here until
        # replaced by api_lib.utils.waiter.
        return compute_client.MakeRequests(requests)
      except exceptions.ToolException as e:
        invalid_machine_type_message_regex = (
            r'Invalid value for field \'resource.machineType\': .+. '
            r'Machine type with name \'.+\' does not exist in zone \'.+\'\.')
        if re.search(invalid_machine_type_message_regex, six.text_type(e)):
          raise exceptions.ToolException(
              six.text_type(e) +
              '\nUse `gcloud compute machine-types list --zones` to see the '
              'available machine  types.')
        raise

    errors_to_collect = []
    responses = compute_client.BatchRequests(requests, errors_to_collect)
    for r in responses:
      err = getattr(r, 'error', None)
      if err:
        errors_to_collect.append(poller.OperationErrors(err.errors))
    if errors_to_collect:
      raise core_exceptions.MultiError(errors_to_collect)

    operation_refs = [holder.resources.Parse(r.selfLink) for r in responses]

    log.status.Print('NOTE: The users will be charged for public IPs when VMs '
                     'are created.')

    for instance_ref, operation_ref in zip(instance_refs, operation_refs):
      log.status.Print('Instance creation in progress for [{}]: {}'.format(
          instance_ref.instance, operation_ref.SelfLink()))
    log.status.Print('Use [gcloud compute operations describe URI] command '
                     'to check the status of the operation(s).')
    if not args.IsSpecified('format'):
      # For async output we need a separate format. Since we already printed in
      # the status messages information about operations there is nothing else
      # needs to be printed.
      args.format = 'disable'
    return responses


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class CreateBeta(Create):
  """Create Compute Engine virtual machine instances."""

  _support_regional = True
  _support_kms = True
  _support_nvdimm = False
  _support_public_dns = False
  _support_disk_resource_policy = True
  _support_erase_vss = True
  _support_machine_image_key = True
  _support_location_hint = False
  _support_source_snapshot_csek = False
  _support_image_csek = False
  _support_post_key_revocation_action_type = False
  _support_rsa_encrypted = True
  _deprecate_maintenance_policy = False
  _support_create_disk_snapshots = True
  _support_boot_snapshot_uri = True
  _support_replica_zones = False
  _support_network_interface_nic_type = False

  def GetSourceMachineImage(self, args, resources):
    """Retrieves the specified source machine image's selflink.

    Args:
      args: The arguments passed into the gcloud command calling this function.
      resources: Resource parser used to retrieve the specified resource
        reference.

    Returns:
      A string containing the specified source machine image's selflink.
    """
    if not args.IsSpecified('source_machine_image'):
      return None
    ref = self.SOURCE_MACHINE_IMAGE.ResolveAsResource(args, resources)
    return ref.SelfLink()

  @classmethod
  def Args(cls, parser):
    _CommonArgs(
        parser,
        enable_regional=cls._support_regional,
        enable_kms=cls._support_kms,
        enable_resource_policy=cls._support_disk_resource_policy,
        supports_erase_vss=cls._support_erase_vss,
        support_replica_zones=cls._support_replica_zones)
    cls.SOURCE_INSTANCE_TEMPLATE = (
        instances_flags.MakeSourceInstanceTemplateArg())
    cls.SOURCE_INSTANCE_TEMPLATE.AddArgument(parser)
    cls.SOURCE_MACHINE_IMAGE = (instances_flags.AddMachineImageArg())
    cls.SOURCE_MACHINE_IMAGE.AddArgument(parser)
    instances_flags.AddSourceMachineImageEncryptionKey(parser)
    instances_flags.AddLocalSsdArgs(parser)
    instances_flags.AddMinCpuPlatformArgs(parser, base.ReleaseTrack.BETA)
    instances_flags.AddPrivateIpv6GoogleAccessArg(
        parser, utils.COMPUTE_BETA_API_VERSION)
    instances_flags.AddConfidentialComputeArgs(parser)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateAlpha(CreateBeta):
  """Create Compute Engine virtual machine instances."""

  _support_regional = True
  _support_kms = True
  _support_nvdimm = True
  _support_public_dns = True
  _support_disk_resource_policy = True
  _support_erase_vss = True
  _support_machine_image_key = True
  _support_location_hint = True
  _support_source_snapshot_csek = True
  _support_image_csek = True
  _support_post_key_revocation_action_type = True
  _support_rsa_encrypted = True
  _deprecate_maintenance_policy = True
  _support_create_disk_snapshots = True
  _support_boot_snapshot_uri = True
  _enable_pd_interface = True
  _support_enable_nested_virtualization = True
  _support_replica_zones = True
  _support_network_interface_nic_type = True

  @classmethod
  def Args(cls, parser):
    _CommonArgs(
        parser,
        enable_regional=cls._support_regional,
        enable_kms=cls._support_kms,
        deprecate_maintenance_policy=cls._deprecate_maintenance_policy,
        enable_resource_policy=cls._support_disk_resource_policy,
        supports_location_hint=cls._support_location_hint,
        support_network_interface_nic_type=cls
        ._support_network_interface_nic_type,
        supports_erase_vss=cls._support_erase_vss,
        snapshot_csek=cls._support_source_snapshot_csek,
        image_csek=cls._support_image_csek,
        support_replica_zones=cls._support_replica_zones)
    CreateAlpha.SOURCE_INSTANCE_TEMPLATE = (
        instances_flags.MakeSourceInstanceTemplateArg())
    CreateAlpha.SOURCE_INSTANCE_TEMPLATE.AddArgument(parser)
    CreateAlpha.SOURCE_MACHINE_IMAGE = (instances_flags.AddMachineImageArg())
    CreateAlpha.SOURCE_MACHINE_IMAGE.AddArgument(parser)
    instances_flags.AddSourceMachineImageEncryptionKey(parser)
    instances_flags.AddMinCpuPlatformArgs(parser, base.ReleaseTrack.ALPHA)
    instances_flags.AddPublicDnsArgs(parser, instance=True)
    instances_flags.AddLocalSsdArgsWithSize(parser)
    instances_flags.AddLocalNvdimmArgs(parser)
    instances_flags.AddConfidentialComputeArgs(parser)
    instances_flags.AddPostKeyRevocationActionTypeArgs(parser)
    instances_flags.AddPrivateIpv6GoogleAccessArg(
        parser, utils.COMPUTE_ALPHA_API_VERSION)
    instances_flags.AddStableFleetArgs(parser)
    instances_flags.AddNestedVirtualizationArgs(parser)


Create.detailed_help = DETAILED_HELP
