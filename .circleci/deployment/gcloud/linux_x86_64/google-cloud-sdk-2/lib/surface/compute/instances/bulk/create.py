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
"""Command for creating instances."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.api_lib.compute import instance_utils
from googlecloudsdk.api_lib.compute import metadata_utils
from googlecloudsdk.api_lib.compute.instances.create import utils as create_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.compute import flags
from googlecloudsdk.command_lib.compute import scope as compute_scopes
from googlecloudsdk.command_lib.compute.instances import flags as instances_flags
from googlecloudsdk.command_lib.compute.resource_policies import flags as maintenance_flags
from googlecloudsdk.command_lib.compute.resource_policies import util as maintenance_util
from googlecloudsdk.command_lib.util.apis import arg_utils
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.core import log
from googlecloudsdk.core import properties

DETAILED_HELP = {
    'brief':
        """
          Create multiple Compute Engine virtual machines.
        """,
    'DESCRIPTION':
        """
        *{command}* facilitates the creation of multiple Compute Engine
        virtual machines with a single command. They offer a number of advantages
        compared to the single instance creation command. This includes the
        ability to automatically pick a zone in which to create instances based
        on resource availability, the ability to specify that the request be
        atomic or best-effort, and a faster rate of instance creation.
        """,
    'EXAMPLES':
        """
        To create instances called 'example-instance-1', 'example-instance-2',
        and 'example-instance-3' in the 'us-central1-a' zone, run:

          $ {command} --predefined-names=example-instance-1,example-instance-2,example-instance-3 --zone=us-central1-a
        """,
}


def _CommonArgs(parser,
                deprecate_maintenance_policy=False,
                enable_resource_policy=False,
                supports_min_node_cpu=False,
                supports_location_hint=False,
                supports_erase_vss=False,
                snapshot_csek=False,
                image_csek=False):
  """Register parser args common to all tracks."""
  metadata_utils.AddMetadataArgs(parser)
  instances_flags.AddCreateDiskArgs(
      parser,
      enable_snapshots=True,
      resource_policy=enable_resource_policy,
      source_snapshot_csek=snapshot_csek,
      image_csek=image_csek,
      include_name=False)
  instances_flags.AddCanIpForwardArgs(parser)
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
  instances_flags.AddNoAddressArg(parser)
  instances_flags.AddNetworkArgs(parser)
  instances_flags.AddNetworkTierArgs(parser, instance=True)
  instances_flags.AddBulkCreateNetworkingArgs(parser)

  instances_flags.AddImageArgs(parser, enable_snapshots=True)
  instances_flags.AddShieldedInstanceConfigArgs(parser)
  instances_flags.AddDisplayDeviceArg(parser)

  instances_flags.AddReservationAffinityGroup(
      parser,
      group_text='Specifies the reservation for the instance.',
      affinity_text='The type of reservation for the instance.')

  maintenance_flags.AddResourcePoliciesArgs(parser, 'added to', 'instance')

  if supports_min_node_cpu:
    instances_flags.AddMinNodeCpuArg(parser)

  if supports_location_hint:
    instances_flags.AddLocationHintArg(parser)

  if supports_erase_vss:
    flags.AddEraseVssSignature(parser, 'source snapshots or source machine'
                               ' image')

  labels_util.AddCreateLabelsFlags(parser)

  parser.add_argument(
      '--description', help='Specifies a textual description of the instances.')

  base.ASYNC_FLAG.AddToParser(parser)


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateAlpha(base.Command):
  """Create Compute Engine virtual machine instances."""

  _support_nvdimm = False
  _support_public_dns = False
  _support_disk_resource_policy = True
  _support_erase_vss = True
  _support_min_node_cpu = True
  _support_location_hint = True
  _support_source_snapshot_csek = False
  _support_image_csek = True
  _support_confidential_compute = True
  _support_post_key_revocation_action_type = True
  _support_rsa_encrypted = True
  _deprecate_maintenance_policy = True
  _support_create_disk_snapshots = True
  _support_boot_snapshot_uri = True
  _support_enable_nested_virtualization = True

  _log_async = False

  @classmethod
  def Args(cls, parser):
    _CommonArgs(
        parser,
        deprecate_maintenance_policy=cls._deprecate_maintenance_policy,
        enable_resource_policy=cls._support_disk_resource_policy,
        supports_min_node_cpu=cls._support_min_node_cpu,
        supports_location_hint=cls._support_location_hint,
        supports_erase_vss=cls._support_erase_vss,
        snapshot_csek=cls._support_source_snapshot_csek,
        image_csek=cls._support_image_csek)
    CreateAlpha.SOURCE_INSTANCE_TEMPLATE = (
        instances_flags.MakeBulkSourceInstanceTemplateArg())
    CreateAlpha.SOURCE_INSTANCE_TEMPLATE.AddArgument(parser)
    instances_flags.AddMinCpuPlatformArgs(parser, base.ReleaseTrack.ALPHA)
    instances_flags.AddPublicDnsArgs(parser, instance=True)
    instances_flags.AddLocalSsdArgsWithSize(parser)
    instances_flags.AddConfidentialComputeArgs(parser)
    instances_flags.AddPostKeyRevocationActionTypeArgs(parser)
    instances_flags.AddBulkCreateArgs(parser)
    instances_flags.AddBootDiskArgs(parser)
    instances_flags.AddNestedVirtualizationArgs(parser)

  def Collection(self):
    return 'compute.instances'

  def GetSourceInstanceTemplate(self, args, resources):
    """Get sourceInstanceTemplate value as required by API."""
    if not args.IsSpecified('source_instance_template'):
      return None
    ref = self.SOURCE_INSTANCE_TEMPLATE.ResolveAsResource(args, resources)
    return ref.SelfLink()

  def _CreateRequests(self, args, holder, compute_client, resource_parser,
                      project, location, scope):
    # gcloud creates default values for some fields in Instance resource
    # when no value was specified on command line.
    # When --source-instance-template was specified, defaults are taken from
    # Instance Template and gcloud flags are used to override them - by default
    # fields should not be initialized.

    instance_names = args.predefined_names
    instance_count = args.count or len(instance_names)

    instance_min_count = instance_count
    if args.IsSpecified('min_count'):
      instance_min_count = args.min_count

    source_instance_template = self.GetSourceInstanceTemplate(
        args, resource_parser)
    skip_defaults = source_instance_template is not None

    scheduling = instance_utils.GetScheduling(
        args,
        compute_client,
        skip_defaults,
        support_node_affinity=False,
        support_min_node_cpu=self._support_min_node_cpu,
        support_location_hint=self._support_location_hint)
    tags = instance_utils.GetTags(args, compute_client)
    labels = instance_utils.GetLabels(
        args, compute_client, instance_properties=True)
    metadata = instance_utils.GetMetadata(args, compute_client, skip_defaults)

    network_interfaces = create_utils.GetBulkNetworkInterfaces(
        args=args,
        resource_parser=resource_parser,
        compute_client=compute_client,
        holder=holder,
        project=project,
        location=location,
        scope=scope,
        skip_defaults=skip_defaults)

    create_boot_disk = True
    image_uri = create_utils.GetImageUri(args, compute_client, create_boot_disk,
                                         project, resource_parser)

    shielded_instance_config = create_utils.BuildShieldedInstanceConfigMessage(
        messages=compute_client.messages, args=args)

    if self._support_confidential_compute:
      confidential_instance_config = (
          create_utils.BuildConfidentialInstanceConfigMessage(
              messages=compute_client.messages, args=args))

    service_accounts = create_utils.GetProjectServiceAccount(
        args, project, compute_client, skip_defaults)

    boot_disk_size_gb = instance_utils.GetBootDiskSizeGb(args)

    disks = []
    if create_utils.CheckSpecifiedDiskArgs(
        args=args, support_disks=False, skip_defaults=skip_defaults):
      disks = create_utils.CreateDiskMessages(
          args=args,
          project=project,
          location=location,
          scope=scope,
          compute_client=compute_client,
          resource_parser=resource_parser,
          image_uri=image_uri,
          create_boot_disk=create_boot_disk,
          boot_disk_size_gb=boot_disk_size_gb,
          support_nvdimm=self._support_nvdimm,
          support_disk_resource_policy=self._support_disk_resource_policy,
          support_source_snapshot_csek=self._support_source_snapshot_csek,
          support_boot_snapshot_uri=self._support_boot_snapshot_uri,
          support_image_csek=self._support_image_csek,
          support_create_disk_snapshots=self._support_create_disk_snapshots,
          support_persistent_attached_disks=False,
          use_disk_type_uri=False)

    machine_type = 'n1-standard-1'
    if args.IsSpecified('machine_type'):
      machine_type = args.machine_type

    can_ip_forward = instance_utils.GetCanIpForward(args, skip_defaults)
    guest_accelerators = create_utils.GetAcceleratorsForInstanceProperties(
        args=args,
        compute_client=compute_client)

    advanced_machine_features = None
    if (self._support_enable_nested_virtualization and
        args.enable_nested_virtualization is not None):
      advanced_machine_features = (
          instance_utils.CreateAdvancedMachineFeaturesMessage(
              compute_client.messages, args.enable_nested_virtualization))

    parsed_resource_policies = []
    resource_policies = getattr(args, 'resource_policies', None)
    if resource_policies:
      for policy in resource_policies:
        resource_policy_ref = maintenance_util.ParseResourcePolicyWithScope(
            resource_parser,
            policy,
            project=project,
            location=location,
            scope=scope)
        parsed_resource_policies.append(resource_policy_ref.SelfLink())

    display_device = None
    if args.IsSpecified('enable_display_device'):
      display_device = compute_client.messages.DisplayDevice(
          enableDisplay=args.enable_display_device)

    reservation_affinity = instance_utils.GetReservationAffinity(
        args, compute_client)

    instance_properties = compute_client.messages.InstanceProperties(
        canIpForward=can_ip_forward,
        description=args.description,
        disks=disks,
        guestAccelerators=guest_accelerators,
        labels=labels,
        machineType=machine_type,
        metadata=metadata,
        minCpuPlatform=args.min_cpu_platform,
        networkInterfaces=network_interfaces,
        serviceAccounts=service_accounts,
        scheduling=scheduling,
        tags=tags,
        resourcePolicies=parsed_resource_policies,
        shieldedInstanceConfig=shielded_instance_config,
        displayDevice=display_device,
        reservationAffinity=reservation_affinity,
        advancedMachineFeatures=advanced_machine_features)

    if self._support_confidential_compute and confidential_instance_config:
      instance_properties.confidentialInstanceConfig = confidential_instance_config

    if self._support_erase_vss and \
      args.IsSpecified('erase_windows_vss_signature'):
      instance_properties.eraseWindowsVssSignature = args.erase_windows_vss_signature

    if self._support_post_key_revocation_action_type and args.IsSpecified(
        'post_key_revocation_action_type'):
      instance_properties.postKeyRevocationActionType = arg_utils.ChoiceToEnum(
          args.post_key_revocation_action_type, compute_client.messages.Instance
          .PostKeyRevocationActionTypeValueValuesEnum)

    bulk_instance_resource = compute_client.messages.BulkInsertInstanceResource(
        count=instance_count,
        instanceProperties=instance_properties,
        minCount=instance_min_count,
        predefinedNames=instance_names,
        sourceInstanceTemplate=source_instance_template)

    if scope == compute_scopes.ScopeEnum.ZONE:
      instance_service = compute_client.apitools_client.instances
      request_message = compute_client.messages.ComputeInstancesBulkInsertRequest(
          bulkInsertInstanceResource=bulk_instance_resource,
          project=project,
          zone=location)
    elif scope == compute_scopes.ScopeEnum.REGION:
      instance_service = compute_client.apitools_client.regionInstances
      request_message = compute_client.messages.ComputeRegionInstancesBulkInsertRequest(
          bulkInsertInstanceResource=bulk_instance_resource,
          project=project,
          region=location)

    return instance_service, request_message

  def Run(self, args):
    instances_flags.ValidateBulkDiskFlags(
        args,
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

    project = properties.VALUES.core.project.GetOrFail()
    location = None
    scope = None

    if args.IsSpecified('zone'):
      location = args.zone
      scope = compute_scopes.ScopeEnum.ZONE
    elif args.IsSpecified('region'):
      location = args.region
      scope = compute_scopes.ScopeEnum.REGION

    instances_service, request = self._CreateRequests(args, holder,
                                                      compute_client,
                                                      resource_parser, project,
                                                      location, scope)

    self._errors = []
    self._log_async = False
    self._status_message = None

    if args.async_:
      self._log_async = True
      try:
        response = instances_service.BulkInsert(request)
        self._operation_selflink = response.selfLink
        return
      except exceptions.HttpError as error:
        raise error

    errors_to_collect = []
    response = compute_client.MakeRequests(
        [(instances_service, 'BulkInsert', request)],
        errors_to_collect=errors_to_collect,
        log_result=False,
        always_return_operation=True,
        no_followup=True)

    self._errors = errors_to_collect
    if response:
      self._status_message = response[0].statusMessage

    return

  def Epilog(self, resources_were_displayed):
    del resources_were_displayed
    if self._errors:
      log.error(self._errors[0][1])
    elif self._log_async:
      log.status.Print('Bulk instance creation in progress: {}'.format(
          self._operation_selflink))
    else:
      if self._errors:
        log.warning(self._errors[0][1])
      log.status.Print(
          'Bulk create request finished with status message: [{}]'.format(
              self._status_message))


CreateAlpha.detailed_help = DETAILED_HELP
