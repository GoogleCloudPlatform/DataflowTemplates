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
"""Database Migration Service migration jobs API."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.database_migration import api_util
from googlecloudsdk.api_lib.storage import storage_util
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.core.resource import resource_property

import six


class MigrationJobsClient(object):
  """Client for migration jobs service in the API."""

  _FIELDS_MAP = ['display_name', 'type', 'dump_path', 'source', 'destination']
  _REVERSE_MAP = ['vm_ip', 'vm_port', 'vm', 'vpc']

  def __init__(self, client=None, messages=None):
    self.client = client or api_util.GetClientInstance()
    self.messages = messages or api_util.GetMessagesModule()
    self._service = self.client.projects_locations_migrationJobs
    self.resource_parser = api_util.GetResourceParser()

  def _ValidateArgs(self, args):
    self._ValidateDumpPath(args)

  def _ValidateDumpPath(self, args):
    if args.dump_path is None:
      return
    try:
      storage_util.ObjectReference.FromArgument(
          args.dump_path, allow_empty_object=False)
    except Exception as e:
      raise exceptions.InvalidArgumentException('dump-path', six.text_type(e))

  def _GetType(self, mj_type, type_value):
    return mj_type.TypeValueValuesEnum.lookup_by_name(type_value)

  def _GetVpcPeeringConnectivity(self, args):
    return self.messages.VpcPeeringConnectivity(vpc=args.peer_vpc)

  def _GetReverseSshConnectivity(self, args):
    return self.messages.ReverseSshConnectivity(
        vm=args.vm,
        vmIp=args.vm_ip,
        vmPort=args.vm_port,
        vpc=args.vpc
    )

  def _GetStaticIpConnectivity(self):
    return self.messages.StaticIpConnectivity()

  def _UpdateLabels(self, args, migration_job, update_fields):
    """Updates labels of the migration job."""
    add_labels = labels_util.GetUpdateLabelsDictFromArgs(args)
    remove_labels = labels_util.GetRemoveLabelsListFromArgs(args)
    value_type = self.messages.MigrationJob.LabelsValue
    update_result = labels_util.Diff(
        additions=add_labels,
        subtractions=remove_labels,
        clear=args.clear_labels
    ).Apply(value_type)
    if update_result.needs_update:
      migration_job.labels = update_result.labels
      update_fields.append('labels')

  def _GetMigrationJob(self, migration_job_id,
                       source_ref, destination_ref, args):
    """Returns a migration job."""
    migration_job_type = self.messages.MigrationJob
    labels = labels_util.ParseCreateArgs(
        args, self.messages.MigrationJob.LabelsValue)
    type_value = self._GetType(migration_job_type, args.type)
    source = source_ref.RelativeName()
    destination = destination_ref.RelativeName()
    params = {}
    if args.IsSpecified('peer_vpc'):
      params['vpcPeeringConnectivity'] = self._GetVpcPeeringConnectivity(args)
    elif args.IsSpecified('vm_ip'):
      params['reverseSshConnectivity'] = self._GetReverseSshConnectivity(args)
    else:
      params['staticIpConnectivity'] = self._GetStaticIpConnectivity()
    return migration_job_type(
        name=migration_job_id,
        labels=labels,
        displayName=args.display_name,
        state=migration_job_type.StateValueValuesEnum.CREATING,
        type=type_value,
        dumpPath=args.dump_path,
        source=source,
        destination=destination,
        **params)

  def _UpdateConnectivity(self, migration_job, args):
    """Update connectivity method for the migration job."""
    if args.IsSpecified('peer_vpc'):
      migration_job.vpcPeeringConnectivity = self._GetVpcPeeringConnectivity(
          args)
      migration_job.reverseSshConnectivity = None
      return
    for field in self._REVERSE_MAP:
      if args.IsSpecified(field):
        migration_job.reverseSshConnectivity = self._GetReverseSshConnectivity(
            args)
        migration_job.vpcPeeringConnectivity = None
        return

  def _GetUpdateMask(self, args):
    """Returns update mask for specified fields."""
    update_fields = [resource_property.ConvertToCamelCase(field)
                     for field in sorted(self._FIELDS_MAP)
                     if args.IsSpecified(field)]
    update_fields.extend(
        ['reverseSshConnectivity.{0}'.format(
            resource_property.ConvertToCamelCase(field))
         for field in sorted(self._REVERSE_MAP) if args.IsSpecified(field)])
    if args.IsSpecified('peer_vpc'):
      update_fields.append('vpcPeeringConnectivity.vpc')
    return  update_fields

  def _GetUpdatedMigrationJob(
      self, migration_job, source_ref, destination_ref, args):
    """Returns updated migration job and list of updated fields."""
    update_fields = self._GetUpdateMask(args)
    if args.IsSpecified('display_name'):
      migration_job.displayName = args.display_name
    if args.IsSpecified('type'):
      migration_job.type = self._GetType(self.messages.MigrationJob, args.type)
    if args.IsSpecified('dump_path'):
      migration_job.dumpPath = args.dump_path
    if args.IsSpecified('source'):
      migration_job.source = source_ref.RelativeName()
    if args.IsSpecified('destination'):
      migration_job.destination = destination_ref.RelativeName()
    self._UpdateConnectivity(migration_job, args)
    self._UpdateLabels(args, migration_job, update_fields)
    return migration_job, update_fields

  def _GetExistingMigrationJob(self, name):
    get_req = self.messages.DatamigrationProjectsLocationsMigrationJobsGetRequest(
        name=name
    )
    return self._service.Get(get_req)

  def Create(self, parent_ref, migration_job_id,
             source_ref, destination_ref, args=None):
    """Creates a migration job.

    Args:
      parent_ref: a Resource reference to a parent
        datamigration.projects.locations resource for this migration
        job.
      migration_job_id: str, the name of the resource to create.
      source_ref: a Resource reference to a
        datamigration.projects.locations.connectionProfiles resource.
      destination_ref: a Resource reference to a
        datamigration.projects.locations.connectionProfiles resource.
      args: argparse.Namespace, The arguments that this command was
          invoked with.

    Returns:
      Operation: the operation for creating the migration job.
    """
    self._ValidateArgs(args)

    migration_job = self._GetMigrationJob(
        migration_job_id, source_ref, destination_ref, args)

    request_id = api_util.GenerateRequestId()
    create_req_type = self.messages.DatamigrationProjectsLocationsMigrationJobsCreateRequest
    create_req = create_req_type(
        migrationJob=migration_job,
        migrationJobId=migration_job.name,
        parent=parent_ref,
        requestId=request_id
    )

    return self._service.Create(create_req)

  def Update(self, name, source_ref, destination_ref, args=None):
    """Updates a migration job.

    Args:
      name: str, the reference of the migration job to
          update.
      source_ref: a Resource reference to a
        datamigration.projects.locations.connectionProfiles resource.
      destination_ref: a Resource reference to a
        datamigration.projects.locations.connectionProfiles resource.
      args: argparse.Namespace, The arguments that this command was
          invoked with.

    Returns:
      Operation: the operation for updating the migration job.678888888
    """
    self._ValidateArgs(args)

    current_mj = self._GetExistingMigrationJob(name)

    migration_job, update_fields = self._GetUpdatedMigrationJob(
        current_mj, source_ref, destination_ref, args)

    request_id = api_util.GenerateRequestId()
    update_req_type = self.messages.DatamigrationProjectsLocationsMigrationJobsPatchRequest
    update_req = update_req_type(
        migrationJob=migration_job,
        name=name,
        requestId=request_id,
        updateMask=','.join(update_fields)
    )

    return self._service.Patch(update_req)
