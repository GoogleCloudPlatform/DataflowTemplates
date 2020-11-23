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
"""Command for getting an instance's KRM configuration."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.compute import base_classes
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.compute.instances import flags
from googlecloudsdk.command_lib.util.declarative import flags as declarative_flags
from googlecloudsdk.command_lib.util.declarative.clients import kcc_client
from googlecloudsdk.core import properties


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Export(base.DeclarativeCommand):
  """Export the configuration for a Compute Engine virtual machine."""

  def _GetInstanceRef(self, holder, args):
    return flags.INSTANCE_ARG.ResolveAsResource(
        args,
        holder.resources,
        scope_lister=flags.GetInstanceZoneScopeLister(holder.client))

  @classmethod
  def Args(cls, parser):
    mutex_group = parser.add_group(mutex=True, required=True)
    resource_group = mutex_group.add_group()
    flags.INSTANCE_ARG_NOT_REQUIRED.AddArgument(
        resource_group, operation_type='export')
    declarative_flags.AddAllFlag(mutex_group, collection='project')
    declarative_flags.AddPathFlag(parser)

  def GetAllInstances(self, args, holder):
    request = holder.client.messages.ComputeInstancesAggregatedListRequest(
        project=properties.VALUES.core.project.GetOrFail())
    instances = holder.client.MakeRequests([
        (holder.client.apitools_client.instances, 'AggregatedList', request)
    ])
    resource_uris = [instance.selfLink for instance in instances]

    client = kcc_client.KccClient()
    kcc_documents = client.GetAll(
        resource_uris=resource_uris,
        use_progress_tracker=args.IsSpecified('path'))
    client.SerializeAll(
        resources=kcc_documents, path=args.path, scope_field='zone')

  def Run(self, args):
    declarative_flags.ValidateAllPathArgs(args)
    holder = base_classes.ComputeApiHolder(self.ReleaseTrack())
    if args.IsSpecified('all'):
      self.GetAllInstances(args, holder)
      return

    instance_ref = self._GetInstanceRef(holder, args)
    client = kcc_client.KccClient()
    resource = client.Get(
        resource_uri=instance_ref.SelfLink(),
        use_progress_tracker=args.IsSpecified('path'))
    client.Serialize(path=args.path, resource=resource)
    return
