# -*- coding: utf-8 -*- #
# Copyright 2018 Google LLC. All Rights Reserved.
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
"""Create worker pool command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding
from googlecloudsdk.api_lib.cloudbuild import cloudbuild_util
from googlecloudsdk.api_lib.cloudbuild import workerpool_config
from googlecloudsdk.api_lib.compute import utils as compute_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.cloudbuild import workerpool_flags
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core import resources


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class CreateBeta(base.CreateCommand):
  """Create a worker pool for use by Google Cloud Build.

  Create a worker pool for use by Google Cloud Build.
  """

  @staticmethod
  def Args(parser):
    """Register flags for this command.

    Args:
      parser: An argparse.ArgumentParser-like object. It is mocked out in order
        to capture some information, but behaves like an ArgumentParser.
    """
    parser = workerpool_flags.AddWorkerpoolCreateArgs(
        parser, release_track=base.ReleaseTrack.BETA)
    parser.display_info.AddFormat("""
          table(
            name,
            createTime.date('%Y-%m-%dT%H:%M:%S%Oz', undefined='-'),
            state
          )
        """)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      Some value that we want to have printed later.
    """

    wp_region = args.region

    release_track = self.ReleaseTrack()
    client = cloudbuild_util.GetClientInstance(release_track, region=wp_region)
    messages = cloudbuild_util.GetMessagesModule(release_track)

    # Get the workerpool proto from either the flags or the specified file.
    wp = messages.WorkerPool()
    if args.config_from_file is not None:
      wp = workerpool_config.LoadWorkerpoolConfigFromPath(
          args.config_from_file, messages)
    else:
      wp.name = args.WORKER_POOL
      if args.peered_network is not None:
        network_config = messages.NetworkConfig()
        network_config.peeredNetwork = args.peered_network
        wp.networkConfig = network_config
      worker_config = messages.WorkerConfig()
      if args.worker_machine_type is not None:
        worker_config.machineType = args.worker_machine_type
      if args.worker_disk_size is not None:
        worker_config.diskSizeGb = compute_utils.BytesToGb(
            args.worker_disk_size)
      wp.workerConfig = worker_config

    parent = properties.VALUES.core.project.Get(required=True)

    # Get the parent project.location ref
    parent_resource = resources.REGISTRY.Create(
        collection='cloudbuild.projects.locations',
        projectsId=parent,
        locationsId=wp_region)

    # Send the Create request
    created_op = client.projects_locations_workerPools.Create(
        messages.CloudbuildProjectsLocationsWorkerPoolsCreateRequest(
            workerPool=wp,
            parent=parent_resource.RelativeName(),
            workerPoolId=wp.name))

    raw_dict = encoding.MessageToDict(created_op.response)
    created_wp = encoding.DictToMessage(raw_dict, messages.WorkerPool)

    # Get the workerpool ref
    wp_resource = resources.REGISTRY.Parse(
        None,
        collection='cloudbuild.projects.locations.workerPools',
        api_version=cloudbuild_util.RELEASE_TRACK_TO_API_VERSION[release_track],
        params={
            'projectsId': parent,
            'locationsId': wp_region,
            'workerPoolsId': created_wp.name,
        })

    log.CreatedResource(wp_resource)

    # Format the workerpool name for display
    try:
      created_wp.name = cloudbuild_util.RegionalWorkerPoolShortName(
          created_wp.name)
    except ValueError:
      pass  # Must be an old version.

    return created_wp


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateAlpha(base.CreateCommand):
  """Create a worker pool for use by Google Cloud Build.

  Create a worker pool for use by Google Cloud Build.
  """

  @staticmethod
  def Args(parser):
    """Register flags for this command.

    Args:
      parser: An argparse.ArgumentParser-like object. It is mocked out in order
        to capture some information, but behaves like an ArgumentParser.
    """
    parser = workerpool_flags.AddWorkerpoolCreateArgs(
        parser, release_track=base.ReleaseTrack.ALPHA)
    parser.display_info.AddFormat("""
          table(
            name,
            createTime.date('%Y-%m-%dT%H:%M:%S%Oz', undefined='-'),
            state
          )
        """)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Returns:
      Some value that we want to have printed later.
    """

    release_track = self.ReleaseTrack()
    client = cloudbuild_util.GetClientInstance(release_track)
    messages = cloudbuild_util.GetMessagesModule(release_track)

    parent = properties.VALUES.core.project.Get(required=True)

    # Get the workerpool proto from either the flags or the specified file.
    wp = messages.WorkerPool()
    if args.config_from_file is not None:
      wp = workerpool_config.LoadWorkerpoolConfigFromPath(
          args.config_from_file, messages)
    else:
      wp.name = args.WORKER_POOL
      if args.region is not None:
        wp.region = args.region
      if args.peered_network is not None:
        network_config = messages.NetworkConfig()
        network_config.peeredNetwork = args.peered_network
        wp.networkConfig = network_config
      worker_config = messages.WorkerConfig()
      if args.worker_machine_type is not None:
        worker_config.machineType = args.worker_machine_type
      if args.worker_disk_size is not None:
        worker_config.diskSizeGb = compute_utils.BytesToGb(
            args.worker_disk_size)
      wp.workerConfig = worker_config

    # Get the parent project ref
    parent_resource = resources.REGISTRY.Create(
        collection='cloudbuild.projects', projectId=parent)

    # Send the Create request
    created_wp = client.projects_workerPools.Create(
        messages.CloudbuildProjectsWorkerPoolsCreateRequest(
            workerPool=wp,
            parent=parent_resource.RelativeName(),
            workerPoolId=wp.name))

    # Get the workerpool ref
    wp_resource = resources.REGISTRY.Parse(
        None,
        collection='cloudbuild.projects.workerPools',
        api_version=cloudbuild_util.RELEASE_TRACK_TO_API_VERSION[release_track],
        params={
            'projectsId': parent,
            'workerPoolsId': created_wp.name,
        })

    log.CreatedResource(wp_resource)

    # Format the workerpool name for display
    try:
      created_wp.name = cloudbuild_util.GlobalWorkerPoolShortName(
          created_wp.name)
    except ValueError:
      pass  # Must be an old version.

    return created_wp
