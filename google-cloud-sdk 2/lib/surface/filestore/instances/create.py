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
"""Creates a Cloud Filestore instance."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.filestore import filestore_client
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions
from googlecloudsdk.command_lib.filestore.instances import flags as instances_flags
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.core import log
from googlecloudsdk.core import properties

import six


def _CommonArgs(parser, api_version=filestore_client.V1_API_VERSION):
  instances_flags.AddInstanceCreateArgs(parser, api_version)


@base.ReleaseTracks(base.ReleaseTrack.GA)
class Create(base.CreateCommand):
  """Create a Cloud Filestore instance."""

  _API_VERSION = filestore_client.V1_API_VERSION

  @staticmethod
  def Args(parser):
    _CommonArgs(parser, Create._API_VERSION)

  def Run(self, args):
    """Create a Cloud Filestore instance in the current project."""
    instance_ref = args.CONCEPTS.instance.Parse()
    client = filestore_client.FilestoreClient(self._API_VERSION)
    tier = instances_flags.GetTierArg(
        client.messages, self._API_VERSION).GetEnumForChoice(args.tier)
    labels = labels_util.ParseCreateArgs(args,
                                         client.messages.Instance.LabelsValue)
    instance = client.ParseFilestoreConfig(
        tier=tier,
        description=args.description,
        file_share=args.file_share,
        network=args.network,
        labels=labels,
        zone=instance_ref.locationsId)
    try:
      client.ValidateFileShares(instance)
    except filestore_client.InvalidCapacityError as e:
      raise exceptions.InvalidArgumentException('--file-share',
                                                six.text_type(e))
    result = client.CreateInstance(instance_ref, args.async_, instance)
    if args.async_:
      command = properties.VALUES.metrics.command_name.Get().split('.')
      if command:
        command[-1] = 'list'
      log.status.Print(
          'Check the status of the new instance by listing all instances:\n  '
          '$ {} '.format(' '.join(command)))
    return result


@base.ReleaseTracks(base.ReleaseTrack.BETA)
class CreateBeta(Create):
  """Create a Cloud Filestore instance."""

  _API_VERSION = filestore_client.BETA_API_VERSION

  @staticmethod
  def Args(parser):
    _CommonArgs(parser, CreateBeta._API_VERSION)

  def Run(self, args):
    """Create a Cloud Filestore instance in the current project."""
    instance_ref = args.CONCEPTS.instance.Parse()
    client = filestore_client.FilestoreClient(self._API_VERSION)
    tier = instances_flags.GetTierArg(
        client.messages, self._API_VERSION).GetEnumForChoice(args.tier)
    labels = labels_util.ParseCreateArgs(args,
                                         client.messages.Instance.LabelsValue)
    try:
      nfs_export_options = client.MakeNFSExportOptionsMsg(
          messages=client.messages,
          nfs_export_options=args.file_share.get('nfs-export-options', []))
    except KeyError as err:
      raise exceptions.InvalidArgumentException('--file-share',
                                                six.text_type(err))
    instance = client.ParseFilestoreConfig(
        tier=tier,
        description=args.description,
        file_share=args.file_share,
        network=args.network,
        labels=labels,
        zone=instance_ref.locationsId,
        nfs_export_options=nfs_export_options)
    result = client.CreateInstance(instance_ref, args.async_, instance)
    if args.async_:
      command = properties.VALUES.metrics.command_name.Get().split('.')
      if command:
        command[-1] = 'list'
      log.status.Print(
          'Check the status of the new instance by listing all instances:\n  '
          '$ {} '.format(' '.join(command)))
    return result


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class CreateAlpha(CreateBeta):
  """Create a Cloud Filestore instance."""

  _API_VERSION = filestore_client.ALPHA_API_VERSION

  @staticmethod
  def Args(parser):
    _CommonArgs(parser, CreateAlpha._API_VERSION)


Create.detailed_help = {
    'DESCRIPTION':
        'Create a Cloud Filestore instance.',
    'EXAMPLES':
        """\
The following command creates a Cloud Filestore instance named NAME with a
single volume.

  $ {command} NAME \
  --description=DESCRIPTION --tier=TIER \
  --file-share=name=VOLUME_NAME,capacity=CAPACITY \
  --network=name=NETWORK_NAME,reserved-ip-range=RESERVED_IP_RANGE
"""
}


CreateBeta.detailed_help = {
    'DESCRIPTION':
        'Create a Cloud Filestore instance.',
    'EXAMPLES':
        """\
    The following command creates a Cloud Filestore instance named NAME with a single volume.

      $ {command} NAME \
      --description=DESCRIPTION --tier=TIER \
      --file-share=name=VOLUME_NAME,capacity=CAPACITY \
      --network=name=NETWORK_NAME,reserved-ip-range=RESERVED_IP_RANGE \
      --zone=ZONE \
      --flags-file=FLAGS_FILE

    Example json configuration file:
  {
  "--file-share":
  {
    "capacity": "102400",
    "name": "my_vol",
    "nfs-export-options": [
      {
        "access-mode": "READ_WRITE",
        "ip-ranges": [
          "10.0.0.0/29",
          "10.2.0.0/29"
        ],
        "squash-mode": "ROOT_SQUASH",
        "anon_uid": 1003,
        "anon_gid": 1003
      },
       {
        "access-mode": "READ_ONLY",
        "ip-ranges": [
          "192.168.0.0/24"
        ],
        "squash-mode": "NO_ROOT_SQUASH"
      }
    ],
  }
  }

    """
    }
