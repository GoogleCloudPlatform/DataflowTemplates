# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
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
"""Utils for GKE Hub commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import base64
import textwrap

from googlecloudsdk.api_lib.cloudresourcemanager import projects_api
from googlecloudsdk.core.util import files


def AddUnRegisterCommonArgs(parser):
  """Adds the flags shared between '(un)register' subcommands to parser.

  Args:
    parser: an argparse.ArgumentParser, to which the common flags will be added
  """
  # A top level Cluster identifier mutually exclusive group.
  group = parser.add_group(
      mutex=True, required=True, help='Cluster identifier.')
  group.add_argument(
      '--gke-uri',
      type=str,
      help=textwrap.dedent("""\
          The URI of the GKE cluster; for example,
          'https://container.googleapis.com/projects/my-project/locations/us-central1-a/clusters/my-cluster'
          The URI can obtain by calling:
              gcloud container clusters list --uri
          This is only valid if the represented cluster is a GKE cluster. The
          provided URI will be validated to confirm that it maps to the valid
          GKE cluster.
        """),
  )
  group.add_argument(
      '--gke-cluster',
      type=str,
      metavar='LOCATION/CLUSTER_NAME',
      help=textwrap.dedent("""\
          The location/name of the GKE cluster. The location can be a zone or
          a region for e.g `us-central1-a/my-cluster`.
        """),
  )
  # A group with context and kubeconfig flags.
  context_group = group.add_group(help='Non-GKE cluster identifier.')
  context_group.add_argument(
      '--context',
      type=str,
      required=True,
      help=textwrap.dedent("""\
        The cluster context as it appears in the kubeconfig file. You can get
        this value from the command line by running command:
        `kubectl config current-context`.
      """),
  )
  context_group.add_argument(
      '--kubeconfig',
      type=str,
      help=textwrap.dedent("""\
            The kubeconfig file containing an entry for the cluster. Defaults to
            $KUBECONFIG if it is set in the environment, otherwise defaults to
            $HOME/.kube/config.
          """),
  )


def AddCommonArgs(parser):
  """Adds the flags shared between 'hub' subcommands to parser.

  Args:
    parser: an argparse.ArgumentParser, to which the common flags will be added
  """
  parser.add_argument(
      '--kubeconfig',
      type=str,
      help=textwrap.dedent("""\
          The kubeconfig file containing an entry for the cluster. Defaults to
          $KUBECONFIG if it is set in the environment, otherwise defaults to
          to $HOME/.kube/config.
        """),
  )

  parser.add_argument(
      '--context',
      type=str,
      help=textwrap.dedent("""\
        The context in the kubeconfig file that specifies the cluster.
      """),
  )


def UserAccessibleProjectIDSet():
  """Retrieve the project IDs of projects the user can access.

  Returns:
    set of project IDs.
  """
  return set(p.projectId for p in projects_api.List())


def Base64EncodedFileContents(filename):
  """Reads the provided file, and returns its contents, base64-encoded.

  Args:
    filename: The path to the file, absolute or relative to the current working
      directory.

  Returns:
    A string, the contents of filename, base64-encoded.

  Raises:
   files.Error: if the file cannot be read.
  """
  return base64.b64encode(
      files.ReadBinaryFileContents(files.ExpandHomeDir(filename)))


def GenerateWIUpdateMsgString(membership, issuer_url, resource_name,
                              cluster_name):
  """Generates user message with information about enabling/disabling Workload Identity.

  We do not allow updating issuer url from one non-empty value to another.
  Args:
    membership: membership resource.
    issuer_url: The discovery URL for the cluster's service account token
      issuer.
    resource_name: The full membership resource name.
    cluster_name: User supplied cluster_name.

  Returns:
    A string, the message string for user to display information about
    enabling/disabling WI on a membership, if the issuer url is changed
    from empty to non-empty value or vice versa. An empty string is returned
    for other cases
  """
  if membership.authority and not issuer_url:
    # Since the issuer is being set to an empty value from a non-empty value
    # the user is trying to disable WI on the associated membership resource.
    return ('A membership [{}] for the cluster [{}] already exists. The cluster'
            ' was previously registered with Workload Identity'
            ' enabled. Continuing will disable Workload Identity on your'
            ' membership, and will reinstall the Connect agent deployment.'
            .format(resource_name, cluster_name))

  if not membership.authority and issuer_url:
    # Since the issuer is being set to a non-empty value from an empty value
    # the user is trying to enable WI on the associated membership resource.
    return ('A membership [{}] for the cluster [{}] already exists. The cluster'
            ' was previously registered without Workload Identity.'
            ' Continuing will enable Workload Identity on your'
            ' membership, and will reinstall the Connect agent deployment.'
            .format(resource_name, cluster_name))

  return ''


def ReleaseTrackCommandPrefix(release_track):
  """Returns a prefix to add to a gcloud command.

  This is meant for formatting an example string, such as:
    gcloud {}container hub register-cluster

  Args:
    release_track: A ReleaseTrack

  Returns:
   a prefix to add to a gcloud based on the release track
  """

  prefix = release_track.prefix
  return prefix + ' ' if prefix else ''
