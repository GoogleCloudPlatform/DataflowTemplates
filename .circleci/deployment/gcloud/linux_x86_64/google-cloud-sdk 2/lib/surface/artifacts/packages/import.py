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

"""Implements the command to import packages into a repository."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.artifacts import flags


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class Import(base.Command):
  """Import one or more packages into an artifact repository."""

  api_version = 'v1alpha1'

  @staticmethod
  def Args(parser):
    """Set up arguements for this command.

    Args:
      parser: An argparse.ArgumentPaser.
    """
    flags.GetRepoArgFromBeta().AddToParser(parser)

    parser.add_argument(
        '--gcs-source',
        metavar='GCS_SOURCE',
        required=True,
        type=arg_parsers.ArgList(),
        help="""\
            The Google Cloud Storage location of a package to import.""")

  def Run(self, args):
    """Run package import command."""
    client = apis.GetClientInstance('artifactregistry', self.api_version)
    messages = client.MESSAGES_MODULE

    repo_ref = args.CONCEPTS.repository.Parse()
    gcs_source = messages.GoogleDevtoolsArtifactregistryV1alpha1GcsSource(
        uris=args.gcs_source)
    import_request = (
        messages.GoogleDevtoolsArtifactregistryV1alpha1ImportArtifactsRequest(
            gcsSource=gcs_source))

    request = (
        messages.ArtifactregistryProjectsLocationsRepositoriesImportRequest(
            googleDevtoolsArtifactregistryV1alpha1ImportArtifactsRequest=import_request,
            parent=repo_ref.RelativeName()))

    return client.projects_locations_repositories.Import(request)


Import.detailed_help = {
    'brief': 'Import one or more packages into an artifact repository.',
    'DESCRIPTION': """
      *{command}* imports packages from Google Cloud Storage into the specified
      artifact repository.
      """,
    'EXAMPLES': """
      To import the package `my-package.deb` from Google Cloud Storage into
      `my-repo`, run:

        $ {0} my-repo --location=us-central1 --gcs-source={1}

      To import the packages `my-package.deb` and `other-package.deb` into
      `my-repo`, run:

        $ {0} my-repo --location=us-central1 --gcs-source={1},{2}
    """.format('{command}', 'gs://my-bucket/path/to/my-package.deb',
               'gs://my-bucket/path/to/other-package.deb')
}
