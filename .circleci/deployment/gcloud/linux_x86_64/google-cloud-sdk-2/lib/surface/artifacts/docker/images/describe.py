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
"""Delete an Artifact Registry container image."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.artifacts import docker_util
from googlecloudsdk.command_lib.artifacts import flags


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA,
                    base.ReleaseTrack.GA)
class Describe(base.DescribeCommand):
  """Describe an Artifact Registry container image.

  Reference an image by tag or digest using the format:

    LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY-ID/IMAGE:tag
    LOCATION-docker.pkg.dev/PROJECT-ID/REPOSITORY-ID/IMAGE@sha256:digest

  This command can fail for the following reasons:
    * The repository format is invalid.
    * The specified image does not exist.
    * The active account does not have permission to view images.
  """

  detailed_help = {
      "DESCRIPTION":
          "{description}",
      "EXAMPLES":
          """\
    To describe an image digest `abcxyz` under image `busy-box`:

        $ {command} us-west1-docker.pkg.dev/my-project/my-repository/busy-box@sha256:abcxyz

    To describe an image `busy-box` with tag `my-tag`:

        $ {command} us-west1-docker.pkg.dev/my-project/my-repository/busy-box:my-tag
    """,
  }

  @staticmethod
  def Args(parser):
    parser.display_info.AddFormat("yaml")
    flags.GetImageRequiredArg().AddToParser(parser)
    flags.GetShowAllMetadataFlag().AddToParser(parser)
    flags.GetMetadataFilterFlag().AddToParser(parser)
    flags.GetShowBuildDetailsFlag().AddToParser(parser)
    flags.GetShowPackageVulnerabilityFlag().AddToParser(parser)
    flags.GetShowImageBasisFlag().AddToParser(parser)
    flags.GetShowDeploymentFlag().AddToParser(parser)

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: an argparse namespace. All the arguments that were provided to this
        command invocation.

    Raises:
      InvalidImageNameError: If the user specified an invalid image name.
    Returns:
      Some value that we want to have printed later.
    """
    return docker_util.DescribeDockerImage(args)
