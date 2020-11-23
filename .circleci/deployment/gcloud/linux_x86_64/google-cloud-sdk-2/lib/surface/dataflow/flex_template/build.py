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
"""Implementation of gcloud dataflow flex_template build command.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.dataflow import apis
from googlecloudsdk.calliope import actions
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.core import properties


def _CommonArgs(parser):
  """Register flags for this command.

  Args:
    parser: argparse.ArgumentParser to register arguments with.
  """
  image_args = parser.add_mutually_exclusive_group(required=True)
  image_building_args = image_args.add_argument_group()
  parser.add_argument(
      'template_file_gcs_path',
      metavar='TEMPLATE_FILE_GCS_PATH',
      help=('The Google Cloud Storage location of the flex template file.'
            'Overrides if file already exists.'),
      type=arg_parsers.RegexpValidator(r'^gs://.*',
                                       'Must begin with \'gs://\''))

  image_args.add_argument(
      '--image',
      help=('Path to the any image registry location of the prebuilt flex '
            'template image.'))

  parser.add_argument(
      '--sdk-language',
      help=('SDK language of the flex template job.'),
      choices=['JAVA', 'PYTHON'],
      required=True)

  parser.add_argument(
      '--metadata-file',
      help='Local path to the metadata json file for the flex template.',
      type=arg_parsers.FileContents())

  parser.add_argument(
      '--print-only',
      help=('Prints the container spec to stdout. Does not save in '
            'Google Cloud Storage.'),
      default=False,
      action=actions.StoreBooleanProperty(
          properties.VALUES.dataflow.print_only))

  image_building_args.add_argument(
      '--image-gcr-path',
      help=('The Google Container Registry location to store the flex '
            'template image to be built.'),
      type=arg_parsers.RegexpValidator(r'^gcr.io/.*',
                                       'Must begin with \'gcr.io/\''),
      required=True)

  image_building_args.add_argument(
      '--jar',
      metavar='JAR',
      type=arg_parsers.ArgList(),
      action=arg_parsers.UpdateAction,
      help=('Local path to your dataflow pipeline jar file and all their '
            'dependent jar files required for the flex template classpath. '
            'You can pass them as a comma separated list or repeat '
            'individually with --jar flag. Ex: --jar="code.jar,dep.jar" or '
            '--jar code.jar, --jar dep.jar.'),
      required=True)

  image_building_args.add_argument(
      '--flex-template-base-image',
      help=('Flex template base image to be used while building the '
            'container image. Allowed choices are JAVA8, JAVA11 or gcr.io '
            'path of the specific version of the base image. For JAVA8 and '
            'JAVA11 option, we use the latest base image version to build '
            'the container. You can also provide a specific version from '
            'this link  https://gcr.io/dataflow-templates-base/'),
      type=arg_parsers.RegexpValidator(
          r'^JAVA11$|^JAVA8$|^gcr.io/.*',
          'Must be JAVA11 or JAVA8 or begin with \'gcr.io/\''),
      required=True)

  image_building_args.add_argument(
      '--env',
      metavar='ENV',
      type=arg_parsers.ArgDict(),
      action=arg_parsers.UpdateAction,
      help=
      ('Environment variables to create for the Dockerfile. '
       'You can pass them as a comma separated list or repeat individually '
       'with --env flag. Ex: --env="A=B,C=D" or --env A=B, --env C=D.'
       'You can find the list of supported environment variables in this '
       'link. https://cloud.google.com/dataflow/docs/guides/templates/'
       'troubleshooting-flex-templates'
       '#setting_required_dockerfile_environment_variables'),
      required=True)


def _CommonRun(args):
  """Runs the command.

  Args:
    args: The arguments that were provided to this command invocation.

  Returns:
    A Job message.
  """
  image_path = args.image
  if not args.image:
    image_path = args.image_gcr_path
    apis.Templates.BuildAndStoreFlexTemplateImage(
        args.image_gcr_path, args.flex_template_base_image, args.jar, args.env,
        args.sdk_language)

  return apis.Templates.BuildAndStoreFlexTemplateFile(
      args.template_file_gcs_path, image_path,
      args.metadata_file, args.sdk_language, args.print_only)


@base.ReleaseTracks(base.ReleaseTrack.GA, base.ReleaseTrack.BETA)
class Build(base.Command):
  """Builds a flex template file from the specified parameters."""

  detailed_help = {
      'DESCRIPTION':
          'Builds a flex template file from the specified parameters.',
      'EXAMPLES':
          """\
          To build and store a the flex template json file, run:

            $ {command} gs://template-file-gcs-path --image=gcr://image-path \
              --metadata-file=/local/path/to/metadata.json --sdk-language=JAVA

            $ {command} gs://template-file-gcs-path \
            --image-gcr-path=gcr://path-tos-tore-image \
            --jar=path/to/pipeline.jar --jar=path/to/dependency.jar \
            --env=FLEX_TEMPLATE_JAVA_MAIN_CLASS=classpath \
            --flex-template-base-image=JAVA11 \
            --metadata-file=/local/path/to/metadata.json --sdk-language=JAVA
          """,
  }

  @staticmethod
  def Args(parser):
    _CommonArgs(parser)

  def Run(self, args):
    return _CommonRun(args)

