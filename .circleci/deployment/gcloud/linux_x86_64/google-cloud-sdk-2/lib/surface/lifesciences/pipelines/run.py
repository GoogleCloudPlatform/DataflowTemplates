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

"""Implementation of gcloud lifesciences pipelines run.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import base64
from googlecloudsdk.api_lib import lifesciences as lib
from googlecloudsdk.api_lib.lifesciences import exceptions
from googlecloudsdk.api_lib.lifesciences import lifesciences_util
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope.concepts import concepts
from googlecloudsdk.command_lib.util.apis import yaml_data
from googlecloudsdk.command_lib.util.args import labels_util
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.util import files
import six

CLOUD_SDK_IMAGE = 'google/cloud-sdk:slim'
SHARED_DISK = 'gcloud-shared'


class _SharedPathGenerator(object):

  def __init__(self, root):
    self.root = root
    self.index = -1

  def Generate(self):
    self.index += 1
    return '/%s/%s%d' % (SHARED_DISK, self.root, self.index)


def _ValidateAndMergeArgInputs(args):
  """Turn args.inputs and args.inputs_from_file dicts into a single dict.

  Args:
    args: The parsed command-line arguments

  Returns:
    A dict that is the merge of args.inputs and args.inputs_from_file
  Raises:
    files.Error
  """

  is_local_file = {}

  # If no inputs from file, then no validation or merge needed
  if not args.inputs_from_file:
    return args.inputs, is_local_file

  # Initialize the merged dictionary
  arg_inputs = {}

  if args.inputs:
    # Validate args.inputs and args.inputs-from-file do not overlap
    overlap = set(args.inputs.keys()).intersection(
        set(args.inputs_from_file.keys()))
    if overlap:
      raise exceptions.LifeSciencesError(
          '--{0} and --{1} may not specify overlapping values: {2}'
          .format('inputs', 'inputs-from-file', ', '.join(overlap)))

    # Add the args.inputs
    arg_inputs.update(args.inputs)

  # Read up the inputs-from-file and add the values from the file
  for key, value in six.iteritems(args.inputs_from_file):
    arg_inputs[key] = files.ReadFileContents(value)
    is_local_file[key] = True

  return arg_inputs, is_local_file


class Run(base.SilentCommand):
  r"""Defines and runs a pipeline.

  A pipeline is a transformation of a set of inputs to a set of outputs.
  Supports Docker-based commands.

  ## EXAMPLES
  To run a pipeline described in the `pipeline.json` file, run:

    $ {command} --pipeline-file=pipeline.json
  """

  @staticmethod
  def Args(parser):
    """Args is called by calliope to gather arguments for this command.

    Args:
      parser: An argparse parser that you can use to add arguments that go
          on the command line after this command. Positional arguments are
          allowed.
    """
    location_spec = concepts.ResourceSpec.FromYaml(
        yaml_data.ResourceYAMLData.FromPath('lifesciences.location')
        .GetData())
    concept_parsers.ConceptParser.ForResource(
        '--location',
        location_spec,
        'The Google Cloud location to run the pipeline.',
        required=True).AddToParser(parser)

    pipeline = parser.add_mutually_exclusive_group(required=True)
    pipeline.add_argument(
        '--pipeline-file',
        help='''A YAML or JSON file containing a Pipeline object. See
[](https://cloud.google.com/life-sciences/docs/reference/rest/v2beta/projects.locations.pipelines/run#pipeline)
''')
    pipeline.add_argument(
        '--command-line',
        category=base.COMMONLY_USED_FLAGS,
        help='''Command line to run with /bin/sh in the specified
            Docker image. Cannot be used with --pipeline-file.''')

    parser.add_argument(
        '--docker-image',
        category=base.COMMONLY_USED_FLAGS,
        default=CLOUD_SDK_IMAGE,
        help='''A Docker image to run. Requires --command-line to
            be specified and cannot be used with --pipeline-file.''')

    parser.add_argument(
        '--inputs',
        category=base.COMMONLY_USED_FLAGS,
        metavar='NAME=VALUE',
        type=arg_parsers.ArgDict(),
        action=arg_parsers.UpdateAction,
        help='''Map of input PipelineParameter names to values.
            Used to pass literal parameters to the pipeline, and to specify
            input files in Google Cloud Storage that will have a localCopy
            made. Specified as a comma-separated list: --inputs
            file=gs://my-bucket/in.txt,name=hello''')

    parser.add_argument(
        '--inputs-from-file',
        category=base.COMMONLY_USED_FLAGS,
        metavar='NAME=FILE',
        type=arg_parsers.ArgDict(),
        action=arg_parsers.UpdateAction,
        help='''Map of input PipelineParameter names to values.
            Used to pass literal parameters to the pipeline where values come
            from local files; this can be used to send large pipeline input
            parameters, such as code, data, or configuration values.
            Specified as a comma-separated list:
            --inputs-from-file script=myshellscript.sh,pyfile=mypython.py''')

    parser.add_argument(
        '--outputs',
        category=base.COMMONLY_USED_FLAGS,
        metavar='NAME=VALUE',
        type=arg_parsers.ArgDict(),
        action=arg_parsers.UpdateAction,
        help='''Map of output PipelineParameter names to values.
            Used to specify output files in Google Cloud Storage that will be
            made from a localCopy. Specified as a comma-separated list:
            --outputs ref=gs://my-bucket/foo,ref2=gs://my-bucket/bar''')

    parser.add_argument(
        '--logging',
        category=base.COMMONLY_USED_FLAGS,
        help='''The location in Google Cloud Storage to which the pipeline logs
            will be copied. Can be specified as a fully qualified directory
            path, in which case logs will be output with a unique identifier
            as the filename in that directory, or as a fully specified path,
            which must end in `.log`, in which case that path will be
            used. Stdout and stderr logs from the run are also generated and
            output as `-stdout.log` and `-stderr.log`.''')

    parser.add_argument(
        '--env-vars',
        category=base.COMMONLY_USED_FLAGS,
        metavar='NAME=VALUE',
        type=arg_parsers.ArgDict(),
        help='''List of key-value pairs to set as environment variables.''')

    labels_util.AddCreateLabelsFlags(parser)

    parser.add_argument(
        '--disk-size',
        category=base.COMMONLY_USED_FLAGS,
        default=None,
        help='''The disk size(s) in GB, specified as a comma-separated list of
            pairs of disk name and size. For example:
            --disk-size "name:size,name2:size2".
            Overrides any values specified in the pipeline-file.''')

    parser.add_argument(
        '--preemptible',
        category=base.COMMONLY_USED_FLAGS,
        action='store_true',
        help='''Whether to use a preemptible VM for this pipeline. The
            "resource" section of the pipeline-file must also set preemptible
            to "true" for this flag to take effect.''')

    parser.add_argument(
        '--run-id',
        hidden=True,
        help='THIS ARGUMENT NEEDS HELP TEXT.')

    parser.add_argument(
        '--service-account-email',
        default='default',
        help='''The service account used to run the pipeline. If unspecified,
            defaults to the Compute Engine service account for your project.''')

    parser.add_argument(
        '--service-account-scopes',
        metavar='SCOPE',
        type=arg_parsers.ArgList(),
        default=[],
        help='''List of additional scopes to be made available for this service
             account. The following scopes are always requested:

             https://www.googleapis.com/auth/cloud-platform''')

    parser.add_argument(
        '--machine-type',
        default='n1-standard-1',
        help='''The type of VirtualMachine to use. Defaults to n1-standard-1.''')

    parser.add_argument(
        '--zones',
        metavar='ZONE',
        type=arg_parsers.ArgList(),
        help='''List of Compute Engine zones the pipeline can run in.

If no zones are specified with the zones flag, then zones in the
pipeline definition file will be used.

If no zones are specified in the pipeline definition, then the
default zone in your local client configuration is used (and must be specified).

For more information on default zones, see
https://cloud.google.com/compute/docs/gcloud-compute/#set_default_zone_and_region_in_your_local_client''')

    parser.add_argument(
        '--regions',
        metavar='REGION',
        type=arg_parsers.ArgList(),
        help='''List of Compute Engine regions the pipeline can
            run in.

If no regions are specified with the regions flag, then regions in the
pipeline definition file will be used.

If no regions are specified in the pipeline definition, then the
default region in your local client configuration is used.

At least one region or region must be specified.

For more information on default regions, see
https://cloud.google.com/compute/docs/gcloud-compute/#set_default_zone_and_region_in_your_local_client''')

    parser.add_argument(
        '--network',
        help='''The network name to attach the VM's network
            interface to.

The value will be prefixed with global/networks/ unless it contains a /, in
which case it is assumed to be a fully specified network resource URL.

If unspecified, the global default network is used.''')

    parser.add_argument(
        '--subnetwork',
        help='''The subnetwork to use on the provided network.

If the specified network is configured for custom subnet creation, the name of
the subnetwork to attach the instance to must be specified here.

The value is prefixed with regions/*/subnetworks/ unless it contains a /, in
which case it is assumed to be a fully specified subnetwork resource URL.

If the * character appears in the value, it is replaced with the region that
the virtual machine has been allocated in.''')

    parser.add_argument(
        '--boot-disk-size',
        type=int,
        help='''The size of the boot disk in GB.

The boot disk size must be large enough to accommodate all Docker images from
each action in the pipeline at the same time. If not specified, a small but
reasonable default value is used.''')

  def Run(self, args):
    """This is what gets called when the user runs this command.

    Args:
      args: argparse.Namespace, All the arguments that were provided to this
        command invocation.

    Raises:
      files.Error: A file argument could not be read.
      LifeSciencesError: User input was invalid.
      HttpException: An http error response was received while executing api
          request.
    Returns:
      Operation representing the running pipeline.
    """
    pipeline = None
    apitools_client = lifesciences_util.GetLifeSciencesClient('v2beta')
    lifesciences_messages = lifesciences_util.GetLifeSciencesMessages('v2beta')
    if args.pipeline_file:
      pipeline = lifesciences_util.GetFileAsMessage(
          args.pipeline_file,
          lifesciences_messages.Pipeline,
          self.context[lib.STORAGE_V1_CLIENT_KEY])
    elif args.command_line:
      pipeline = lifesciences_messages.Pipeline(
          actions=[lifesciences_messages.Action(
              imageUri=args.docker_image,
              commands=['-c', args.command_line],
              entrypoint='bash')])

    arg_inputs, is_local_file = _ValidateAndMergeArgInputs(args)

    request = None
    # Create messages up front to avoid checking for None everywhere.
    if not pipeline.resources:
      pipeline.resources = lifesciences_messages.Resources()
    resources = pipeline.resources

    if not resources.virtualMachine:
      resources.virtualMachine = lifesciences_messages.VirtualMachine(
          machineType=args.machine_type)
    virtual_machine = resources.virtualMachine

    if not virtual_machine.serviceAccount:
      virtual_machine.serviceAccount = lifesciences_messages.ServiceAccount()

    if args.preemptible:
      virtual_machine.preemptible = args.preemptible

    if args.zones:
      resources.zones = args.zones
    elif not resources.zones and properties.VALUES.compute.zone.Get():
      resources.zones = [properties.VALUES.compute.zone.Get()]

    if args.regions:
      resources.regions = args.regions
    elif not resources.regions and properties.VALUES.compute.region.Get():
      resources.regions = [properties.VALUES.compute.region.Get()]

    if args.service_account_email != 'default':
      virtual_machine.serviceAccount.email = args.service_account_email

    if args.service_account_scopes:
      virtual_machine.serviceAccount.scopes = args.service_account_scopes

    # Always add the cloud-platform scope for user convenience.
    virtual_machine.serviceAccount.scopes.append(
        'https://www.googleapis.com/auth/cloud-platform')

    # Attach custom network/subnetwork (if set).
    if args.network or args.subnetwork:
      if not virtual_machine.network:
        virtual_machine.network = lifesciences_messages.Network()
      if args.network:
        virtual_machine.network.network = args.network
      if args.subnetwork:
        virtual_machine.network.subnetwork = args.subnetwork

    if args.boot_disk_size is not None:
      if args.boot_disk_size <= 0:
        raise exceptions.LifeSciencesError(
            'Boot disk size must be greater than zero.')
      virtual_machine.bootDiskSizeGb = args.boot_disk_size

    # Generate paths for inputs and outputs in a shared location and put them
    # into the environment for actions based on their name.
    env = {}
    if arg_inputs:
      input_generator = _SharedPathGenerator('input')
      for name, value in arg_inputs.items():
        if lifesciences_util.IsGcsPath(value):
          env[name] = input_generator.Generate()
          pipeline.actions.insert(0, lifesciences_messages.Action(
              imageUri=CLOUD_SDK_IMAGE,
              commands=['/bin/sh', '-c', 'gsutil -m -q cp %s ${%s}' %
                        (value, name)]))
        elif name in is_local_file:
          env[name] = input_generator.Generate()
          pipeline.actions.insert(
              0,
              lifesciences_messages.Action(
                  imageUri=CLOUD_SDK_IMAGE,
                  commands=[
                      '/bin/sh', '-c',
                      'echo "%s" | base64 -d > ${%s}' %
                      (base64.b64encode(value.encode()).decode(), name)
                  ]))
        else:
          env[name] = value

    if args.outputs:
      output_generator = _SharedPathGenerator('output')
      for name, value in args.outputs.items():
        env[name] = output_generator.Generate()
        pipeline.actions.append(lifesciences_messages.Action(
            imageUri=CLOUD_SDK_IMAGE,
            commands=['/bin/sh', '-c', 'gsutil -m -q cp ${%s} %s' % (name,
                                                                       value)]))
    if args.env_vars:
      for name, value in args.env_vars.items():
        env[name] = value

    # Merge any existing pipeline arguments into the generated environment and
    # update the pipeline.
    if pipeline.environment:
      for val in pipeline.environment.additionalProperties:
        if val.key not in env:
          env[val.key] = val.value

    pipeline.environment = lifesciences_messages.Pipeline.EnvironmentValue(
        additionalProperties=lifesciences_util.ArgDictToAdditionalPropertiesList(
            env,
            lifesciences_messages.Pipeline.EnvironmentValue.AdditionalProperty))

    if arg_inputs or args.outputs:
      virtual_machine.disks.append(lifesciences_messages.Disk(
          name=SHARED_DISK))

      for action in pipeline.actions:
        action.mounts.append(lifesciences_messages.Mount(
            disk=SHARED_DISK,
            path='/' + SHARED_DISK))

    if args.logging:
      pipeline.actions.append(lifesciences_messages.Action(
          imageUri=CLOUD_SDK_IMAGE,
          commands=['/bin/sh', '-c',
                    'gsutil -m -q cp /google/logs/output ' + args.logging],
          alwaysRun=True))

    # Update disk sizes if specified, potentially including the shared disk.
    if args.disk_size:
      disk_sizes = {}
      for disk_encoding in args.disk_size.split(','):
        parts = disk_encoding.split(':', 1)
        try:
          disk_sizes[parts[0]] = int(parts[1])
        except:
          raise exceptions.LifeSciencesError('Invalid --disk-size.')

      for disk in virtual_machine.disks:
        if disk.name in disk_sizes:
          disk.sizeGb = disk_sizes[disk.name]

    request = lifesciences_messages.RunPipelineRequest(
        pipeline=pipeline,
        labels=labels_util.ParseCreateArgs(
            args, lifesciences_messages.RunPipelineRequest.LabelsValue))
    projectId = lifesciences_util.GetProjectId()
    location_ref = args.CONCEPTS.location.Parse()
    request_wrapper = lifesciences_messages.LifesciencesProjectsLocationsPipelinesRunRequest(
        parent=location_ref.RelativeName(),
        runPipelineRequest=request)

    result = apitools_client.projects_locations_pipelines.Run(request_wrapper)
    log.status.Print('Running [{0}].'.format(result.name))
    return result
