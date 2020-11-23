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
"""Flags defination for gcloud aiplatform."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import sys

from googlecloudsdk.api_lib.util import apis

from googlecloudsdk.calliope import actions
from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope.concepts import concepts
from googlecloudsdk.calliope.concepts import deps
from googlecloudsdk.command_lib.ai import constants
from googlecloudsdk.command_lib.ai import errors
from googlecloudsdk.command_lib.ai import region_util
from googlecloudsdk.command_lib.util.apis import arg_utils
from googlecloudsdk.command_lib.util.concepts import concept_parsers
from googlecloudsdk.core import properties


CUSTOM_JOB_NAME = base.Argument('name', help=('Custom job\'s name to query.'))
CUSTOM_JOB_DISPLAY_NAME = base.Argument(
    '--display-name',
    required=True,
    help=('Display name of the custom job to create.'))
AIPLATFORM_REGION = base.Argument(
    '--region',
    help=(
        'Region of the AI Platform service to use. If not specified, the value '
        'of the `ai/region` config property is used. If that property '
        'is not configured, then you will be prompted to select a region. When '
        'you specify this flag, its value is stored in the `ai/region` '
        'config property.'),
    action=actions.StoreProperty(properties.VALUES.ai.region))
PYTHON_PACKGE_URIS = base.Argument(
    '--python-package-uris',
    metavar='PYTHON_PACKAGE_URIS',
    type=arg_parsers.ArgList(),
    help='The common python package uris that will be used by python image. '
    'e.g. --python-package-uri=path1,path2'
    'If customizing the python package is needed, please use config instead.')

CUSTOM_JOB_CONFIG = base.Argument(
    '--config',
    help="""
Path to the job configuration file. This file should be a YAML document containing a CustomJobSpec.
If an option is specified both in the configuration file **and** via command line arguments, the command line arguments
override the configuration file. Note that keys with underscore are invalid.

Example(YAML):

  workerPoolSpecs:
    machineSpec:
      machineType: n1-highmem-2
    replicaCount: 1
    containerSpec:
      imageUri: gcr.io/ucaip-test/ucaip-training-test""")

WORKER_POOL_SPEC = base.Argument(
    '--worker-pool-spec',
    action='append',
    type=arg_parsers.ArgDict(
        spec={
            'replica-count': int,
            'machine-type': str,
            'container-image-uri': str,
            'python-image-uri': str,
            'python-module': str,
        },
        required_keys=['machine-type']),
    metavar='WORKER_POOL_SPEC',
    help="""
Define the worker pool configuration used by the custom job. You can specify multiple
worker pool specs in order to create a custom job with multiple worker pools.

The spec can contain the following fields, which are listed with corresponding
fields in the WorkerPoolSpec API message:

*machine-type*::: (Required): machineSpec.machineType
*replica-count*::: replicaCount
*container-image-uri*::: containerSpec.imageUri
*python-image-uri*::: pythonPackageSpec.executorImageUri
*python-module*::: pythonPackageSpec.pythonModule

For example:
`--worker-pool-spec=replica-count=1,machine-type=n1-highmem-2,container-image-uri=gcr.io/ucaip-test/ucaip-training-test`
""")

HPTUNING_JOB_DISPLAY_NAME = base.Argument(
    '--display-name',
    required=True,
    help=('Display name of the hyperparameter tuning job to create.'))

HPTUNING_MAX_TRIAL_COUNT = base.Argument(
    '--max-trial-count',
    type=int,
    default=1,
    help=('Desired total number of trials. The default value is 1.'))

HPTUNING_PARALLEL_TRIAL_COUNT = base.Argument(
    '--parallel-trial-count',
    type=int,
    default=1,
    help=(
        'Desired number of Trials to run in parallel. The default value is 1.'))

HPTUNING_JOB_CONFIG = base.Argument(
    '--config',
    required=True,
    help="""
Path to the job configuration file. This file should be a YAML document containing a HyperparameterTuningSpec.
If an option is specified both in the configuration file **and** via command line arguments, the command line arguments
override the configuration file.

Example(YAML):

  displayName: TestHpTuningJob
  maxTrialCount: 1
  parallelTrialCount: 1
  studySpec:
    metrics:
    - metricId: x
      goal: MINIMIZE
    parameters:
    - parameterId: z
      integerValueSpec:
        minValue: 1
        maxValue: 100
    algorithm: RANDOM_SEARCH
  trialJobSpec:
    workerPoolSpecs:
    - machineSpec:
        machineType: n1-standard-4
      replicaCount: 1
      containerSpec:
        imageUri: gcr.io/ucaip-test/ucaip-training-test
""")

_POLLING_INTERVAL_FLAG = base.Argument(
    '--polling-interval',
    type=arg_parsers.BoundedInt(1, sys.maxsize, unlimited=True),
    default=60,
    help=('Number of seconds to wait between efforts to fetch the latest '
          'log messages.'))

_ALLOW_MULTILINE_LOGS = base.Argument(
    '--allow-multiline-logs',
    action='store_true',
    default=False,
    help='Output multiline log messages as single records.')

_TASK_NAME = base.Argument(
    '--task-name',
    required=False,
    default=None,
    help='If set, display only the logs for this particular task.')


def AddCreateCustomJobFlags(parser):
  """Adds flags related to create a custom job."""
  AddRegionResourceArg(parser, 'to create a custom job')
  CUSTOM_JOB_DISPLAY_NAME.AddToParser(parser)
  PYTHON_PACKGE_URIS.AddToParser(parser)
  worker_pool_spec_group = base.ArgumentGroup(
      help='Worker pool specification.', required=True)
  worker_pool_spec_group.AddArgument(CUSTOM_JOB_CONFIG)
  worker_pool_spec_group.AddArgument(WORKER_POOL_SPEC)
  worker_pool_spec_group.AddToParser(parser)


def AddStreamLogsFlags(parser):
  _POLLING_INTERVAL_FLAG.AddToParser(parser)
  _TASK_NAME.AddToParser(parser)
  _ALLOW_MULTILINE_LOGS.AddToParser(parser)


def GetModelIdArg(required=True):
  return base.Argument(
      '--model',
      help='Id of the uploaded model.',
      required=required)


def GetDeployedModelId(required=True):
  return base.Argument(
      '--deployed-model-id',
      help='Id of the deployed model.',
      required=required)


def GetDisplayNameArg(noun, required=True):
  return base.Argument(
      '--display-name',
      required=required,
      help='Display name of the {noun}.'.format(noun=noun))


def GetDescriptionArg(noun):
  return base.Argument(
      '--description',
      required=False,
      default=None,
      help='Description of the {noun}.'.format(noun=noun))


def AddPredictInstanceArg(parser, required=True):
  """Add arguments for different types of predict instances."""
  base.Argument(
      '--json-request',
      required=required,
      help="""\
      Path to a local file containing the body of a JSON request.

      An example of a JSON request:

          {
            "instances": [
              {"x": [1, 2], "y": [3, 4]},
              {"x": [-1, -2], "y": [-3, -4]}
            ]
          }

      This flag accepts "-" for stdin.
      """).AddToParser(parser)


def GetTrafficSplitArg():
  """Add arguments for traffic split."""
  return base.Argument(
      '--traffic-split',
      metavar='DEPLOYED_MODEL_ID=VALUE',
      type=arg_parsers.ArgDict(value_type=int),
      action=arg_parsers.UpdateAction,
      help=('List of paris of deployed model id and value to set as traffic '
            'split.'))


def AddTrafficSplitGroupArgs(parser):
  """Add arguments for traffic split."""
  group = parser.add_mutually_exclusive_group(required=False)
  group.add_argument(
      '--traffic-split',
      metavar='DEPLOYED_MODEL_ID=VALUE',
      type=arg_parsers.ArgDict(value_type=int),
      action=arg_parsers.UpdateAction,
      help=('List of paris of deployed model id and value to set as traffic '
            'split.'))

  group.add_argument(
      '--clear-traffic-split',
      action='store_true',
      help=('Clears the traffic split map. If the map is empty, the endpoint '
            'is to not accept any traffic at the moment.'))


def AddPredictionResourcesArgs(parser, version):
  """Add arguments for prediction resources."""
  base.Argument(
      '--min-replica-count',
      type=arg_parsers.BoundedInt(1, sys.maxsize, unlimited=True),
      help=("""\
Minimum number of machine replicas the deployed model will be always deployed
on. If specified, the value must be equal to or larger than 1.

If not specified and the uploaded models use dedicated resources, the default
value is 1.
""")
  ).AddToParser(parser)

  base.Argument(
      '--max-replica-count',
      type=int,
      help=('Maximum number of machine replicas the deployed model will be '
            'always deployed on.')
  ).AddToParser(parser)

  # TODO(b/168930155): add the link for available machine types after the docs
  #   are out.
  base.Argument(
      '--machine-type',
      help="""\
Type of machine on which to serve the model. Currently only applies to online prediction.
If the uploaded models use dedicated resources, the machine type is a required field for deployment.
""").AddToParser(parser)

  base.Argument(
      '--accelerator',
      type=arg_parsers.ArgDict(
          spec={
              'type': str,
              'count': int,
          }, required_keys=['type']),
      help="""\
Manage the accelerator config for GPU serving. When deploying a model with
Compute Engine Machine Types, a GPU accelerator may also
be selected.

*type*::: The type of the accelerator. Choices are {}.

*count*::: The number of accelerators to attach to each machine running the job.
 This is usually 1. If not specified, the default value is 1.

For example:
`--accelerator=type=nvidia-tesla-k80,count=1`""".format(', '.join([
    "'{}'".format(c) for c in GetAcceleratorTypeMapper(version).choices
  ]))).AddToParser(parser)


def GetEnableAccessLoggingArg():
  return base.Argument(
      '--enable-access-logging',
      action='store_true',
      default=False,
      required=False,
      help="""\
If true, online prediction access logs are sent to Cloud Logging.

These logs are standard server access logs, containing information like
timestamp and latency for each prediction request.
""")


def GetEnableContainerLoggingArg():
  return base.Argument(
      '--enable-container-logging',
      action='store_true',
      default=False,
      required=False,
      help="""\
If true, the container of the deployed model instances will send `stderr` and
`stdout` streams to Cloud Logging.

Currently, only supported for custom-trained Models and AutoML Tables Models.
""")


def GetServiceAccountArg():
  return base.Argument(
      '--service-account',
      required=False,
      help="""\
Service account that the deployed model's container runs as. Specify the
email address of the service account. If this service account is not
specified, the container runs as a service account that doesn't have access
to the resource project.
""")


def RegionAttributeConfig():
  return concepts.ResourceParameterAttributeConfig(
      name='region',
      help_text='Cloud region for the {resource}.',
      fallthroughs=[
          deps.PropertyFallthrough(properties.VALUES.ai.region),
          deps.Fallthrough(function=region_util.PromptForRegion, hint='region')
      ])


def GetRegionResourceSpec():
  return concepts.ResourceSpec(
      'aiplatform.projects.locations',
      resource_name='region',
      locationsId=RegionAttributeConfig(),
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG)


def GetModelResourceSpec(resource_name='model'):
  return concepts.ResourceSpec(
      'aiplatform.projects.locations.models',
      resource_name=resource_name,
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG,
      locationsId=RegionAttributeConfig(),
      disable_auto_completers=False)


def AddRegionResourceArg(parser, verb):
  """Add a resource argument for a cloud AI Platform region.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
  """
  concept_parsers.ConceptParser.ForResource(
      '--region',
      GetRegionResourceSpec(),
      'Cloud region {}.'.format(verb),
      required=True).AddToParser(parser)


def AddModelResourceArg(parser, verb):
  """Add a resource argument for a cloud AI Platform model.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
  """
  name = 'model'
  concept_parsers.ConceptParser.ForResource(
      name, GetModelResourceSpec(), 'Model {}.'.format(verb),
      required=True).AddToParser(parser)


def AddUploadModelFlags(parser):
  """Adds flags for UploadModel."""
  AddRegionResourceArg(parser, 'to upload model')
  base.Argument(
      '--display-name', required=True,
      help=('Display name of the model.')).AddToParser(parser)
  base.Argument(
      '--description', required=False,
      help=('Description of the model.')).AddToParser(parser)
  base.Argument(
      '--container-image-uri',
      required=True,
      help=("""\
URI of the Model serving container file in the Container Registry
(e.g. gcr.io/myproject/server:latest).
""")).AddToParser(parser)
  base.Argument(
      '--artifact-uri',
      help=("""\
Path to the directory containing the Model artifact and any of its
supporting files.
""")).AddToParser(parser)
  parser.add_argument(
      '--container-env-vars',
      metavar='KEY=VALUE',
      type=arg_parsers.ArgDict(),
      action=arg_parsers.UpdateAction,
      help='List of key-value pairs to set as environment variables.')
  parser.add_argument(
      '--container-command',
      type=arg_parsers.ArgList(),
      metavar='COMMAND',
      action=arg_parsers.UpdateAction,
      help="""\
Entrypoint for the container image. If not specified, the container
image's default entrypoint is run.
""")
  parser.add_argument(
      '--container-args',
      metavar='ARG',
      type=arg_parsers.ArgList(),
      action=arg_parsers.UpdateAction,
      help="""\
Comma-separated arguments passed to the command run by the container
image. If not specified and no `--command` is provided, the container
image's default command is used.
""")
  parser.add_argument(
      '--container-ports',
      metavar='PORT',
      type=arg_parsers.ArgList(element_type=arg_parsers.BoundedInt(1, 65535)),
      action=arg_parsers.UpdateAction,
      help="""\
Container ports to receive requests at. Must be a number between 1 and 65535,
inclusive.
""")
  parser.add_argument(
      '--container-predict-route',
      help='HTTP path to send prediction requests to inside the container.')
  parser.add_argument(
      '--container-health-route',
      help='HTTP path to send health checks to inside the container.')
  # For Explanation.
  parser.add_argument(
      '--explanation-method',
      help='Method used for explanation. Accepted values are `integrated-gradients`, `xrai` and `sampled-shapley`.'
  )
  parser.add_argument(
      '--explanation-metadata-file',
      help='Path to a local JSON file that contains the metadata describing the Model\'s input and output for explanation.'
  )
  parser.add_argument(
      '--explanation-step-count',
      type=int,
      help='Number of steps to approximate the path integral for explanation.')
  parser.add_argument(
      '--explanation-path-count',
      type=int,
      help='Number of feature permutations to consider when approximating the Shapley values for explanation.'
  )
  parser.add_argument(
      '--smooth-grad-noisy-sample-count',
      type=int,
      help='Number of gradient samples used for approximation at explanation. Only applicable to explanation method `integrated-gradients` or `xrai`.'
  )
  parser.add_argument(
      '--smooth-grad-noise-sigma',
      type=float,
      help='Single float value used to add noise to all the features for explanation. Only applicable to explanation method `integrated-gradients` or `xrai`.'
  )
  parser.add_argument(
      '--smooth-grad-noise-sigma-by-feature',
      metavar='KEY=VALUE',
      type=arg_parsers.ArgDict(),
      action=arg_parsers.UpdateAction,
      help='Noise sigma by features for explanation. Noise sigma represents the standard deviation of the gaussian kernel that will be used to add noise to interpolated inputs prior to computing gradients. Only applicable to explanation method `integrated-gradients` or `xrai`.'
  )


def GetEndpointId():
  return base.Argument('name', help='The endpoint\'s id.')


def GetEndpointResourceSpec(resource_name='endpoint'):
  return concepts.ResourceSpec(
      constants.ENDPOINTS_COLLECTION,
      resource_name=resource_name,
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG,
      locationsId=RegionAttributeConfig(),
      disable_auto_completers=False)


def AddEndpointResourceArg(parser, verb):
  """Add a resource argument for a Cloud AI Platform endpoint.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
  """
  concept_parsers.ConceptParser.ForResource(
      'endpoint',
      GetEndpointResourceSpec(),
      'The endpoint {}.'.format(verb),
      required=True).AddToParser(parser)


def ParseAcceleratorFlag(accelerator, version):
  """Validates and returns a accelerator config message object."""
  if accelerator is None:
    return None
  types = list(c for c in GetAcceleratorTypeMapper(version).choices)
  raw_type = accelerator.get('type', None)
  if raw_type not in types:
    raise errors.ArgumentError("""\
The type of the accelerator can only be one of the following: {}.
""".format(', '.join(["'{}'".format(c) for c in types])))
  accelerator_count = accelerator.get('count', 1)
  if accelerator_count <= 0:
    raise errors.ArgumentError("""\
The count of the accelerator must be greater than 0.
""")
  if version == constants.ALPHA_VERSION:
    accelerator_msg = (
        apis.GetMessagesModule(
            constants.AI_PLATFORM_API_NAME,
            constants.AI_PLATFORM_API_VERSION[version])
        .GoogleCloudAiplatformV1alpha1MachineSpec)
  else:
    accelerator_msg = (
        apis.GetMessagesModule(
            constants.AI_PLATFORM_API_NAME,
            constants.AI_PLATFORM_API_VERSION[version])
        .GoogleCloudAiplatformV1beta1MachineSpec)
  accelerator_type = arg_utils.ChoiceToEnum(
      raw_type, accelerator_msg.AcceleratorTypeValueValuesEnum)
  return accelerator_msg(
      acceleratorCount=accelerator_count,
      acceleratorType=accelerator_type)


def GetAcceleratorTypeMapper(version):
  """Get a mapper for accelerator type to enum value."""
  if version == constants.ALPHA_VERSION:
    return arg_utils.ChoiceEnumMapper(
        'generic-accelerator',
        apis.GetMessagesModule(constants.AI_PLATFORM_API_NAME,
                               constants.AI_PLATFORM_API_VERSION[version])
        .GoogleCloudAiplatformV1beta1MachineSpec.AcceleratorTypeValueValuesEnum,
        help_str='The available types of accelerators.',
        include_filter=lambda x: x.startswith('NVIDIA'),
        required=False)
  return arg_utils.ChoiceEnumMapper(
      'generic-accelerator',
      apis.GetMessagesModule(constants.AI_PLATFORM_API_NAME,
                             constants.AI_PLATFORM_API_VERSION[version])
      .GoogleCloudAiplatformV1beta1MachineSpec.AcceleratorTypeValueValuesEnum,
      help_str='The available types of accelerators.',
      include_filter=lambda x: x.startswith('NVIDIA'),
      required=False)


def AddCreateHpTuningJobFlags(parser, algorithm_enum):
  AddRegionResourceArg(parser, 'to upload model')
  HPTUNING_JOB_DISPLAY_NAME.AddToParser(parser)
  HPTUNING_JOB_CONFIG.AddToParser(parser)
  HPTUNING_MAX_TRIAL_COUNT.AddToParser(parser)
  HPTUNING_PARALLEL_TRIAL_COUNT.AddToParser(parser)

  arg_utils.ChoiceEnumMapper(
      '--algorithm',
      algorithm_enum,
      help_str='Search algorithm specified for the given study. '
  ).choice_arg.AddToParser(parser)


def GetCustomJobResourceSpec(resource_name='custom_job'):
  return concepts.ResourceSpec(
      constants.CUSTOM_JOB_COLLECTION,
      resource_name=resource_name,
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG,
      locationsId=RegionAttributeConfig(),
      disable_auto_completers=False)


def AddCustomJobResourceArg(parser, verb):
  """Add a resource argument for a Cloud AI Platform custom job.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
  """
  concept_parsers.ConceptParser.ForResource(
      'custom_job',
      GetCustomJobResourceSpec(),
      'The custom job {}.'.format(verb),
      required=True).AddToParser(parser)


def GetHptuningJobResourceSpec(resource_name='hptuning_job'):
  return concepts.ResourceSpec(
      constants.HPTUNING_JOB_COLLECTION,
      resource_name=resource_name,
      projectsId=concepts.DEFAULT_PROJECT_ATTRIBUTE_CONFIG,
      locationsId=RegionAttributeConfig(),
      disable_auto_completers=False)


def AddHptuningJobResourceArg(parser, verb):
  """Add a resource argument for a Cloud AI Platform hyperparameter tuning  job.

  NOTE: Must be used only if it's the only resource arg in the command.

  Args:
    parser: the parser for the command.
    verb: str, the verb to describe the resource, such as 'to update'.
  """
  concept_parsers.ConceptParser.ForResource(
      'hptuning_job',
      GetHptuningJobResourceSpec(),
      'The hyperparameter tuning job {}.'.format(verb),
      required=True).AddToParser(parser)
