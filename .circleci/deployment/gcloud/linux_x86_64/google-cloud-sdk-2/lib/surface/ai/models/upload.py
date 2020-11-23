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
"""Command to upload a model in AI platform."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.ai import operations
from googlecloudsdk.api_lib.ai.models import client
from googlecloudsdk.api_lib.util import messages as messages_util
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions as gcloud_exceptions
from googlecloudsdk.command_lib.ai import constants
from googlecloudsdk.command_lib.ai import endpoint_util
from googlecloudsdk.command_lib.ai import flags
from googlecloudsdk.command_lib.ai import models_util
from googlecloudsdk.command_lib.ai import operations_util
from googlecloudsdk.core import yaml


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA)
class Upload(base.CreateCommand):
  """Upload a new model.

  Create a new AI Platform model.
  """

  def __init__(self, *args, **kwargs):
    super(Upload, self).__init__(*args, **kwargs)
    self.messages = client.ModelsClient().messages

  @staticmethod
  def Args(parser):
    flags.AddUploadModelFlags(parser)

  def Run(self, args):
    region_ref = args.CONCEPTS.region.Parse()
    region = region_ref.AsDict()['locationsId']
    with endpoint_util.AiplatformEndpointOverrides(
        version=constants.BETA_VERSION, region=region):
      operation = client.ModelsClient().UploadV1Beta1(
          region_ref, args.display_name, args.description, args.artifact_uri,
          args.container_image_uri, args.container_command, args.container_args,
          args.container_env_vars, args.container_ports,
          args.container_predict_route, args.container_health_route,
          self._BuildExplanationSpec(args))
      return operations_util.WaitForOpMaybe(
          operations_client=operations.OperationsClient(),
          op=operation,
          op_ref=models_util.ParseModelOperation(operation.name))

  def _BuildExplanationSpec(self, args):
    parameters = None
    method = args.explanation_method
    if not method:
      return None
    if method.lower() == 'integrated-gradients':
      parameters = (
          self.messages.GoogleCloudAiplatformV1beta1ExplanationParameters(
              integratedGradientsAttribution=self.messages
              .GoogleCloudAiplatformV1beta1IntegratedGradientsAttribution(
                  stepCount=args.explanation_step_count,
                  smoothGradConfig=self._BuildSmoothGradConfig(args))))
    elif method.lower() == 'xrai':
      parameters = (
          self.messages.GoogleCloudAiplatformV1beta1ExplanationParameters(
              xraiAttribution=self.messages
              .GoogleCloudAiplatformV1beta1XraiAttribution(
                  stepCount=args.explanation_step_count,
                  smoothGradConfig=self._BuildSmoothGradConfig(args))))
    elif method.lower() == 'sampled-shapley':
      parameters = (
          self.messages.GoogleCloudAiplatformV1beta1ExplanationParameters(
              sampledShapleyAttribution=self.messages
              .GoogleCloudAiplatformV1beta1SampledShapleyAttribution(
                  pathCount=args.explanation_path_count)))
    else:
      raise gcloud_exceptions.BadArgumentException(
          '--explanation-method',
          'Explanation method must be one of `integrated-gradients`, '
          '`xrai` and `sampled-shapley`.')
    return self.messages.GoogleCloudAiplatformV1beta1ExplanationSpec(
        metadata=self._ReadExplanationMetadata(args.explanation_metadata_file),
        parameters=parameters)

  def _BuildSmoothGradConfig(self, args):
    if (args.smooth_grad_noise_sigma is None and
        args.smooth_grad_noisy_sample_count is None and
        args.smooth_grad_noise_sigma_by_feature is None):
      return None
    if (args.smooth_grad_noise_sigma is not None and
        args.smooth_grad_noise_sigma_by_feature is not None):
      raise gcloud_exceptions.BadArgumentException(
          '--smooth-grad-noise-sigma', 'Only one of smooth-grad-noise-sigma '
          'and smooth-grad-noise-sigma-by-feature can be set.')
    smooth_grad_config = (
        self.messages.GoogleCloudAiplatformV1beta1SmoothGradConfig(
            noiseSigma=args.smooth_grad_noise_sigma,
            noisySampleCount=args.smooth_grad_noisy_sample_count))
    sigmas = args.smooth_grad_noise_sigma_by_feature
    if sigmas:
      smooth_grad_config.featureNoiseSigma = (
          self.messages
          .GoogleCloudAiplatformV1beta1FeatureNoiseSigma(noiseSigma=[
              self.messages.
              GoogleCloudAiplatformV1beta1FeatureNoiseSigmaNoiseSigmaForFeature(
                  name=k, sigma=float(sigmas[k])) for k in sigmas
          ]))
    return smooth_grad_config

  def _ReadExplanationMetadata(self, explanation_metadata_file):
    explanation_metadata = None
    if not explanation_metadata_file:
      raise gcloud_exceptions.BadArgumentException(
          '--explanation-metadata-file',
          'Explanation metadata file must be specified.')
    # Yaml is a superset of json, so parse json file as yaml.
    data = yaml.load_path(explanation_metadata_file)
    if data:
      explanation_metadata = messages_util.DictToMessageWithErrorCheck(
          data, self.messages.GoogleCloudAiplatformV1beta1ExplanationMetadata)
    return explanation_metadata
