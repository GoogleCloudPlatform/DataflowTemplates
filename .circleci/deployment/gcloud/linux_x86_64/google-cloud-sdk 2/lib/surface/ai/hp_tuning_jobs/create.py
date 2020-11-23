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
"""Command to create a hyperparameter tuning job in AI platform."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.api_lib.ai.hp_tuning_jobs import client
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.ai import constants
from googlecloudsdk.command_lib.ai import endpoint_util
from googlecloudsdk.command_lib.ai import flags
from googlecloudsdk.command_lib.ai import hp_tuning_jobs_util
from googlecloudsdk.command_lib.util.apis import arg_utils
from googlecloudsdk.core import log


@base.ReleaseTracks(base.ReleaseTrack.BETA, base.ReleaseTrack.ALPHA)
class Create(base.CreateCommand):
  """Create a hyperparameter tuning job."""

  @staticmethod
  def Args(parser):
    flags.AddCreateHpTuningJobFlags(
        parser, client.HpTuningJobsClient.GetAlgorithmEnum())

  def Run(self, args):
    region_ref = args.CONCEPTS.region.Parse()
    region = region_ref.AsDict()['locationsId']
    with endpoint_util.AiplatformEndpointOverrides(
        version=constants.BETA_VERSION, region=region):
      algorithm = arg_utils.ChoiceToEnum(
          args.algorithm, client.HpTuningJobsClient.GetAlgorithmEnum())
      response = client.HpTuningJobsClient().Create(
          parent=region_ref.RelativeName(),
          config_path=args.config,
          display_name=args.display_name,
          max_trial_count=args.max_trial_count,
          parallel_trial_count=args.parallel_trial_count,
          algorithm=algorithm)
      log.status.Print(
          constants.HPTUNING_JOB_CREATION_DISPLAY_MESSAGE.format(
              id=hp_tuning_jobs_util.ParseJobName(response.name),
              state=response.state))
      return response
