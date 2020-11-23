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
"""Utility for updating Memorystore Redis instances."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import encoding
from googlecloudsdk.command_lib.redis import util
from googlecloudsdk.core import exceptions
from googlecloudsdk.core.console import console_io
from six.moves import filter  # pylint: disable=redefined-builtin


class NoFieldsSpecified(exceptions.Error):
  """Error for calling update command with no args that represent fields."""


def CheckFieldsSpecifiedGA(unused_instance_ref, args, patch_request):
  """Checks if fields to update are registered for GA track."""
  return CheckFieldsSpecifiedCommon(args, patch_request, [])


def CheckFieldsSpecifiedBeta(unused_instance_ref, args, patch_request):
  """Checks if fields to update are registered for BETA track."""
  return CheckFieldsSpecifiedCommon(args, patch_request, ['enable_auth'])


def CheckFieldsSpecifiedAlpha(unused_instance_ref, args, patch_request):
  """Checks if fields to update are registered for ALPHA track."""
  return CheckFieldsSpecifiedCommon(args, patch_request, ['enable_auth'])


def CheckFieldsSpecifiedCommon(args, patch_request, additional_update_args):
  update_args = [
      'clear_labels',
      'display_name',
      'remove_labels',
      'remove_redis_config',
      'size',
      'update_labels',
      'update_redis_config',
  ] + additional_update_args
  if list(filter(args.IsSpecified, update_args)):
    return patch_request
  raise NoFieldsSpecified(
      'Must specify at least one valid instance parameter to update')


def AddFieldToUpdateMask(field, patch_request):
  update_mask = patch_request.updateMask
  if update_mask:
    if update_mask.count(field) == 0:
      patch_request.updateMask = update_mask + ',' + field
  else:
    patch_request.updateMask = field
  return patch_request


def AddDisplayName(unused_instance_ref, args, patch_request):
  if args.IsSpecified('display_name'):
    patch_request.instance.displayName = args.display_name
    patch_request = AddFieldToUpdateMask('display_name', patch_request)
  return patch_request


def _WarnForDestructiveSizeUpdate(instance_ref, instance):
  """Adds prompt that warns about a destructive size update."""
  messages = util.GetMessagesForResource(instance_ref)
  message = 'Change to instance size requested. '
  if instance.tier == messages.Instance.TierValueValuesEnum.BASIC:
    message += ('Scaling a Basic Tier instance will result in a full cache '
                'flush, and the instance will be unavailable during the '
                'operation. ')
  elif instance.tier == messages.Instance.TierValueValuesEnum.STANDARD_HA:
    message += ('Scaling a Standard Tier instance may result in the loss of '
                'unreplicated data, and the instance will be briefly '
                'unavailable during failover. ')
  else:
    # To future proof this against new instance types, add a default message.
    message += ('Scaling a redis instance may result in data loss, and the '
                'instance will be briefly unavailable during scaling. ')
  message += (
      'For more information please take a look at '
      'https://cloud.google.com/memorystore/docs/redis/scaling-instances')

  console_io.PromptContinue(
      message=message,
      prompt_string='Do you want to proceed with update?',
      cancel_on_no=True)


def AddSize(instance_ref, args, patch_request):
  """Python hook to add size update to the redis instance update request."""
  if args.IsSpecified('size'):
    # Changing size is destructive and users should be warned before proceeding.
    _WarnForDestructiveSizeUpdate(instance_ref, patch_request.instance)
    patch_request.instance.memorySizeGb = args.size
    patch_request = AddFieldToUpdateMask('memory_size_gb', patch_request)
  return patch_request


def RemoveRedisConfigs(instance_ref, args, patch_request):
  if not getattr(patch_request.instance, 'redisConfigs', None):
    return patch_request
  if args.IsSpecified('remove_redis_config'):
    config_dict = encoding.MessageToDict(patch_request.instance.redisConfigs)
    for removed_key in args.remove_redis_config:
      config_dict.pop(removed_key, None)
    patch_request = AddNewRedisConfigs(instance_ref, config_dict, patch_request)
  return patch_request


def UpdateRedisConfigs(instance_ref, args, patch_request):
  if args.IsSpecified('update_redis_config'):
    config_dict = {}
    if getattr(patch_request.instance, 'redisConfigs', None):
      config_dict = encoding.MessageToDict(patch_request.instance.redisConfigs)
    config_dict.update(args.update_redis_config)
    patch_request = AddNewRedisConfigs(instance_ref, config_dict, patch_request)
  return patch_request


def AddNewRedisConfigs(instance_ref, redis_configs_dict, patch_request):
  messages = util.GetMessagesForResource(instance_ref)
  new_redis_configs = util.PackageInstanceRedisConfig(redis_configs_dict,
                                                      messages)
  patch_request.instance.redisConfigs = new_redis_configs
  patch_request = AddFieldToUpdateMask('redis_configs', patch_request)
  return patch_request


def UpdateAuthEnabled(unused_instance_ref, args, patch_request):
  """Hook to add auth_enabled to the update mask of the request."""
  if args.IsSpecified('enable_auth'):
    util.WarnOnAuthEnabled(args.enable_auth)
    patch_request = AddFieldToUpdateMask('auth_enabled', patch_request)
  return patch_request
