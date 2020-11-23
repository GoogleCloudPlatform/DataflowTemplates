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
"""Set-policy command for the Resource Settings CLI."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import exceptions as api_exceptions
from argcomplete import completers
from googlecloudsdk.api_lib.resourcesettings import service
from googlecloudsdk.api_lib.resourcesettings import utils as api_utils
from googlecloudsdk.calliope import base
from googlecloudsdk.command_lib.resource_settings import arguments
from googlecloudsdk.command_lib.resource_settings import exceptions
from googlecloudsdk.command_lib.resource_settings import utils


@base.ReleaseTracks(base.ReleaseTrack.ALPHA)
class SetValue(base.Command):
  r"""Update the value of a resource setting.

  Update the value of a resource setting

  This first converts the contents of the specified file into a setting
  object. It then fetches the current setting using GetSetting. If it does not
  exist, the setting is created using CreateSetting.  If it does, the retrieved
  setting is checked to see if it needs to be updated. If so, the setting is
  updated using UpdateSetting.

  ## EXAMPLES

  To set the setting from the file on the path ``./sample_path'', run:

    $ {command} SETTING_NAME --value-file="./test_input.json"
  """

  @staticmethod
  def Args(parser):
    arguments.AddSettingsNameArgToParser(parser)
    parser.add_argument(
        '--value-file',
        metavar='value-file',
        completer=completers.FilesCompleter,
        required=True,
        help='Path to JSON or YAML file that contains the resource setting.')
    arguments.AddResourceFlagsToParser(parser)
    parser.add_argument(
        '--etag',
        metavar='etag',
        help='Etag of the resource setting.')

  def Run(self, args):
    """Creates or updates a setting from a JSON or YAML file.

    Args:
      args: argparse.Namespace, An object that contains the values for the
        arguments specified in the Args method.

    Returns:
      The created or updated setting.
    """
    settings_service = api_utils.GetServiceFromArgs(args)
    value_service = api_utils.GetValueServiceFromArgs(args)
    settings_message = service.ResourceSettingsMessages()

    input_setting = utils.GetMessageFromFile(
        args.value_file,
        settings_message.GoogleCloudResourcesettingsV1alpha1SettingValue)

    if not input_setting.name:
      raise exceptions.InvalidInputError(
          'Name field not present in the resource setting.')

    setting_name = '{}/value'.format(utils.GetSettingsPathFromArgs(args))
    get_request = api_utils.GetGetValueRequestFromArgs(args, setting_name)

    try:
      setting_value = settings_service.GetValue(get_request)
    except api_exceptions.HttpNotFoundError:
      create_request = api_utils.GetCreateRequestFromArgs(args, input_setting)

      create_response = value_service.Create(create_request)
      return create_response

    if setting_value == input_setting:
      return setting_value

    update_request = api_utils.GetUpdateValueRequestFromArgs(args,
                                                             input_setting)
    update_response = settings_service.UpdateValue(update_request)
    return update_response


