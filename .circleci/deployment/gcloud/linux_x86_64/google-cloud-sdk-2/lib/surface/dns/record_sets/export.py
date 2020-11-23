# -*- coding: utf-8 -*- #
# Copyright 2014 Google LLC. All Rights Reserved.
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
"""gcloud dns record-sets export command."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from apitools.base.py import exceptions as apitools_exceptions
from apitools.base.py import list_pager
from googlecloudsdk.api_lib.dns import export_util
from googlecloudsdk.api_lib.dns import util
from googlecloudsdk.calliope import base
from googlecloudsdk.calliope import exceptions as calliope_exceptions
from googlecloudsdk.command_lib.dns import flags
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.util import files


class Export(base.Command):
  r"""Export your record-sets into a file.

  This command exports the record-sets contained within the specified
  managed-zone into a file.
  The formats you can export to are YAML records format (default) and
  BIND zone file format.

  ## EXAMPLES

  To export record-sets into a yaml file, run:

    $ {command} records.yaml --zone=examplezonename

  To export record-sets into a BIND zone formatted file instead, run:

    $ {command} pathto.zonefile --zone=examplezonename --zone-file-format

  Similarly, to import record-sets into a BIND zone formatted zone file, run:

    $ gcloud dns record-sets import pathto.zonefile --zone-file-format \
      --zone=examplezonename
  """

  @staticmethod
  def Args(parser):
    flags.GetZoneArg().AddToParser(parser)
    parser.add_argument('records_file',
                        help='File to which record-sets should be exported.')
    parser.add_argument(
        '--zone-file-format',
        required=False,
        action='store_true',
        help='Indicates that records-file should be in the zone file format.'
             ' When using this flag, expect the record-set'
             ' to be exported to a BIND zone formatted file. If you omit this '
             'flag, the record-set is exported into a YAML formatted records '
             'file. Note, this format flag determines the format of the '
             'output recorded in the records-file; it is different from the '
             'global `--format` flag which affects console output alone.')

  def Run(self, args):
    api_version = 'v1'
    # If in the future there are differences between API version, do NOT use
    # this patter of checking ReleaseTrack. Break this into multiple classes.
    if self.ReleaseTrack() == base.ReleaseTrack.BETA:
      api_version = 'v1beta2'
    if self.ReleaseTrack() == base.ReleaseTrack.ALPHA:
      api_version = 'v1alpha2'

    dns = util.GetApiClient(api_version)

    # Get the managed-zone.
    zone_ref = util.GetRegistry(api_version).Parse(
        args.zone,
        params={
            'project': properties.VALUES.core.project.GetOrFail,
        },
        collection='dns.managedZones')
    try:
      zone = dns.managedZones.Get(
          dns.MESSAGES_MODULE.DnsManagedZonesGetRequest(
              project=zone_ref.project,
              managedZone=zone_ref.managedZone))
    except apitools_exceptions.HttpError as error:
      raise calliope_exceptions.HttpException(error)

    # Get all the record-sets.
    record_sets = []
    for record_set in list_pager.YieldFromList(
        dns.resourceRecordSets,
        dns.MESSAGES_MODULE.DnsResourceRecordSetsListRequest(
            project=zone_ref.project,
            managedZone=zone_ref.Name()),
        field='rrsets'):
      record_sets.append(record_set)

    # Export the record-sets.
    try:
      with files.FileWriter(args.records_file) as export_file:
        if args.zone_file_format:
          export_util.WriteToZoneFile(export_file, record_sets, zone.dnsName)
        else:
          export_util.WriteToYamlFile(export_file, record_sets)
    except Exception as exp:
      msg = 'Unable to export record-sets to file [{0}]: {1}'.format(
          args.records_file, exp)
      raise export_util.UnableToExportRecordsToFile(msg)

    log.status.Print('Exported record-sets to [{0}].'.format(args.records_file))
