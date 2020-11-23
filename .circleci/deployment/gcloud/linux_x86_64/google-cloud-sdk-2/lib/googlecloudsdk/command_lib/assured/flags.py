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
"""Flags and helpers for the Assured related commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import arg_parsers
from googlecloudsdk.command_lib.assured import resource_args
from googlecloudsdk.command_lib.util.concepts import concept_parsers


def AddListWorkloadsFlags(parser):
  parser.add_argument(
      '--location',
      required=True,
      help=('The location of the Assured Workloads environments. For a '
            'current list of supported LOCATION values, see '
            '[Assured Workloads locations]'
            '(http://cloud/assured-workloads/docs/locations).'))
  parser.add_argument(
      '--organization',
      required=True,
      help=('The parent organization of the Assured Workloads environments, '
            'provided as an organization ID.'))


def AddListOperationsFlags(parser):
  parser.add_argument(
      '--location',
      required=True,
      help=('The location of the Assured Workloads operations. For a '
            'current list of supported LOCATION values, see '
            '[Assured Workloads locations]'
            '(http://cloud/assured-workloads/docs/locations).'))
  parser.add_argument(
      '--organization',
      required=True,
      help=('The parent organization of the Assured Workloads operations, '
            'provided as an organization ID.'))


def AddCreateWorkloadFlags(parser):
  parser.add_argument(
      '--location',
      required=True,
      help=('The location of the new Assured Workloads environment. For a '
            'current list of supported LOCATION values, see '
            '[Assured Workloads locations]'
            '(http://cloud/assured-workloads/docs/locations).'))
  parser.add_argument(
      '--organization',
      required=True,
      help=('The parent organization of the new Assured Workloads environment, '
            'provided as an organization ID'))
  parser.add_argument(
      '--external-identifier',
      help='The external identifier of the new Assured Workloads environment')
  parser.add_argument(
      '--display-name',
      required=True,
      help='The display name of the new Assured Workloads environment')
  parser.add_argument(
      '--compliance-regime',
      required=True,
      choices=['CJIS', 'FEDRAMP_HIGH', 'FEDRAMP_MODERATE', 'IL4'],
      help='The compliance regime of the new Assured Workloads environment')
  parser.add_argument(
      '--billing-account',
      required=True,
      help=('The billing account of the new Assured Workloads environment, for '
            'example, billingAccounts/0000AA-AAA00A-A0A0A0'))
  parser.add_argument(
      '--next-rotation-time',
      required=True,
      help=('The next rotation time of the new Assured Workloads environment, '
            'for example, 2020-12-30T10:15:30.00Z'))
  parser.add_argument(
      '--rotation-period',
      required=True,
      help=('The billing account of the new Assured Workloads environment, '
            'for example, 172800s'))
  parser.add_argument(
      '--labels',
      type=arg_parsers.ArgDict(),
      metavar='KEY=VALUE',
      help=('The labels of the new Assured Workloads environment, for example, '
            'LabelKey1=LabelValue1,LabelKey2=LabelValue2'))
  parser.add_argument(
      '--provisioned-resources-parent',
      help=('The parent of the provisioned projects, for example, '
            'folders/{FOLDER_ID}'))


def AddDeleteWorkloadFlags(parser):
  AddWorkloadResourceArgToParser(parser, verb='delete')
  parser.add_argument(
      '--etag',
      help=('The etag acquired by reading the Assured Workloads environment or '
            'AW "resource".'))


def AddDescribeWorkloadFlags(parser):
  AddWorkloadResourceArgToParser(parser, verb='describe')


def AddUpdateWorkloadFlags(parser):
  AddWorkloadResourceArgToParser(parser, verb='update')
  parser.add_argument(
      '--etag',
      help=('The etag acquired by reading the Assured Workloads environment '
            'before updating.'))
  updatable_fields = parser.add_group(
      required=True,
      help='Settings that can be updated on the Assured Workloads environment.')
  updatable_fields.add_argument(
      '--display-name',
      help='The new display name of the Assured Workloads environment.')
  updatable_fields.add_argument(
      '--labels',
      metavar='KEY=VALUE',
      type=arg_parsers.ArgDict(),
      help=('The new labels of the Assured Workloads environment, for example, '
            'LabelKey1=LabelValue1,LabelKey2=LabelValue2'))


def AddDescribeOperationFlags(parser):
  concept_parsers.ConceptParser.ForResource(
      'operation',
      resource_args.GetOperationResourceSpec(),
      ('The Assured Workloads operation resource to describe.'),
      required=True).AddToParser(parser)


def AddWorkloadResourceArgToParser(parser, verb):
  concept_parsers.ConceptParser.ForResource(
      'workload',
      resource_args.GetWorkloadResourceSpec(),
      ('The Assured Workloads environment resource to {}.'.format(verb)),
      required=True).AddToParser(parser)
