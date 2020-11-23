# -*- coding: utf-8 -*- #
# Copyright 2017 Google LLC. All Rights Reserved.
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
"""Common command-agnostic utility functions for sql export commands."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


def SqlExportContext(sql_messages,
                     uri,
                     database=None,
                     table=None,
                     offload=False):
  """Generates the ExportContext for the given args, for exporting to SQL.

  Args:
    sql_messages: module, The messages module that should be used.
    uri: The URI of the bucket to export to; the output of the 'uri' arg.
    database: The list of databases to export from; the output of the
      '--database' flag.
    table: The list of tables to export from; the output of the '--table' flag.
    offload: bool, The export offload flag.

  Returns:
    ExportContext, for use in InstancesExportRequest.exportContext.
  """
  return sql_messages.ExportContext(
      kind='sql#exportContext',
      uri=uri,
      databases=database or [],
      offload=offload,
      fileType=sql_messages.ExportContext.FileTypeValueValuesEnum.SQL,
      sqlExportOptions=sql_messages.ExportContext.SqlExportOptionsValue(
          tables=table or []))


def CsvExportContext(sql_messages,
                     uri,
                     database=None,
                     query=None,
                     offload=False):
  """Generates the ExportContext for the given args, for exporting to CSV.

  Args:
    sql_messages: module, The messages module that should be used.
    uri: The URI of the bucket to export to; the output of the 'uri' arg.
    database: The list of databases to export from; the output of the
      '--database' flag.
    query: The query string to use to generate the table; the output of the
      '--query' flag.
    offload: bool, The export offload flag.

  Returns:
    ExportContext, for use in InstancesExportRequest.exportContext.
  """
  return sql_messages.ExportContext(
      kind='sql#exportContext',
      uri=uri,
      databases=database or [],
      offload=offload,
      fileType=sql_messages.ExportContext.FileTypeValueValuesEnum.CSV,
      csvExportOptions=sql_messages.ExportContext.CsvExportOptionsValue(
          selectQuery=query))


def BakExportContext(sql_messages, uri, database):
  """Generates the ExportContext for the given args, for exporting to BAK.

  Args:
    sql_messages: module, The messages module that should be used.
    uri: The URI of the bucket to export to; the output of the 'uri' arg.
    database: The list of databases to export from; the output of the
      '--database' flag.

  Returns:
    ExportContext, for use in InstancesExportRequest.exportContext.
  """
  return sql_messages.ExportContext(
      kind='sql#exportContext',
      uri=uri,
      databases=database,
      fileType=sql_messages.ExportContext.FileTypeValueValuesEnum.BAK)
