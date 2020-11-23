#!/usr/bin/env python
# Lint as: python2, python3
# pylint: disable=g-unknown-interpreter
# Copyright 2012 Google Inc. All Rights Reserved.

"""Bigquery Client library for Python."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import abc
import collections
import datetime
import errno
import hashlib
import itertools
import json
import logging
import os
import pkgutil
import random
import re
import shutil
import string
import sys
import tempfile
import textwrap
import time
import traceback
import uuid

# To configure apiclient logging.
from absl import flags
from absl import logging as absl_logging
import googleapiclient
from googleapiclient import discovery
from googleapiclient import http as http_request
from googleapiclient import model
import httplib2
import inflection
import six
from six.moves import map
from six.moves import range
from six.moves import zip
import six.moves.http_client

# pylint: disable=unused-import
import bq_flags



# A unique non-None default, for use in kwargs that need to
# distinguish default from None.
_DEFAULT = object()

# Maximum number of jobs that can be retrieved by ListJobs (sanity limit).
_MAX_RESULTS = 100000



_GCS_SCHEME_PREFIX = 'gs://'


# Maps supported connection type names to the corresponding property in the
# connection proto.
CONNECTION_TYPE_TO_PROPERTY_MAP = {
    'CLOUD_SQL': 'cloudSql',
    'AWS': 'aws',
    'Azure': 'azure',
    'SQL_DATA_SOURCE': 'sqlDataSource',
}
CONNECTION_PROPERTY_TO_TYPE_MAP = {
    p: t for t, p in six.iteritems(CONNECTION_TYPE_TO_PROPERTY_MAP)
}
CONNECTION_TYPES = CONNECTION_TYPE_TO_PROPERTY_MAP.keys()

# Data Transfer Service Authorization Info
AUTHORIZATION_CODE = 'authorization_code'
VERSION_INFO = 'version_info'


def MakeIamRoleIdPropertiesJson(iam_role_id):
  """Returns propeties for a connection with IAM role id.

  Args:
    iam_role_id: IAM role id.

  Returns:
    JSON string with properties to create a connection with IAM role id.
  """

  return '{"crossAccountRole": {"iamRoleId": "%s"}}' % iam_role_id


def MakeTenantIdPropertiesJson(tenant_id):
  """Returns propeties for a connection with tenant id.

  Args:
    tenant_id: tenant id.

  Returns:
    JSON string with properties to create a connection with customer's tenant
    id.
  """

  return '{"customerTenantId": "%s"}' % tenant_id


def MaybePrintManualInstructionsForConnection(connection):
  """Prints follow-up instructions for created or updated connections."""

  if not connection:
    return

  if connection.get('aws') and connection['aws'].get(
      'crossAccountRole'):
    print(('Please add the following identity to your AWS IAM Role \'%s\'\n'
           'IAM user: \'%s\'\n'
           'External Id: \'%s\'\n') %
          (connection['aws']['crossAccountRole'].get('iamRoleId'),
           connection['aws']['crossAccountRole'].get('iamUserId'),
           connection['aws']['crossAccountRole'].get('externalId')))

  if connection.get('azure'):
    print(('Please create a Service Principal in your directory '
           'for appId: \'%s\',\n'
           'and perform role assignment to app: \'%s\' to allow BigQuery '
           'to access your Azure data. \n') %
          (connection['azure'].get('clientId'),
           connection['azure'].get('application')))



def _Typecheck(obj, types, message=None, method=None):
  if not isinstance(obj, types):
    if not message:
      if method:
        message = 'Invalid reference for %s: %r' % (method, obj)
      else:
        message = 'Type of %r is not one of %s' % (obj, types)
    raise TypeError(message)


def _ToLowerCamel(name):
  """Convert a name with underscores to camelcase."""
  return re.sub('_[a-z]', lambda match: match.group(0)[1].upper(), name)


def _ToFilename(url):
  """Converts a url to a filename."""
  return ''.join([c for c in url if c in string.ascii_lowercase])


def _ApplyParameters(config, **kwds):
  """Adds all kwds to config dict, adjusting keys to camelcase.

  Note this does not remove entries that are set to None, however.

  kwds: A dict of keys and values to set in the config.

  Args:
    config: A configuration dict.
  """
  config.update(
      (_ToLowerCamel(k), v) for k, v in six.iteritems(kwds) if v is not None)


def _OverwriteCurrentLine(s, previous_token=None):
  """Print string over the current terminal line, and stay on that line.

  The full width of any previous output (by the token) will be wiped clean.
  If multiple callers call this at the same time, it would be bad.

  Args:
    s: string to print.  May not contain newlines.
    previous_token: token returned from previous call, or None on first call.

  Returns:
    a token to pass into your next call to this function.
  """
  # Tricks in use:
  # carriage return \r brings the printhead back to the start of the line.
  # sys.stdout.write() does not add a newline.

  # Erase any previous, in case new string is shorter.
  if previous_token is not None:
    sys.stderr.write('\r' + (' ' * previous_token))
  # Put new string.
  sys.stderr.write('\r' + s)
  # Display.
  sys.stderr.flush()
  return len(s)


def _FormatLabels(labels):
  """Format a resource's labels for printing."""
  result_lines = []
  for key, value in six.iteritems(labels):
    label_str = '%s:%s' % (key, value)
    result_lines.extend([label_str])
  return '\n'.join(result_lines)


def _FormatStandardSqlFields(standard_sql_fields):
  """Returns a string with standard_sql_fields.

  Currently only supports printing primitive field types and repeated fields.
  Args:
    standard_sql_fields: A list of standard sql fields.

  Returns:
    The formatted standard sql fields.
  """
  lines = []
  for field in standard_sql_fields:
    if field['type']['typeKind'] == 'ARRAY':
      field_type = field['type']['arrayElementType']['typeKind']
    else:
      field_type = field['type']['typeKind']
    entry = '|- %s: %s' % (field['name'], field_type.lower())
    if field['type']['typeKind'] == 'ARRAY':
      entry += ' (repeated)'
    lines.extend([entry])
  return '\n'.join(lines)


def _FormatProjectIdentifierForTransfers(project_reference, location):
  """Formats a project identifier for data transfers.

  Data transfer API calls take in the format projects/(projectName), so because
  by default project IDs take the format (projectName), add the beginning format
  to perform data transfer commands

  Args:
    project_reference: The project id to format for data transfer commands.
    location: The location id, e.g. 'us' or 'eu'.

  Returns:
    The formatted project name for transfers.
  """

  return 'projects/' + project_reference.projectId + '/locations/' + location




def _ParseJobIdentifier(identifier):
  """Parses a job identifier string into its components.

  Args:
    identifier: String specifying the job identifier in the format
      "project_id:job_id", "project_id:location.job_id", or "job_id".

  Returns:
    A tuple of three elements: containing project_id, location,
    job_id. If an element is not found, it is represented by
    None. If no elements are found, the tuple contains three None
    values.
  """
  project_id_pattern = r'[\w:\-.]*[\w:\-]+'
  location_pattern = r'[a-zA-Z\-0-9]+'
  job_id_pattern = r'[\w\-]+'

  pattern = re.compile(
      r"""
    ^((?P<project_id>%(PROJECT_ID)s)
    :)?
    ((?P<location>%(LOCATION)s)
    \.)?
    (?P<job_id>%(JOB_ID)s)
    $
  """ % {
      'PROJECT_ID': project_id_pattern,
      'LOCATION': location_pattern,
      'JOB_ID': job_id_pattern
  }, re.X)

  match = re.search(pattern, identifier)
  if match:
    return (match.groupdict().get('project_id', None),
            match.groupdict().get('location', None),
            match.groupdict().get('job_id', None))
  return (None, None, None)


def _ParseReservationIdentifier(identifier):
  """Parses the reservation identifier string into its components.

  Args:
    identifier: String specifying the reservation identifier in the format
    "project_id:reservation_id", "project_id:location.reservation_id", or
    "reservation_id".

  Returns:
    A tuple of three elements: containing project_id, location, and
    reservation_id. If an element is not found, it is represented by None.

  Raises:
    BigqueryError: if the identifier could not be parsed.
  """

  pattern = re.compile(
      r"""
  ^((?P<project_id>[\w:\-.]*[\w:\-]+):)?
  ((?P<location>[\w\-]+)\.)?
  (?P<reservation_id>[\w\-]*)$
  """, re.X)

  match = re.search(pattern, identifier)
  if not match:
    raise BigqueryError(
        'Could not parse reservation identifier: %s' % identifier)

  project_id = match.groupdict().get('project_id', None)
  location = match.groupdict().get('location', None)
  reservation_id = match.groupdict().get('reservation_id', None)
  return (project_id, location, reservation_id)


def _ParseReservationPath(path):
  """Parses the reservation path string into its components.

  Args:
    path: String specifying the reservation path in the format
      projects/<project_id>/locations/<location>/reservations/<reservation_id>

  Returns:
    A tuple of three elements: containing project_id, location and
    reservation_id. If an element is not found, it is represented by None.

  Raises:
    BigqueryError: if the path could not be parsed.
  """

  pattern = re.compile(
      r"""
  ^projects\/(?P<project_id>[\w:\-.]*[\w:\-]+)?
  \/locations\/(?P<location>[\w\-]+)?
  \/reservations\/(?P<reservation_id>[\w\-\/]+)$
  """, re.X)

  match = re.search(pattern, path)
  if not match:
    raise BigqueryError('Could not parse reservation path: %s' % path)

  project_id = match.groupdict().get('project_id', None)
  location = match.groupdict().get('location', None)
  reservation_id = match.groupdict().get('reservation_id', None)
  return (project_id, location, reservation_id)


def _ParseCapacityCommitmentIdentifier(identifier, allow_commas):
  """Parses the capacity commitment identifier string into its components.

  Args:
    identifier: String specifying the capacity commitment identifier in the
      format
    "project_id:capacity_commitment_id",
      "project_id:location.capacity_commitment_id", or "capacity_commitment_id".
    allow_commas: whether to allow commas in the capacity commitment id.

  Returns:
    A tuple of three elements: containing project_id, location
    and capacity_commitment_id. If an element is not found, it is represented by
    None.

  Raises:
    BigqueryError: if the identifier could not be parsed.
  """
  pattern = None
  if allow_commas:
    pattern = re.compile(
        r"""
    ^((?P<project_id>[\w:\-.]*[\w:\-]+):)?
    ((?P<location>[\w\-]+)\.)?
    (?P<capacity_commitment_id>[\w|,]*)$
    """, re.X)
  else:
    pattern = re.compile(
        r"""
    ^((?P<project_id>[\w:\-.]*[\w:\-]+):)?
    ((?P<location>[\w\-]+)\.)?
    (?P<capacity_commitment_id>[\w]*)$
    """, re.X)

  match = re.search(pattern, identifier)
  if not match:
    raise BigqueryError('Could not parse capacity commitment identifier: %s' %
                        identifier)

  project_id = match.groupdict().get('project_id', None)
  location = match.groupdict().get('location', None)
  capacity_commitment_id = match.groupdict().get('capacity_commitment_id', None)
  return (project_id, location, capacity_commitment_id)


def _ParseCapacityCommitmentPath(path):
  """Parses the capacity commitment path string into its components.

  Args:
    path: String specifying the capacity commitment path in the format
      projects/<project_id>/locations/<location>/capacityCommitments/<capacity_commitment_id>

  Returns:
    A tuple of three elements: containing project_id, location,
    and capacity_commitment_id. If an element is not found, it is represented by
    None.

  Raises:
    BigqueryError: if the path could not be parsed.
  """
  pattern = re.compile(
      r"""
  ^projects\/(?P<project_id>[\w:\-.]*[\w:\-]+)?
  \/locations\/(?P<location>[\w\-]+)?
  \/capacityCommitments\/(?P<capacity_commitment_id>[\w]+)$
  """, re.X)

  match = re.search(pattern, path)
  if not match:
    raise BigqueryError('Could not parse capacity commitment path: %s' % path)

  project_id = match.groupdict().get('project_id', None)
  location = match.groupdict().get('location', None)
  capacity_commitment_id = match.groupdict().get('capacity_commitment_id', None)
  return (project_id, location, capacity_commitment_id)


def _ParseReservationAssignmentIdentifier(identifier):
  """Parses the reservation assignment identifier string into its components.

  Args:
    identifier: String specifying the reservation assignment identifier in the
      format
    "project_id:reservation_id.assignment_id",
    "project_id:location.reservation_id.assignment_id", or
      "reservation_id.assignment_id".

  Returns:
    A tuple of three elements: containing project_id, location, and
    reservation_assignment_id. If an element is not found, it is represented by
    None.

  Raises:
    BigqueryError: if the identifier could not be parsed.
  """

  pattern = re.compile(
      r"""
  ^((?P<project_id>[\w:\-.]*[\w:\-]+):)?
  ((?P<location>[\w\-]+)\.)?
  (?P<reservation_id>[\w\-\/]+)\.
  (?P<reservation_assignment_id>[\w\-_]+)$
  """, re.X)

  match = re.search(pattern, identifier)
  if not match:
    raise BigqueryError(
        'Could not parse reservation assignment identifier: %s' % identifier)

  project_id = match.groupdict().get('project_id', None)
  location = match.groupdict().get('location', None)
  reservation_id = match.groupdict().get('reservation_id', None)
  reservation_assignment_id = match.groupdict().get('reservation_assignment_id',
                                                    None)
  return (project_id, location, reservation_id, reservation_assignment_id)


def _ParseReservationAssignmentPath(path):
  """Parses the reservation assignment path string into its components.

  Args:
    path: String specifying the reservation assignment path in the format
      projects/<project_id>/locations/<location>/
      reservations/<reservation_id>/assignments/<assignment_id> The
      reservation_id must be that of a top level reservation.

  Returns:
    A tuple of three elements: containing project_id, location and
    reservation_assignment_id. If an element is not found, it is represented by
    None.

  Raises:
    BigqueryError: if the path could not be parsed.
  """

  pattern = re.compile(
      r"""
  ^projects\/(?P<project_id>[\w:\-.]*[\w:\-]+)?
  \/locations\/(?P<location>[\w\-]+)?
  \/reservations\/(?P<reservation_id>[\w\-]+)
  \/assignments\/(?P<reservation_assignment_id>[\w\-_]+)$
  """, re.X)

  match = re.search(pattern, path)
  if not match:
    raise BigqueryError('Could not parse reservation assignment path: %s' %
                        path)

  project_id = match.groupdict().get('project_id', None)
  location = match.groupdict().get('location', None)
  reservation_id = match.groupdict().get('reservation_id', None)
  reservation_assignment_id = match.groupdict().get('reservation_assignment_id',
                                                    None)
  return (project_id, location, reservation_id, reservation_assignment_id)


def _ParseConnectionIdentifier(identifier):
  """Parses the connection identifier string into its components.

  Args:
    identifier: String specifying the connection identifier in the format
    "connection_id",
    "location.connection_id",
    "project_id.location.connection_id"

  Returns:
    A tuple of four elements: containing project_id, location, connection_id
    If an element is not found, it is represented by None.

  Raises:
    BigqueryError: if the identifier could not be parsed.
  """

  if not identifier:
    raise BigqueryError('Empty connection identifier')

  tokens = identifier.split('.')
  num_tokens = len(tokens)
  if num_tokens > 4:
    raise BigqueryError(
        'Could not parse connection identifier: %s' % identifier)
  connection_id = tokens[num_tokens-1]
  location = tokens[num_tokens-2] if num_tokens > 1 else None
  project_id = '.'.join(tokens[:num_tokens-2]) if num_tokens > 2 else None

  return (project_id, location, connection_id)


def _ParseConnectionPath(path):
  """Parses the connection path string into its components.

  Args:
    path: String specifying the connection path in the format
      projects/<project_id>/locations/<location>/connections/<connection_id>

  Returns:
    A tuple of three elements: containing project_id, location and
    connection_id. If an element is not found, it is represented by None.

  Raises:
    BigqueryError: if the path could not be parsed.
  """

  pattern = re.compile(
      r"""
  ^projects\/(?P<project_id>[\w:\-.]*[\w:\-]+)?
  \/locations\/(?P<location>[\w\-]+)?
  \/connections\/(?P<connection_id>[\w\-\/]+)$
  """, re.X)

  match = re.search(pattern, path)
  if not match:
    raise BigqueryError('Could not parse connection path: %s' % path)

  project_id = match.groupdict().get('project_id', None)
  location = match.groupdict().get('location', None)
  connection_id = match.groupdict().get('connection_id', None)
  return (project_id, location, connection_id)


def ConfigurePythonLogger(apilog=None):
  """Sets up Python logger, which BigqueryClient logs with.

  Applications can configure logging however they want, but this
  captures one pattern of logging which seems useful when dealing with
  a single command line option for determining logging.

  Args:
    apilog: To log to sys.stdout, specify '', '-', '1', 'true', or
      'stdout'. To log to sys.stderr, specify 'stderr'. To log to a
      file, specify the file path. Specify None to disable logging.
  """
  if apilog is None:
    # Effectively turn off logging.
    logging.disable(logging.CRITICAL)
  else:
    if apilog in ('', '-', '1', 'true', 'stdout'):
      _SetLogFile(sys.stdout)
    elif apilog == 'stderr':
      _SetLogFile(sys.stderr)
    elif apilog:
      _SetLogFile(open(apilog, 'w'))
    else:
      logging.basicConfig(level=logging.INFO)
    # Turn on apiclient logging of http requests and responses. (Here
    # we handle both the flags interface from apiclient < 1.2 and the
    # module global in apiclient >= 1.2.)
    if hasattr(flags.FLAGS, 'dump_request_response'):
      flags.FLAGS.dump_request_response = True
    else:
      model.dump_request_response = True


def _SetLogFile(logfile):
  absl_logging.use_python_logging(quiet=True)
  absl_logging.get_absl_handler().python_handler.stream = logfile

InsertEntry = collections.namedtuple('InsertEntry',
                                     ['insert_id', 'record'])


def JsonToInsertEntry(insert_id, json_string):
  """Parses a JSON encoded record and returns an InsertEntry.

  Arguments:
    insert_id: Id for the insert, can be None.
    json_string: The JSON encoded data to be converted.
  Returns:
    InsertEntry object for adding to a table.
  """
  try:
    row = json.loads(json_string)
    if not isinstance(row, dict):
      raise BigqueryClientError('Value is not a JSON object')
    return InsertEntry(insert_id, row)
  except ValueError as e:
    raise BigqueryClientError('Could not parse object: %s' % (str(e),))


def EncodeForPrinting(o):
  """Safely encode an object as the encoding for sys.stdout."""
  # Not all file objects provide an encoding attribute, so we make sure to
  # handle the case where the attribute is completely absent.
  encoding = getattr(sys.stdout, 'encoding', None) or 'ascii'
  # We want to prevent conflicts in python2 between formatting
  # a str type with a unicode type, e.g. b'abc%s' % (u'[unicode]',)
  # where the byte type will be auto decoded as ascii thus causing
  # an error.
  # Thus we only want to encode the object if it's passed in as a
  # unicode type and the unicode type is not a str type.
  if isinstance(o, type(u'')) and not isinstance(o, str):
    return o.encode(encoding, 'backslashreplace')
  else:
    return str(o)


class BigqueryError(Exception):

  @staticmethod
  def Create(error, server_error, error_ls, job_ref=None):
    """Returns a BigqueryError for json error embedded in server_error.

    If error_ls contains any errors other than the given one, those
    are also included in the returned message.

    Args:
      error: The primary error to convert.
      server_error: The error returned by the server. (This is only used
        in the case that error is malformed.)
      error_ls: Additional errors to include in the error message.
      job_ref: JobReference, if this is an error associated with a job.

    Returns:
      BigqueryError representing error.
    """
    reason = error.get('reason')
    if job_ref:
      message = 'Error processing %r: %s' % (job_ref, error.get('message'))
    else:
      message = error.get('message', '')
    # We don't want to repeat the "main" error message.
    new_errors = [err for err in error_ls if err != error]
    if new_errors:
      message += '\nFailure details:\n'
    wrap_error_message = True
    new_error_messages = [
        ': '.join(filter(
            None, [err.get('location'), err.get('message')]))
        for err in new_errors
    ]
    if wrap_error_message:
      message += '\n'.join(
          textwrap.fill(msg, initial_indent=' - ', subsequent_indent='   ')
          for msg in new_error_messages)
    else:
      error_message = '\n'.join(new_error_messages)
      if error_message:
        message += '- ' + error_message

    # Sometimes we will have type(message) being <type 'unicode'>, for example
    # from an invalid query containing a non-English string.  Reduce this
    # to <type 'string'> now -- otherwise it's a trap for any code that
    # tries to %s-format the exception later: str() uses 'ascii' codec.
    # And the message is for display only, so this shouldn't confuse other code.
    message = EncodeForPrinting(message)

    if not reason or not message:
      return BigqueryInterfaceError(
          'Error reported by server with missing error fields. '
          'Server returned: %s' % (str(server_error),))
    if reason == 'notFound':
      return BigqueryNotFoundError(message, error, error_ls, job_ref=job_ref)
    if reason == 'duplicate':
      return BigqueryDuplicateError(message, error, error_ls, job_ref=job_ref)
    if reason == 'accessDenied':
      return BigqueryAccessDeniedError(
          message, error, error_ls, job_ref=job_ref)
    if reason == 'invalidQuery':
      return BigqueryInvalidQueryError(
          message, error, error_ls, job_ref=job_ref)
    if reason == 'termsOfServiceNotAccepted':
      return BigqueryTermsOfServiceError(
          message, error, error_ls, job_ref=job_ref)
    if reason == 'backendError':
      return BigqueryBackendError(
          message, error, error_ls, job_ref=job_ref)
    # We map the remaining errors to BigqueryServiceError.
    return BigqueryServiceError(message, error, error_ls, job_ref=job_ref)


class BigqueryCommunicationError(BigqueryError):
  """Error communicating with the server."""
  pass


class BigqueryInterfaceError(BigqueryError):
  """Response from server missing required fields."""
  pass


class BigqueryServiceError(BigqueryError):
  """Base class of Bigquery-specific error responses.

  The BigQuery server received request and returned an error.
  """

  def __init__(self, message, error, error_list, job_ref=None,
               *args, **kwds):
    """Initializes a BigqueryServiceError.

    Args:
      message: A user-facing error message.
      error: The error dictionary, code may inspect the 'reason' key.
      error_list: A list of additional entries, for example a load job
        may contain multiple errors here for each error encountered
        during processing.
      job_ref: Optional JobReference, if this error was encountered
        while processing a job.
    """
    super(BigqueryServiceError, self).__init__(message, *args, **kwds)
    self.error = error
    self.error_list = error_list
    self.job_ref = job_ref

  def __repr__(self):
    return '%s: error=%s, error_list=%s, job_ref=%s' % (
        self.__class__.__name__, self.error, self.error_list, self.job_ref)


class BigqueryNotFoundError(BigqueryServiceError):
  """The requested resource or identifier was not found."""
  pass


class BigqueryDuplicateError(BigqueryServiceError):
  """The requested resource or identifier already exists."""
  pass


class BigqueryAccessDeniedError(BigqueryServiceError):
  """The user does not have access to the requested resource."""
  pass


class BigqueryInvalidQueryError(BigqueryServiceError):
  """The SQL statement is invalid."""
  pass


class BigqueryTermsOfServiceError(BigqueryAccessDeniedError):
  """User has not ACK'd ToS."""
  pass


class BigqueryBackendError(BigqueryServiceError):
  """A backend error typically corresponding to retriable HTTP 5xx failures."""
  pass


class BigqueryClientError(BigqueryError):
  """Invalid use of BigqueryClient."""
  pass


class BigqueryClientConfigurationError(BigqueryClientError):
  """Invalid configuration of BigqueryClient."""
  pass


class BigquerySchemaError(BigqueryClientError):
  """Error in locating or parsing the schema."""
  pass


class BigqueryModel(model.JsonModel):
  """Adds optional global parameters to all requests."""

  def __init__(
      self,
      trace=None,
      **kwds):
    super(BigqueryModel, self).__init__(**kwds)
    self.trace = trace

  # pylint: disable=g-bad-name
  def request(self, headers, path_params, query_params, body_value):
    """Updates outgoing request."""
    if 'trace' not in query_params and self.trace:
      query_params['trace'] = self.trace
    return super(BigqueryModel, self).request(
        headers, path_params, query_params, body_value)
  # pylint: enable=g-bad-name

  # pylint: disable=g-bad-name
  def response(self, resp, content):
    """Convert the response wire format into a Python object."""
    return super(BigqueryModel, self).response(
        resp, content)
  # pylint: enable=g-bad-name


class BigqueryHttp(http_request.HttpRequest):
  """Converts errors into Bigquery errors."""

  def __init__(self, bigquery_model, *args, **kwds):
    super(BigqueryHttp, self).__init__(*args, **kwds)
    self._model = bigquery_model

  @staticmethod
  def Factory(
      bigquery_model,
  ):
    """Returns a function that creates a BigqueryHttp with the given model."""

    def _Construct(*args, **kwds):
      captured_model = bigquery_model
      return BigqueryHttp(captured_model, *args, **kwds)
    return _Construct

  @staticmethod
  def RaiseErrorFromHttpError(e):
    """Raises a BigQueryError given an HttpError."""
    if e.resp.get('content-type', '').startswith('application/json'):
      content = json.loads(e.content.decode('utf-8'))
      BigqueryClient.RaiseError(content)
    else:
      # If the HttpError is not a json object, it is a communication error.
      raise BigqueryCommunicationError(
          ('Could not connect with BigQuery server.\n'
           'Http response status: %s\n'
           'Http response content:\n%s') % (
               e.resp.get('status', '(unexpected)'), e.content))

  @staticmethod
  def RaiseErrorFromNonHttpError(e):
    """Raises a BigQueryError given a non-HttpError."""
    raise BigqueryCommunicationError(
        'Could not connect with BigQuery server due to: %r' % (e,))

  def execute(self, **kwds):  # pylint: disable=g-bad-name
    try:
      return super(BigqueryHttp, self).execute(**kwds)
    except googleapiclient.errors.HttpError as e:
      # TODO(user): Remove this when apiclient supports logging
      # of error responses.
      self._model._log_response(e.resp, e.content)  # pylint: disable=protected-access
      BigqueryHttp.RaiseErrorFromHttpError(e)
    except (httplib2.HttpLib2Error, IOError) as e:
      BigqueryHttp.RaiseErrorFromNonHttpError(e)


class JobIdGenerator(six.with_metaclass(abc.ABCMeta, object)):
  """Base class for job id generators."""

  def __init__(self):
    pass

  @abc.abstractmethod
  def Generate(self, job_configuration):
    """Generates a job_id to use for job_configuration."""


class JobIdGeneratorNone(JobIdGenerator):
  """Job id generator that returns None, letting the server pick the job id."""

  def Generate(self, unused_config):
    return None


class JobIdGeneratorRandom(JobIdGenerator):
  """Generates random job ids."""

  def Generate(self, unused_config):
    return 'bqjob_r%08x_%016x' % (random.SystemRandom().randint(
        0, sys.maxsize), int(time.time() * 1000))


class JobIdGeneratorFingerprint(JobIdGenerator):
  """Generates job ids that uniquely match the job config."""

  def _Hash(self, config, sha1):
    """Computes the sha1 hash of a dict."""
    keys = list(config.keys())
    # Python dict enumeration ordering is random. Sort the keys
    # so that we will visit them in a stable order.
    keys.sort()
    for key in keys:
      sha1.update('%s' % (key,))
      v = config[key]
      if isinstance(v, dict):
        logging.info('Hashing: %s...', key)
        self._Hash(v, sha1)
      elif isinstance(v, list):
        logging.info('Hashing: %s ...', key)
        for inner_v in v:
          self._Hash(inner_v, sha1)
      else:
        logging.info('Hashing: %s:%s', key, v)
        sha1.update('%s' % (v,))

  def Generate(self, config):
    s1 = hashlib.sha1()
    self._Hash(config, s1)
    job_id = 'bqjob_c%s' % (s1.hexdigest(),)
    logging.info('Fingerprinting: %s:\n%s', config, job_id)
    return job_id


class JobIdGeneratorIncrementing(JobIdGenerator):
  """Generates job ids that increment each time we're asked."""

  def __init__(self, inner):
    super(JobIdGeneratorIncrementing, self).__init__()
    self._inner = inner
    self._retry = 0

  def Generate(self, config):
    self._retry += 1
    return '%s_%d' % (self._inner.Generate(config), self._retry)


class TransferScheduleArgs(object):
  """Arguments to customize data transfer schedule."""

  def __init__(self,
               schedule=None,
               start_time=None,
               end_time=None,
               disable_auto_scheduling=False):
    self.schedule = schedule
    self.start_time = start_time
    self.end_time = end_time
    self.disable_auto_scheduling = disable_auto_scheduling

  def ToScheduleOptionsPayload(self, options_to_copy=None):
    """Returns a dictionary of schedule options.

    Args:
      options_to_copy: Existing options to be copied.

    Returns:
      A dictionary of schedule options expected by the
      bigquery.transfers.create and bigquery.transfers.update API methods.
    """

    # Copy the current options or start with an empty dictionary.
    options = dict(options_to_copy or {})

    if self.start_time is not None:
      options['startTime'] = self._TimeOrInfitity(self.start_time)
    if self.end_time is not None:
      options['endTime'] = self._TimeOrInfitity(self.end_time)

    options['disableAutoScheduling'] = self.disable_auto_scheduling

    return options

  def _TimeOrInfitity(self, time_str):
    """Returns None to indicate Inifinity, if time_str is an empty string."""
    return time_str or None


class BigqueryClient(object):
  """Class encapsulating interaction with the BigQuery service."""

  def __init__(self, **kwds):
    """Initializes BigqueryClient.

    Required keywords:
      api: the api to connect to, for example "bigquery".
      api_version: the version of the api to connect to, for example "v2".

    Optional keywords:
      project_id: a default project id to use. While not required for
        initialization, a project_id is required when calling any
        method that creates a job on the server. Methods that have
        this requirement pass through **kwds, and will raise
        BigqueryClientConfigurationError if no project_id can be
        found.
      dataset_id: a default dataset id to use.
      discovery_document: the discovery document to use. If None, one
        will be retrieved from the discovery api. If not specified,
        the built-in discovery document will be used.
      job_property: a list of "key=value" strings defining properties
        to apply to all job operations.
      trace: a tracing header to inclue in all bigquery api requests.
      sync: boolean, when inserting jobs, whether to wait for them to
        complete before returning from the insert request.
      wait_printer_factory: a function that returns a WaitPrinter.
        This will be called for each job that we wait on. See WaitJob().

    Raises:
      ValueError: if keywords are missing or incorrectly specified.
    """
    super(BigqueryClient, self).__init__()
    for key, value in six.iteritems(kwds):
      setattr(self, key, value)
    self._apiclient = None
    self._routines_apiclient = None
    self._op_transfer_client = None
    self._op_reservation_client = None
    self._op_bi_reservation_client = None
    self._models_apiclient = None
    self._op_connection_service_client = None
    self._iam_policy_apiclient = None
    for required_flag in ('api', 'api_version'):
      if required_flag not in kwds:
        raise ValueError('Missing required flag: %s' % (required_flag,))
    default_flag_values = {
        'project_id': '',
        'dataset_id': '',
        'discovery_document': _DEFAULT,
        'iam_policy_discovery_document': _DEFAULT,
        'job_property': '',
        'trace': None,
        'sync': True,
        'wait_printer_factory': BigqueryClient.TransitionWaitPrinter,
        'job_id_generator': JobIdGeneratorIncrementing(JobIdGeneratorRandom()),
        'max_rows_per_request': None,
        'transfer_path': None,
        'connection_service_path': None,
    }
    for flagname, default in six.iteritems(default_flag_values):
      if not hasattr(self, flagname):
        setattr(self, flagname, default)

  columns_to_include_for_transfer_run = [
      'updateTime', 'schedule', 'runTime', 'scheduleTime', 'params', 'endTime',
      'dataSourceId', 'destinationDatasetId', 'state', 'startTime', 'name'
  ]

  # These columns appear to be empty with scheduling a new transfer run
  # so there are listed as excluded from the transfer run output.
  columns_excluded_for_make_transfer_run = ['schedule', 'endTime', 'startTime']


  def GetHttp(self):
    """Returns the httplib2 Http to use."""

    proxy_info = httplib2.proxy_info_from_environment
    if flags.FLAGS.proxy_address and flags.FLAGS.proxy_port:
      try:
        port = int(flags.FLAGS.proxy_port)
      except ValueError:
        raise ValueError('Invalid value for proxy_port: {}'
                         .format(flags.FLAGS.proxy_port))
      proxy_info = httplib2.ProxyInfo(
          proxy_type=3,
          proxy_host=flags.FLAGS.proxy_address,
          proxy_port=port,
          proxy_user=flags.FLAGS.proxy_username or None,
          proxy_pass=flags.FLAGS.proxy_password or None)

    http = httplib2.Http(
        proxy_info=proxy_info,
        ca_certs=flags.FLAGS.ca_certificates_file or None,
        disable_ssl_certificate_validation=flags.FLAGS.disable_ssl_validation
    )

    # This asks httplib2 to exclude 308s from the status codes
    # it treats as redirects.
    if hasattr(http, 'redirect_codes'):
      http.redirect_codes = http.redirect_codes - {308}


    return http


  def GetDiscoveryUrl(self):
    """Returns the url to the discovery document for bigquery."""
    discovery_url = self.api + '/discovery/v1/apis/{api}/{apiVersion}/rest'
    return discovery_url

  def GetAuthorizedHttp(self, credentials, http):
    """Returns an http client that is authorized with the given credentials."""
    return credentials.authorize(http)

  def BuildApiClient(
      self,
      discovery_url=None,
  ):
    """Build and return BigQuery Dynamic client from discovery document."""
    http = self.GetAuthorizedHttp(self.credentials, self.GetHttp())
    bigquery_model = BigqueryModel(
        trace=self.trace)
    bigquery_http = BigqueryHttp.Factory(
        bigquery_model,
    )
    discovery_document = None
    if discovery_document == _DEFAULT:
      # Use the api description packed with this client, if one exists.
      try:
        discovery_document = pkgutil.get_data(
            'bigquery_client', 'discovery/%s.bigquery.%s.rest.json'
            % (_ToFilename(self.api), self.api_version))
      except IOError:
        discovery_document = None
    if discovery_document is None:
      # Attempt to retrieve discovery doc with retry logic for transient,
      # retry-able errors.
      max_retries = 3
      iterations = 0
      while iterations < max_retries and discovery_document is None:
        if iterations > 0:
          # Wait briefly before retrying with exponentially increasing wait.
          time.sleep(2 ** iterations)
        iterations += 1
        try:
          if discovery_url is None:
            discovery_url = self.GetDiscoveryUrl().format(
                api='bigquery', apiVersion=self.api_version)
          logging.info('Requesting discovery document from %s', discovery_url)
          response_metadata, discovery_document = http.request(discovery_url)
          discovery_document = discovery_document.decode('utf-8')
          if int(response_metadata.get('status')) >= 400:
            msg = 'Got %s response from discovery url: %s' % (
                response_metadata.get('status'), discovery_url)
            logging.error('%s:\n%s', msg, discovery_document)
            raise BigqueryCommunicationError(msg)
        except (httplib2.HttpLib2Error, googleapiclient.errors.HttpError,
                six.moves.http_client.HTTPException) as e:
          # We can't find the specified server. This can be thrown for
          # multiple reasons, so inspect the error.
          if hasattr(e, 'content'):
            if iterations == max_retries:
              raise BigqueryCommunicationError(
                  'Cannot contact server. Please try again.\nError: %r'
                  '\nContent: %s' % (e, e.content))
          else:
            if iterations == max_retries:
              raise BigqueryCommunicationError(
                  'Cannot contact server. Please try again.\n'
                  'Traceback: %s' % (traceback.format_exc(),))
        except IOError as e:
          if iterations == max_retries:
            raise BigqueryCommunicationError(
                'Cannot contact server. Please try again.\nError: %r' % (e,))
        except googleapiclient.errors.UnknownApiNameOrVersion as e:
          # We can't resolve the discovery url for the given server.
          # Don't retry in this case.
          raise BigqueryCommunicationError(
              'Invalid API name or version: %s' % (str(e),))
    try:
      built_client = discovery.build_from_document(
          discovery_document,
          http=http,
          model=bigquery_model,
          requestBuilder=bigquery_http)
      return built_client
    except Exception:
      logging.error('Error building from discovery document: %s',
                    discovery_document)
      raise

  def BuildDiscoveryNextApiClient(self):
    """Builds and returns BigQuery API client from discovery_next document."""
    http = self.GetAuthorizedHttp(self.credentials, self.GetHttp())
    bigquery_model = BigqueryModel(
        trace=self.trace)
    bigquery_http = BigqueryHttp.Factory(
        bigquery_model,
    )
    models_discovery_document = None
    try:
      models_discovery_document = pkgutil.get_data(
          'bigquery_client',
          'discovery_next/bigquery.json')
    except IOError:
      logging.error('Failed to build discovery_next document')
      raise
    try:
      return discovery.build_from_document(
          models_discovery_document,
          http=http,
          model=bigquery_model,
          requestBuilder=bigquery_http)
    except Exception:
      logging.error('Error building from models document: %s',
                    models_discovery_document)
      raise

  def BuildIAMPolicyApiClient(self):
    """Builds and returns IAM policy API client from discovery document."""
    http = self.GetAuthorizedHttp(self.credentials, self.GetHttp())
    bigquery_model = BigqueryModel(
        trace=self.trace)
    bigquery_http = BigqueryHttp.Factory(
        bigquery_model,
    )
    iam_policy_discovery_document = None
    try:
      iam_policy_discovery_document = pkgutil.get_data(
          'bigquery_client',
          'discovery_next/iam-policy.json')
    except IOError:
      logging.error('Failed to build iam policy discovery document')
      raise
    try:
      return discovery.build_from_document(
          iam_policy_discovery_document,
          http=http,
          model=bigquery_model,
          requestBuilder=bigquery_http)
    except Exception:
      logging.error('Error building from iam policy document: %s',
                    iam_policy_discovery_document)
      raise


  @property
  def apiclient(self):
    """Returns the apiclient attached to self."""
    if self._apiclient is None:
      self._apiclient = self.BuildApiClient()
    return self._apiclient

  def GetModelsApiClient(self):
    """Returns the apiclient attached to self."""
    if self._models_apiclient is None:
      self._models_apiclient = self.BuildDiscoveryNextApiClient()
    return self._models_apiclient

  def GetRoutinesApiClient(self):
    """Return the apiclient attached to self."""
    if self._routines_apiclient is None:
      self._routines_apiclient = self.BuildDiscoveryNextApiClient()
    return self._routines_apiclient

  def GetIAMPolicyApiClient(self):
    """Return the apiclient attached to self."""
    if self._iam_policy_apiclient is None:
      self._iam_policy_apiclient = self.BuildIAMPolicyApiClient()
    return self._iam_policy_apiclient

  def GetInsertApiClient(self):
    """Return the apiclient that supports insert operation."""
    insert_client = self.apiclient
    return insert_client


  def GetTransferV1ApiClient(
       self, transferserver_address=None):
    """Return the apiclient that supports Transfer v1 operation."""
    path = transferserver_address
    if path is None:
      path = self.transfer_path
    if path is None:
      path = 'https://bigquerydatatransfer.googleapis.com'

    if not self._op_transfer_client:
      discovery_url = (path + '/$discovery/rest?version=v1')
      self._op_transfer_client = self.BuildApiClient(
      discovery_url=discovery_url)
    return self._op_transfer_client


  def GetReservationApiClient(self, reservationserver_address=None):
    """Return the apiclient that supports reservation operations."""
    path = reservationserver_address
    # Alpha feature actually is hosted in beta endpoint.
    if self.api_version == 'v1beta1' or self.api_version == 'autoscale_alpha':
      reservation_version = 'v1beta1'
    else:
      reservation_version = 'v1'
    if path is None:
      path = 'https://bigqueryreservation.googleapis.com'
    if not self._op_reservation_client:
      discovery_url = (
          path + '/$discovery/rest?version=' + reservation_version)
      self._op_reservation_client = self.BuildApiClient(
      discovery_url=discovery_url)
    return self._op_reservation_client

  def GetConnectionV1ApiClient(self, connection_service_address=None):
    """Return the apiclient that supports connections operations."""
    path = connection_service_address

    if path is None:
      path = 'https://bigqueryconnection.googleapis.com'
    if not self._op_connection_service_client:
      discovery_url = (path + '/$discovery/rest?version=v1')
      self._op_connection_service_client = self.BuildApiClient(
      discovery_url=discovery_url)
    return self._op_connection_service_client

  #################################
  ## Utility methods
  #################################

  @staticmethod
  def FormatTime(secs):
    return time.strftime('%d %b %H:%M:%S', time.localtime(secs))

  @staticmethod
  def FormatAcl(acl):
    """Format a server-returned ACL for printing."""
    acl_entries = collections.defaultdict(list)
    for entry in acl:
      entry = entry.copy()
      view = entry.pop('view', None)
      if view:
        acl_entries['VIEW'].append('%s:%s.%s' % (view.get('projectId'),
                                                 view.get('datasetId'),
                                                 view.get('tableId')))
      else:
        role = entry.pop('role', None)
        if not role or len(list(entry.values())) != 1:
          raise BigqueryInterfaceError(
              'Invalid ACL returned by server: %s' % acl, {}, [])
        acl_entries[role].extend(six.itervalues(entry))
    # Show a couple things first.
    original_roles = [
        ('OWNER', 'Owners'),
        ('WRITER', 'Writers'),
        ('READER', 'Readers'),
        ('VIEW', 'Authorized Views')]
    result_lines = []
    for role, name in original_roles:
      members = acl_entries.pop(role, None)
      if members:
        result_lines.append('%s:' % name)
        result_lines.append(',\n'.join('  %s' % m for m in sorted(members)))
    # Show everything else.
    for role, members in sorted(six.iteritems(acl_entries)):
      result_lines.append('%s:' % role)
      result_lines.append(',\n'.join('  %s' % m for m in sorted(members)))
    return '\n'.join(result_lines)

  @staticmethod
  def FormatSchema(schema):
    """Format a schema for printing."""

    def PrintFields(fields, indent=0):
      """Print all fields in a schema, recurring as necessary."""
      lines = []
      for field in fields:
        prefix = '|  ' * indent
        junction = '|' if field.get('type', 'STRING') != 'RECORD' else '+'
        entry = '%s- %s: %s' % (
            junction, field['name'], field.get('type', 'STRING').lower())
        if field.get('mode', 'NULLABLE') != 'NULLABLE':
          entry += ' (%s)' % (field['mode'].lower(),)
        lines.append(prefix + entry)
        if 'fields' in field:
          lines.extend(PrintFields(field['fields'], indent + 1))
      return lines

    return '\n'.join(PrintFields(schema.get('fields', [])))

  @staticmethod
  def NormalizeWait(wait):
    try:
      return int(wait)
    except ValueError:
      raise ValueError('Invalid value for wait: %s' % (wait,))

  @staticmethod
  def ValidatePrintFormat(print_format):
    if print_format not in [
        'show', 'list', 'view', 'materialized_view', 'make'
    ]:
      raise ValueError('Unknown format: %s' % (print_format,))

  @staticmethod
  def _ParseDatasetIdentifier(identifier):
    # We need to parse plx datasets separately.
    if identifier.startswith('plx.google:'):
      return 'plx.google', identifier[len('plx.google:'):]
    else:
      project_id, _, dataset_id = identifier.rpartition(':')
      return project_id, dataset_id

  @staticmethod
  def _ShiftInformationSchema(dataset_id, table_id):
    """Moves "INFORMATION_SCHEMA" to table_id for dataset qualified tables."""
    if not dataset_id or not table_id:
      return dataset_id, table_id

    dataset_parts = dataset_id.split('.')
    if dataset_parts[-1] != 'INFORMATION_SCHEMA' or table_id in (
        'SCHEMATA',
        'SCHEMATA_OPTIONS'):
      # We don't shift unless INFORMATION_SCHEMA is present and table_id is for
      # a dataset qualified table.
      return dataset_id, table_id

    return '.'.join(dataset_parts[:-1]), 'INFORMATION_SCHEMA.' + table_id

  @staticmethod
  def _ParseIdentifier(identifier):
    """Parses identifier into a tuple of (possibly empty) identifiers.

    This will parse the identifier into a tuple of the form
    (project_id, dataset_id, table_id) without doing any validation on
    the resulting names; missing names are returned as ''. The
    interpretation of these identifiers depends on the context of the
    caller. For example, if you know the identifier must be a job_id,
    then you can assume dataset_id is the job_id.

    Args:
      identifier: string, identifier to parse

    Returns:
      project_id, dataset_id, table_id: (string, string, string)
    """
    # We need to handle the case of a lone project identifier of the
    # form domain.com:proj separately.
    if re.search(r'^\w[\w.]*\.[\w.]+:\w[\w\d_-]*:?$', identifier):
      return identifier, '', ''
    project_id, _, dataset_and_table_id = identifier.rpartition(':')

    if '.' in dataset_and_table_id:
      dataset_id, _, table_id = dataset_and_table_id.rpartition('.')
    elif project_id:
      # Identifier was a project : <something without dots>.
      # We must have a dataset id because there was a project
      dataset_id = dataset_and_table_id
      table_id = ''
    else:
      # Identifier was just a bare id with no dots or colons.
      # Return this as a table_id.
      dataset_id = ''
      table_id = dataset_and_table_id

    dataset_id, table_id = BigqueryClient._ShiftInformationSchema(
        dataset_id, table_id)

    return project_id, dataset_id, table_id

  def GetProjectReference(self, identifier=''):
    """Determine a project reference from an identifier and self."""
    project_id, dataset_id, table_id = BigqueryClient._ParseIdentifier(
        identifier)
    try:
      # ParseIdentifier('foo') is just a table_id, but we want to read
      # it as a project_id.
      project_id = project_id or table_id or self.project_id
      if not dataset_id and project_id:
        return ApiClientHelper.ProjectReference.Create(projectId=project_id)
    except ValueError:
      pass
    if project_id == '':
      raise BigqueryClientError('Please provide a project ID.')
    else:
      raise BigqueryClientError('Cannot determine project described by %s' % (
          identifier,))

  def GetDatasetReference(self, identifier=''):
    """Determine a DatasetReference from an identifier and self."""
    identifier = self.dataset_id if not identifier else identifier
    project_id, dataset_id, table_id = BigqueryClient._ParseIdentifier(
        identifier)
    if table_id and not project_id and not dataset_id:
      # identifier is 'foo'
      project_id = self.project_id
      dataset_id = table_id
    elif project_id and dataset_id and table_id:
      # Identifier was foo::bar.baz.qux.
      dataset_id = dataset_id + '.' + table_id
    elif project_id and dataset_id and not table_id:
      # identifier is 'foo:bar'
      pass
    else:
      raise BigqueryError('Cannot determine dataset described by %s' % (
          identifier,))

    try:
      return ApiClientHelper.DatasetReference.Create(
          projectId=project_id, datasetId=dataset_id)
    except ValueError:
      raise BigqueryError('Cannot determine dataset described by %s' % (
          identifier,))

  def GetTableReference(self, identifier=''):
    """Determine a TableReference from an identifier and self."""
    project_id, dataset_id, table_id = BigqueryClient._ParseIdentifier(
        identifier)
    if not dataset_id:
      project_id, dataset_id = self._ParseDatasetIdentifier(self.dataset_id)
    try:
      return ApiClientHelper.TableReference.Create(
          projectId=project_id or self.project_id,
          datasetId=dataset_id,
          tableId=table_id,)
    except ValueError:
      raise BigqueryError('Cannot determine table described by %s' % (
          identifier,))

  def GetModelReference(self, identifier=''):
    """Returns a ModelReference from an identifier."""
    project_id, dataset_id, table_id = BigqueryClient._ParseIdentifier(
        identifier)
    if not dataset_id:
      project_id, dataset_id = self._ParseDatasetIdentifier(self.dataset_id)
    try:
      return ApiClientHelper.ModelReference.Create(
          projectId=project_id or self.project_id,
          datasetId=dataset_id,
          modelId=table_id)
    except ValueError:
      raise BigqueryError('Cannot determine model described by %s'
                          % identifier)

  def GetRoutineReference(self, identifier=''):
    """Returns a RoutineReference from an identifier."""
    project_id, dataset_id, table_id = BigqueryClient._ParseIdentifier(
        identifier)
    if not dataset_id:
      project_id, dataset_id = self._ParseDatasetIdentifier(self.dataset_id)
    try:
      return ApiClientHelper.RoutineReference.Create(
          projectId=project_id or self.project_id,
          datasetId=dataset_id,
          routineId=table_id)
    except ValueError:
      raise BigqueryError(
          'Cannot determine routine described by %s' % identifier)

  def GetQueryDefaultDataset(self, identifier):
    parsed_project_id, parsed_dataset_id = self._ParseDatasetIdentifier(
        identifier)
    result = dict(datasetId=parsed_dataset_id)
    if parsed_project_id:
      result['projectId'] = parsed_project_id
    return result

  def GetReference(self, identifier=''):
    """Try to deduce a project/dataset/table reference from a string.

    If the identifier is not compound, treat it as the most specific
    identifier we don't have as a flag, or as the table_id. If it is
    compound, fill in any unspecified part.

    Args:
      identifier: string, Identifier to create a reference for.

    Returns:
      A valid ProjectReference, DatasetReference, or TableReference.

    Raises:
      BigqueryError: if no valid reference can be determined.
    """
    try:
      return self.GetTableReference(identifier)
    except BigqueryError:
      pass
    try:
      return self.GetDatasetReference(identifier)
    except BigqueryError:
      pass
    try:
      return self.GetProjectReference(identifier)
    except BigqueryError:
      pass
    raise BigqueryError('Cannot determine reference for "%s"' % (identifier,))

  def GetJobReference(self, identifier='', default_location=None):
    """Determine a JobReference from an identifier, location, and self."""
    project_id, location, job_id = _ParseJobIdentifier(identifier)
    if not project_id:
      project_id = self.project_id
    if not location:
      location = default_location
    if job_id:
      try:
        return ApiClientHelper.JobReference.Create(
            projectId=project_id, jobId=job_id, location=location)
      except ValueError:
        pass
    raise BigqueryError('Cannot determine job described by %s' % (
        identifier,))

  def GetReservationReference(self,
                              identifier=None,
                              default_location=None,
                              default_reservation_id=None):
    """Determine a ReservationReference from an identifier and location."""
    project_id, location, reservation_id = _ParseReservationIdentifier(
        identifier=identifier)
    if project_id and self.project_id and project_id != self.project_id:
      raise BigqueryError(
          "Specified project '%s' should be the same as the project of the "
          "reservation '%s'." % (self.project_id, project_id))
    project_id = project_id or self.project_id
    if not project_id:
      raise BigqueryError('Project id not specified.')
    location = location or default_location
    if not location:
      raise BigqueryError('Location not specified.')
    if default_location and location.lower() != default_location.lower():
      raise BigqueryError(
          "Specified location '%s' should be the same as the location of the "
          "reservation '%s'." % (default_location, location))
    reservation_id = reservation_id or default_reservation_id
    if not reservation_id:
      raise BigqueryError('Reservation name not specified.')
    if (self.api_version == 'autoscale_alpha'
       ):
      return ApiClientHelper.AutoscaleAlphaReservationReference(
          projectId=project_id, location=location, reservationId=reservation_id)
    else:
      return ApiClientHelper.ReservationReference(
          projectId=project_id, location=location, reservationId=reservation_id)

  def GetBiReservationReference(self, default_location=None):
    """Determine a ReservationReference from an identifier and location."""
    project_id = self.project_id
    if not project_id:
      raise BigqueryError('Project id not specified.')
    location = default_location
    if not location:
      raise BigqueryError('Location not specified.')
    return ApiClientHelper.BiReservationReference.Create(
        projectId=project_id, location=location)

  def GetCapacityCommitmentReference(self,
                                     identifier=None,
                                     path=None,
                                     default_location=None,
                                     default_capacity_commitment_id=None,
                                     allow_commas=None):
    """Determine a CapacityCommitmentReference from an identifier and location."""
    if identifier is not None:
      project_id, location, capacity_commitment_id = _ParseCapacityCommitmentIdentifier(
          identifier, allow_commas)
    elif path is not None:
      project_id, location, capacity_commitment_id = _ParseCapacityCommitmentPath(
          path)
    else:
      raise BigqueryError('Either identifier or path must be specified.')
    project_id = project_id or self.project_id
    if not project_id:
      raise BigqueryError('Project id not specified.')
    location = location or default_location
    if not location:
      raise BigqueryError('Location not specified.')
    capacity_commitment_id = capacity_commitment_id or default_capacity_commitment_id
    if not capacity_commitment_id:
      raise BigqueryError('Capacity commitment id not specified.')
    return ApiClientHelper.CapacityCommitmentReference.Create(
        projectId=project_id,
        location=location,
        capacityCommitmentId=capacity_commitment_id)

  def GetReservationAssignmentReference(self,
                                        identifier=None,
                                        path=None,
                                        default_location=None,
                                        default_reservation_id=None,
                                        default_reservation_assignment_id=None):
    """Determine a ReservationAssignmentReference from an identifier and location."""
    if identifier is not None:
      (project_id, location, reservation_id, reservation_assignment_id
      ) = _ParseReservationAssignmentIdentifier(identifier)
    elif path is not None:
      (project_id, location, reservation_id,
       reservation_assignment_id) = _ParseReservationAssignmentPath(path)
    else:
      raise BigqueryError('Either identifier or path must be specified.')
    project_id = project_id or self.project_id
    if not project_id:
      raise BigqueryError('Project id not specified.')
    location = location or default_location
    if not location:
      raise BigqueryError('Location not specified.')
    reservation_id = reservation_id or default_reservation_id
    reservation_assignment_id = reservation_assignment_id or default_reservation_assignment_id
    return ApiClientHelper.ReservationAssignmentReference.Create(
        projectId=project_id,
        location=location,
        reservationId=reservation_id,
        reservationAssignmentId=reservation_assignment_id)

  def GetConnectionReference(self,
                             identifier=None,
                             path=None,
                             default_location=None,
                             default_connection_id=None):
    """Determine a ConnectionReference from an identifier and location."""
    if identifier is not None:
      (project_id, location,
       connection_id) = _ParseConnectionIdentifier(identifier)
    elif path is not None:
      (project_id, location, connection_id) = _ParseConnectionPath(path)
    project_id = project_id or self.project_id
    if not project_id:
      raise BigqueryError('Project id not specified.')
    location = location or default_location
    if not location:
      raise BigqueryError('Location not specified.')
    connection_id = connection_id or default_connection_id
    if not connection_id:
      raise BigqueryError('Connection name not specified.')
    return ApiClientHelper.ConnectionReference.Create(
        projectId=project_id, location=location, connectionId=connection_id)

  def GetObjectInfo(self, reference):
    """Get all data returned by the server about a specific object."""
    # Projects are handled separately, because we only have
    # bigquery.projects.list.
    if isinstance(reference, ApiClientHelper.ProjectReference):
      projects = self.ListProjects(max_results=1000)
      for project in projects:
        if BigqueryClient.ConstructObjectReference(project) == reference:
          project['kind'] = 'bigquery#project'
          return project
      raise BigqueryNotFoundError(
          'Unknown %r' % (reference,), {'reason': 'notFound'}, [])

    if isinstance(reference, ApiClientHelper.JobReference):
      return self.apiclient.jobs().get(**dict(reference)).execute()
    elif isinstance(reference, ApiClientHelper.DatasetReference):
      return self.apiclient.datasets().get(**dict(reference)).execute()
    elif isinstance(reference, ApiClientHelper.TableReference):
      return self.apiclient.tables().get(**dict(reference)).execute()
    elif isinstance(reference, ApiClientHelper.ModelReference):
      return self.GetModelsApiClient().models().get(
          projectId=reference.projectId,
          datasetId=reference.datasetId,
          modelId=reference.modelId).execute()
    elif isinstance(reference, ApiClientHelper.RoutineReference):
      return self.GetRoutinesApiClient().routines().get(
          projectId=reference.projectId,
          datasetId=reference.datasetId,
          routineId=reference.routineId).execute()
    else:
      raise TypeError('Type of reference must be one of: ProjectReference, '
                      'JobReference, DatasetReference, or TableReference')

  def GetTableSchema(self, table_dict):
    table_info = self.apiclient.tables().get(**table_dict).execute()
    return table_info.get('schema', {})

  def InsertTableRows(self, table_dict, inserts, skip_invalid_rows=None,
                      ignore_unknown_values=None, template_suffix=None):
    """Insert rows into a table.

    Arguments:
      table_dict: table reference into which rows are to be inserted.
      inserts: array of InsertEntry tuples where insert_id can be None.
      skip_invalid_rows: Optional. Attempt to insert any valid rows, even if
          invalid rows are present.
      ignore_unknown_values: Optional. Ignore any values in a row that are not
          present in the schema.
      template_suffix: Optional. The suffix used to generate the template
          table's name.

    Returns:
      result of the operation.
    """
    def _EncodeInsert(insert):
      encoded = dict(json=insert.record)
      if insert.insert_id:
        encoded['insertId'] = insert.insert_id
      return encoded
    op = self.GetInsertApiClient().tabledata().insertAll(
        body=dict(
            skipInvalidRows=skip_invalid_rows,
            ignoreUnknownValues=ignore_unknown_values,
            templateSuffix=template_suffix,
            rows=list(map(_EncodeInsert, inserts))),
        **table_dict)
    return op.execute()


  def GetTransferConfig(self, transfer_id):
    client = self.GetTransferV1ApiClient()
    return client.projects().locations().transferConfigs().get(
        name=transfer_id).execute()

  def GetTransferRun(self, identifier):
    transfer_client = self.GetTransferV1ApiClient()
    return transfer_client.projects().locations().transferConfigs().runs().get(
        name=identifier).execute()

  def CreateReservation(self,
                        reference,
                        slots,
                        ignore_idle_slots,
                        autoscale_max_slots=None):
    """Create a reservation with the given reservation reference.

    Arguments:
      reference: Reservation to create.
      slots: Number of slots allocated to this reservation subtree.
      ignore_idle_slots: Specifies whether queries should ignore idle slots from
        other reservations.
      autoscale_max_slots: Number of slots to be scaled when needed.

    Returns:
      Reservation object that was created.

    Raises:
      BigqueryError: if autoscale_max_slots is used with other version.
    """
    reservation = {}
    reservation['slot_capacity'] = slots
    reservation['ignore_idle_slots'] = ignore_idle_slots
    if autoscale_max_slots is not None:
      if (self.api_version != 'autoscale_alpha'
         ):
        raise BigqueryError(
            'Autoscale is only supported in autoscale_alpha. Please '
            'specify \'--api_version=autoscale_alpha\' and retry.')
      reservation['autoscale'] = {}
      reservation['autoscale']['max_slots'] = autoscale_max_slots
    client = self.GetReservationApiClient()

    parent = 'projects/%s/locations/%s' % (reference.projectId,
                                           reference.location)
    return client.projects().locations().reservations().create(
        parent=parent, body=reservation,
        reservationId=reference.reservationId).execute()

  def ListReservations(self, reference, page_size, page_token):
    """List reservations in the project and location for the given reference.

    Arguments:
      reference: Reservation reference containing project and location.
      page_size: Number of results to show.
      page_token: Token to retrieve the next page of results.

    Returns:
      Reservation object that was created.
    """
    parent = 'projects/%s/locations/%s' % (
        reference.projectId,
        reference.location
        )
    client = self.GetReservationApiClient()
    return client.projects().locations().reservations().list(
        parent=parent, pageSize=page_size, pageToken=page_token).execute()

  def ListBiReservations(self, reference):
    """List BI reservations in the project and location for the given reference.

    Arguments:
      reference: Reservation reference containing project and location.

    Returns:
      List of BI reservations in the given project/location.
    """
    parent = 'projects/%s/locations/%s/biReservation' % (reference.projectId,
                                                         reference.location)
    client = self.GetReservationApiClient()
    response = client.projects().locations().getBiReservation(
        name=parent).execute()
    return response

  def GetReservation(self, reference):
    """Gets a reservation with the given reservation reference.

    Arguments:
      reference: Reservation to get.

    Returns:
      Reservation object corresponding to the given id.
    """
    client = self.GetReservationApiClient()
    return client.projects().locations().reservations().get(
        name=reference.path()).execute()

  def DeleteReservation(self, reference):
    """Deletes a reservation with the given reservation reference.

    Arguments:
      reference: Reservation to delete.
    """
    client = self.GetReservationApiClient()
    client.projects().locations().reservations().delete(
        name=reference.path()).execute()

  def UpdateBiReservation(self, reference, reservation_size):
    """Updates a BI reservation with the given reservation reference.

    Arguments:
      reference: Reservation to update.
      reservation_size: size of reservation in GBs. It may only contain digits,
        optionally followed by 'G', 'g', 'GB, 'gb', 'gB', or 'Gb'.

    Returns:
      Reservation object that was updated.
    Raises:
      ValueError: if reservation_size is malformed.
    """
    client = self.GetReservationApiClient()

    if (reservation_size.upper().endswith('GB') and
        reservation_size[:-2].isdigit()):
      reservation_digits = reservation_size[:-2]
    elif (reservation_size.upper().endswith('G') and
          reservation_size[:-1].isdigit()):
      reservation_digits = reservation_size[:-1]
    elif reservation_size.isdigit():
      reservation_digits = reservation_size
    else:
      raise ValueError("""Invalid reservation size. The unit for BI reservations
      is GB. The specified reservation size may only contain digits, optionally
      followed by G, g, GB, gb, gB, or Gb.""")

    reservation_size = int(reservation_digits) * 1024 * 1024 * 1024

    bi_reservation = {}
    update_mask = ''
    bi_reservation['size'] = reservation_size
    update_mask += 'size,'
    return client.projects().locations().updateBiReservation(
        name=reference.path(), updateMask=update_mask,
        body=bi_reservation).execute()

  def UpdateReservation(self, reference, slots, ignore_idle_slots,
                        autoscale_max_slots):
    """Updates a reservation with the given reservation reference.

    Arguments:
      reference: Reservation to update.
      slots: Number of slots allocated to this reservation subtree.
      ignore_idle_slots: Specifies whether queries should ignore idle slots from
        other reservations.
      autoscale_max_slots: Number of slots to be scaled when needed.

    Returns:
      Reservation object that was updated.

    Raises:
      BigqueryError: if autoscale_max_slots is used with other version.
    """
    reservation = {}
    update_mask = ''
    if slots is not None:
      reservation['slot_capacity'] = slots
      update_mask += 'slot_capacity,'

    if ignore_idle_slots is not None:
      reservation['ignore_idle_slots'] = ignore_idle_slots
      update_mask += 'ignore_idle_slots,'

    if autoscale_max_slots is not None:
      if (self.api_version != 'autoscale_alpha'
         ):
        raise BigqueryError(
            'Autoscale is only supported in autoscale_alpha. Please '
            'specify \'--api_version=autoscale_alpha\' and retry.')
      if autoscale_max_slots != 0:
        reservation['autoscale'] = {}
        reservation['autoscale']['max_slots'] = autoscale_max_slots
        update_mask += 'autoscale.max_slots,'
      else:
        # Disable autoscale.
        update_mask += 'autoscale,'
    client = self.GetReservationApiClient()
    return client.projects().locations().reservations().patch(
        name=reference.path(), updateMask=update_mask,
        body=reservation).execute()

  def CreateCapacityCommitment(
      self,
      reference,
      slots,
      plan,
      renewal_plan):
    # pylint: disable=g-doc-args
    """Create a capacity commitment.

    Arguments:
      reference: Project to create a capacity commitment within.
      slots: Number of slots in this commitment.
      plan: Commitment plan for this capacity commitment.
      renewal_plan: Renewal plan for this capacity commitment.

    Returns:
      Capacity commitment object that was created.
    """
    capacity_commitment = {}
    capacity_commitment['slot_count'] = slots
    capacity_commitment['plan'] = plan
    capacity_commitment['renewal_plan'] = renewal_plan
    client = self.GetReservationApiClient()
    parent = 'projects/%s/locations/%s' % (reference.projectId,
                                           reference.location)
    request = client.projects().locations().capacityCommitments().create(
        parent=parent, body=capacity_commitment)
    return request.execute()

  def ListCapacityCommitments(self, reference, page_size, page_token):
    """Lists capacity commitments for given project and location.

    Arguments:
      reference: Reference to the project and location.
      page_size: Number of results to show.
      page_token: Token to retrieve the next page of results.

    Returns:
      list of CapacityCommitments objects.
    """
    parent = 'projects/%s/locations/%s' % (reference.projectId,
                                           reference.location)
    client = self.GetReservationApiClient()
    return client.projects().locations().capacityCommitments().list(
        parent=parent, pageSize=page_size, pageToken=page_token).execute()

  def GetCapacityCommitment(self, reference):
    """Gets a capacity commitment with the given capacity commitment reference.

    Arguments:
      reference: Capacity commitment to get.

    Returns:
      Capacity commitment object corresponding to the given id.
    """
    client = self.GetReservationApiClient()
    return client.projects().locations().capacityCommitments().get(
        name=reference.path()).execute()

  def DeleteCapacityCommitment(self, reference, force=None):
    """Deletes a capacity commitment with the given capacity commitment reference.

    Arguments:
      reference: Capacity commitment to delete.
      force: Force delete capacity commitment, ignoring commitment end time.
    """
    client = self.GetReservationApiClient()
    client.projects().locations().capacityCommitments().delete(
        name=reference.path()).execute()

  def UpdateCapacityCommitment(self, reference, plan, renewal_plan):
    """Updates a capacity commitment with the given reference.

    Arguments:
      reference: Capacity commitment to update.
      plan: Commitment plan for this capacity commitment.
      renewal_plan: Renewal plan for this capacity commitment.

    Returns:
      Capacity commitment object that was updated.

    Raises:
      BigqueryError: if capacity commitment cannot be updated.
    """
    if plan is None and renewal_plan is None:
      raise BigqueryError('Please specify fields to be updated.')
    capacity_commitment = {}
    update_mask = []
    if plan is not None:
      capacity_commitment['plan'] = plan
      update_mask.append('plan')
    if renewal_plan is not None:
      capacity_commitment['renewal_plan'] = renewal_plan
      update_mask.append('renewal_plan')

    client = self.GetReservationApiClient()
    return client.projects().locations().capacityCommitments().patch(
        name=reference.path(), updateMask=','.join(update_mask),
        body=capacity_commitment).execute()

  def SplitCapacityCommitment(self, reference, slots):
    """Splits a capacity commitment with the given reference into two.

    Arguments:
      reference: Capacity commitment to split.
      slots: Number of slots in the first capacity commitment after the split.

    Returns:
      List of capacity commitment objects after the split.

    Raises:
      BigqueryError: if capacity commitment cannot be updated.
    """
    if slots is None:
      raise BigqueryError('Please specify slots for the split.')
    client = self.GetReservationApiClient()
    body = {'slotCount': slots}
    response = client.projects().locations().capacityCommitments().split(
        name=reference.path(), body=body).execute()
    if 'first' not in response or 'second' not in response:
      raise BigqueryError('internal error')
    return [response['first'], response['second']]

  def MergeCapacityCommitments(self, location, capacity_commitment_ids):
    """Merges capacity commitments into one.

    Arguments:
      location: Capacity commitments location.
      capacity_commitment_ids: List of capacity commitment ids.

    Returns:
      Merged capacity commitment.

    Raises:
      BigqueryError: if capacity commitment cannot be merged.
    """
    if not self.project_id:
      raise BigqueryError('project id must be specified.')
    if not location:
      raise BigqueryError('location must be specified.')
    if capacity_commitment_ids is None or len(capacity_commitment_ids) < 2:
      raise BigqueryError('at least 2 capacity commitments must be specified.')
    client = self.GetReservationApiClient()
    parent = 'projects/%s/locations/%s' % (self.project_id, location)
    body = {'capacityCommitmentIds': capacity_commitment_ids}
    return client.projects().locations().capacityCommitments().merge(
        parent=parent, body=body).execute()

  def CreateReservationAssignment(self, reference, job_type, assignee_type,
                                  assignee_id):
    """Creates a reservation assignment for a given project/folder/organization.

    Arguments:
      reference: Reference to the project reservation is assigned. Location must
        be the same location as the reservation.
      job_type: Type of jobs for this assignment.
      assignee_type: Type of assignees for the reservation assignment.
      assignee_id: Project/folder/organization ID, to which the reservation is
        assigned.

    Returns:
      ReservationAssignment object that was created.

    Raises:
      BigqueryError: if assignment cannot be created.
    """
    reservation_assignment = {}
    if not job_type:
      raise BigqueryError('job_type not specified.')
    reservation_assignment['job_type'] = job_type
    if not assignee_type:
      raise BigqueryError('assignee_type not specified.')
    if not assignee_id:
      raise BigqueryError('assignee_id not specified.')
    # assignee_type is singular, that's why we need additional 's' inside
    # format string for assignee below.
    reservation_assignment['assignee'] = '%ss/%s' % (assignee_type.lower(),
                                                     assignee_id)
    client = self.GetReservationApiClient()
    return client.projects().locations().reservations().assignments().create(
        parent=reference.path(), body=reservation_assignment).execute()

  def DeleteReservationAssignment(self, reference):
    """Deletes given reservation assignment.

    Arguments:
      reference: Reference to the reservation assignment.
    """
    client = self.GetReservationApiClient()
    client.projects().locations().reservations().assignments().delete(
        name=reference.path()).execute()

  def MoveReservationAssignment(self, reference, destination_reservation_id,
                                default_location):
    """Moves given reservation assignment under another reservation."""
    destination_reservation_reference = self.GetReservationReference(
        identifier=destination_reservation_id,
        default_location=default_location)
    client = self.GetReservationApiClient()
    body = {'destinationId': destination_reservation_reference.path()}

    return client.projects().locations().reservations().assignments().move(
        name=reference.path(), body=body).execute()

  def ListReservationAssignments(self, reference, page_size, page_token):
    """Lists reservation assignments for given project and location.

    Arguments:
      reference: Reservation reference for the parent.
      page_size: Number of results to show.
      page_token: Token to retrieve the next page of results.

    Returns:
      ReservationAssignment object that was created.
    """
    client = self.GetReservationApiClient()
    return client.projects().locations().reservations().assignments().list(
        parent=reference.path(), pageSize=page_size,
        pageToken=page_token).execute()

  def SearchReservationAssignments(self, location, job_type, assignee_type,
                                   assignee_id):
    """Searchs reservations assignments for given assignee.

    Arguments:
      location: location of interest.
      job_type: type of job to be queried.
      assignee_type: Type of assignees for the reservation assignment.
      assignee_id: Project/folder/organization ID, to which the reservation is
        assigned.

    Returns:
      ReservationAssignment object that was created.

    Raises:
      BigqueryError: If required parameters are not passed in.
    """
    if not location:
      raise BigqueryError('location not specified.')
    if not job_type:
      raise BigqueryError('job_type not specified.')
    if not assignee_type:
      raise BigqueryError('assignee_type not specified.')
    if not assignee_id:
      raise BigqueryError('assignee_id not specified.')
    # assignee_type is singular, that's why we need additional 's' inside
    # format string for assignee below.
    assignee = '%ss/%s' % (assignee_type.lower(), assignee_id)
    query = 'assignee=%s' % assignee
    parent = 'projects/-/locations/%s' % location
    client = self.GetReservationApiClient()

    response = client.projects().locations().searchAllAssignments(
        parent=parent, query=query).execute()
    assignments = []
    if 'assignments' in response:
      assignments = response['assignments']
    for assignment in assignments:
      if assignment['jobType'] == job_type:
        return assignment
    raise BigqueryError('Reservation assignment not found')

  def GetConnection(self, reference):
    """Gets connection with the given connection reference.

    Arguments:
      reference: Connection to get.

    Returns:
      Connection object with the given id.
    """
    client = self.GetConnectionV1ApiClient()
    return client.projects().locations().connections().get(
        name=reference.path()).execute()

  def CreateConnection(self,
                       project_id,
                       location,
                       connection_type,
                       properties,
                       connection_credential=None,
                       display_name=None,
                       description=None,
                       connection_id=None):
    """Create a connection with the given connection reference.

    Arguments:
      project_id: Project ID.
      location: Location of connection.
      connection_type: Type of connection, allowed values: ['CLOUD_SQL']
      properties: Connection properties in JSON format.
      connection_credential: Connection credentials in JSON format.
      display_name: Friendly name for the connection.
      description: Description of the connection.
      connection_id: Optional connection ID.

    Returns:
      Connection object that was created.
    """

    connection = {}

    if display_name:
      connection['friendlyName'] = display_name

    if description:
      connection['description'] = description

    property_name = CONNECTION_TYPE_TO_PROPERTY_MAP.get(connection_type)
    if property_name:
      connection[property_name] = json.loads(properties)

      if connection_credential:
        connection[property_name]['credential'] = \
          json.loads(connection_credential)

    else:
      raise ValueError('connection_type %s is unsupported' % connection_type)

    client = self.GetConnectionV1ApiClient()
    parent = 'projects/%s/locations/%s' % (project_id, location)
    return client.projects().locations().connections().create(
        parent=parent, connectionId=connection_id, body=connection).execute()

  def UpdateConnection(self,
                       reference,
                       connection_type,
                       properties,
                       connection_credential=None,
                       display_name=None,
                       description=None):
    """Update connection with the given connection reference.

    Arguments:
      reference: Connection to update
      connection_type: Type of connection, allowed values: ['CLOUD_SQL']
      properties: Connection properties
      connection_credential: Connection credentials in JSON format.
      display_name: Friendly name for the connection
      description: Description of the connection

    Returns:
      Connection object that was created.
    """

    connection = {}
    update_mask = []

    def GetUpdateMask(base_path, json_properties):
      """Creates an update mask from json_properties.

      Arguments:
        base_path: 'cloud_sql'
        json_properties:
        {
          'host': ... ,
          'instanceId': ...
        }
      Returns:
         list of  paths in snake case:
         mask = ['cloud_sql.host', 'cloud_sql.instance_id']
      """
      return [base_path + '.' + inflection.underscore(json_property)
              for json_property in json_properties]

    if display_name:
      connection['friendlyName'] = display_name
      update_mask.append('friendlyName')

    if description:
      connection['description'] = description
      update_mask.append('description')

    if connection_type == 'CLOUD_SQL':
      if properties:
        cloudsql_properties = json.loads(properties)
        connection['cloudSql'] = cloudsql_properties

        update_mask.extend(
            GetUpdateMask(connection_type.lower(), cloudsql_properties))

      else:
        connection['cloudSql'] = {}

      if connection_credential:
        connection['cloudSql']['credential'] = json.loads(connection_credential)
        update_mask.append('cloudSql.credential')

    elif connection_type == 'AWS':

      if properties:
        aws_properties = json.loads(properties)
        connection['aws'] = aws_properties
        if aws_properties.get('crossAccountRole') and \
            aws_properties['crossAccountRole'].get('iamRoleId'):
          update_mask.append('aws.crossAccountRole.iamRoleId')
      else:
        connection['aws'] = {}

      if connection_credential:
        connection['aws']['credential'] = json.loads(connection_credential)
      update_mask.append('aws.credential')

    elif connection_type == 'SQL_DATA_SOURCE':
      if properties:
        sql_data_source_properties = json.loads(properties)
        connection['sqlDataSource'] = sql_data_source_properties

        update_mask.extend(
            GetUpdateMask(connection_type.lower(), sql_data_source_properties))

      else:
        connection['sqlDataSource'] = {}

      if connection_credential:
        connection['sqlDataSource']['credential'] = json.loads(
            connection_credential)
        update_mask.append('sqlDataSource.credential')

    client = self.GetConnectionV1ApiClient()

    return client.projects().locations().connections().patch(
        name=reference.path(), updateMask=','.join(update_mask),
        body=connection).execute()

  def DeleteConnection(self, reference):
    """Delete a connection with the given connection reference.

    Arguments:
      reference: Connection to delete.
    """
    client = self.GetConnectionV1ApiClient()
    client.projects().locations().connections().delete(
        name=reference.path()).execute()

  def ListConnections(self, project_id, location, max_results, page_token):
    """List connections in the project and location for the given reference.

    Arguments:
      project_id: Project ID.
      location: Location.
      max_results: Number of results to show.
      page_token: Token to retrieve the next page of results.

    Returns:
      List of connnection objects
    """
    parent = 'projects/%s/locations/%s' % (project_id, location)
    client = self.GetConnectionV1ApiClient()
    return client.projects().locations().connections().list(
        parent=parent, pageToken=page_token, pageSize=max_results).execute()

  def ReadSchemaAndRows(self, table_dict, start_row=None, max_rows=None,
                        selected_fields=None):
    """Convenience method to get the schema and rows from a table.

    Arguments:
      table_dict: table reference dictionary.
      start_row: first row to read.
      max_rows: number of rows to read.
      selected_fields: a subset of fields to return.

    Returns:
      A tuple where the first item is the list of fields and the
      second item a list of rows.

    Raises:
      ValueError: will be raised if start_row is not explicitly provided.
      ValueError: will be raised if max_rows is not explicitly provided.
    """
    if start_row is None:
      raise ValueError('start_row is required')
    if max_rows is None:
      raise ValueError('max_rows is required')
    table_ref = ApiClientHelper.TableReference.Create(**table_dict)
    table_reader = _TableTableReader(self.apiclient,
                                     self.max_rows_per_request,
                                     table_ref)
    return table_reader.ReadSchemaAndRows(start_row, max_rows,
                                          selected_fields=selected_fields)

  def ReadSchemaAndJobRows(self,
                           job_dict,
                           start_row=None,
                           max_rows=None,
                           result_first_page=None):
    """Convenience method to get the schema and rows from job query result.

    Arguments:
      job_dict: job reference dictionary.
      start_row: first row to read.
      max_rows: number of rows to read.
      result_first_page: the first page of the result of a query job.

    Returns:
      A tuple where the first item is the list of fields and the
      second item a list of rows.
    Raises:
      ValueError: will be raised if start_row is not explicitly provided.
      ValueError: will be raised if max_rows is not explicitly provided.
    """
    if start_row is None:
      raise ValueError('start_row is required')
    if max_rows is None:
      raise ValueError('max_rows is required')
    job_ref = ApiClientHelper.JobReference.Create(**job_dict)
    if flags.FLAGS.jobs_query_use_results_from_response and result_first_page:
      reader = _QueryTableReader(self.apiclient, self.max_rows_per_request,
                                 job_ref, result_first_page)
    else:
      reader = _JobTableReader(self.apiclient, self.max_rows_per_request,
                               job_ref)
    return reader.ReadSchemaAndRows(start_row, max_rows)

  @staticmethod
  def ConfigureFormatter(formatter, reference_type, print_format='list',
                         object_info=None):
    """Configure a formatter for a given reference type.

    If print_format is 'show', configures the formatter with several
    additional fields (useful for printing a single record).

    Arguments:
      formatter: TableFormatter object to configure.
      reference_type: Type of object this formatter will be used with.
      print_format: Either 'show' or 'list' to control what fields are
        included.

    Raises:
      ValueError: If reference_type or format is unknown.
    """
    BigqueryClient.ValidatePrintFormat(print_format)
    if reference_type == ApiClientHelper.JobReference:
      if print_format == 'list':
        formatter.AddColumns(('jobId',))
      formatter.AddColumns(('Job Type', 'State', 'Start Time', 'Duration',))
      if print_format == 'show':
        formatter.AddColumns(('User Email',))
        formatter.AddColumns(('Bytes Processed',))
        formatter.AddColumns(('Bytes Billed',))
        formatter.AddColumns(('Billing Tier',))
        formatter.AddColumns(('Labels',))
    elif reference_type == ApiClientHelper.ProjectReference:
      if print_format == 'list':
        formatter.AddColumns(('projectId',))
      formatter.AddColumns(('friendlyName',))
    elif reference_type == ApiClientHelper.DatasetReference:
      if print_format == 'list':
        formatter.AddColumns(('datasetId',))
      if print_format == 'show':
        formatter.AddColumns(('Last modified', 'ACLs',))
        formatter.AddColumns(('Labels',))
        if 'defaultEncryptionConfiguration' in object_info:
          formatter.AddColumns(('kmsKeyName',))
    elif reference_type == ApiClientHelper.TransferConfigReference:
      if print_format == 'list':
        formatter.AddColumns(('name',))
        formatter.AddColumns(('displayName',))
        formatter.AddColumns(('dataSourceId',))
        formatter.AddColumns(('state',))
      if print_format == 'show':
        for key in object_info.keys():
          if key != 'name':
            formatter.AddColumns((key,))
    elif reference_type == ApiClientHelper.TransferRunReference:
      if print_format == 'show':
        for column in BigqueryClient.columns_to_include_for_transfer_run:
          if column != 'name':
            formatter.AddColumns((column,))
      elif print_format == 'list':
        for column in BigqueryClient.columns_to_include_for_transfer_run:
          formatter.AddColumns((column,))
      elif print_format == 'make':
        for column in BigqueryClient.columns_to_include_for_transfer_run:
          if column not in (
              BigqueryClient.columns_excluded_for_make_transfer_run):
            formatter.AddColumns((column,))
    elif reference_type == ApiClientHelper.TransferLogReference:
      formatter.AddColumns(('messageText',))
      formatter.AddColumns(('messageTime',))
      formatter.AddColumns(('severity',))
    elif reference_type == ApiClientHelper.NextPageTokenReference:
      formatter.AddColumns(('nextPageToken',))
    elif reference_type == ApiClientHelper.ModelReference:
      if print_format == 'list':
        formatter.AddColumns(('Id', 'Model Type', 'Labels', 'Creation Time'))
      if print_format == 'show':
        formatter.AddColumns(('Id', 'Model Type', 'Feature Columns',
                              'Label Columns', 'Labels', 'Creation Time',
                              'Expiration Time'))
        if 'encryptionConfiguration' in object_info:
          formatter.AddColumns(('kmsKeyName',))
    elif reference_type == ApiClientHelper.RoutineReference:
      if print_format == 'list':
        formatter.AddColumns(('Id', 'Routine Type', 'Language', 'Creation Time',
                              'Last Modified Time'))
      if print_format == 'show':
        formatter.AddColumns(
            ('Id', 'Routine Type', 'Language', 'Signature', 'Definition',
             'Creation Time', 'Last Modified Time'))
    elif reference_type == ApiClientHelper.TableReference:
      if print_format == 'list':
        formatter.AddColumns(('tableId', 'Type',))
        formatter.AddColumns(('Labels', 'Time Partitioning',
                              'Clustered Fields'))
      if print_format == 'show':
        use_default = True
        if object_info is not None:
          if object_info['type'] == 'VIEW':
            formatter.AddColumns(('Last modified', 'Schema', 'Type',
                                  'Expiration'))
            use_default = False
          elif object_info['type'] == 'EXTERNAL':
            formatter.AddColumns(('Last modified', 'Schema', 'Type',
                                  'Total URIs', 'Expiration'))
            use_default = False
          elif object_info['type'] == 'SNAPSHOT':
            formatter.AddColumns(('Base Table', 'Snapshot TimeStamp'))
        if use_default:
          formatter.AddColumns(('Last modified', 'Schema',
                                'Total Rows', 'Total Bytes',
                                'Expiration', 'Time Partitioning',
                                'Clustered Fields'))
        formatter.AddColumns(('Labels',))
        if 'encryptionConfiguration' in object_info:
          formatter.AddColumns(('kmsKeyName',))
      if print_format == 'view':
        formatter.AddColumns(('Query',))
      if print_format == 'materialized_view':
        formatter.AddColumns((
            'Query',
            'Enable Refresh',
            'Refresh Interval Ms',
            'Last Refresh Time'
        ))
    elif reference_type == ApiClientHelper.EncryptionServiceAccount:
      formatter.AddColumns(list(object_info.keys()))
    elif reference_type == ApiClientHelper.ReservationReference:
      formatter.AddColumns(('name', 'slotCapacity', 'ignoreIdleSlots',
                            'creationTime', 'updateTime'))
    elif reference_type == ApiClientHelper.AutoscaleAlphaReservationReference:
      formatter.AddColumns(
          ('name', 'slotCapacity', 'ignoreIdleSlots', 'autoscaleMaxSlots',
           'autoscaleCurrentSlots', 'creationTime', 'updateTime'))
    elif reference_type == ApiClientHelper.CapacityCommitmentReference:
      formatter.AddColumns(('name', 'slotCount', 'plan', 'renewalPlan', 'state',
                            'commitmentStartTime', 'commitmentEndTime'))
    elif reference_type == ApiClientHelper.ReservationAssignmentReference:
      formatter.AddColumns(('name', 'jobType', 'assignee'))
    elif reference_type == ApiClientHelper.ConnectionReference:
      formatter.AddColumns(('name', 'friendlyName', 'description',
                            'Last modified', 'type', 'hasCredential',
                            'properties'))
    else:
      raise ValueError('Unknown reference type: %s' % (
          reference_type.__name__,))

  @staticmethod
  def RaiseError(result):
    """Raises an appropriate BigQuery error given the json error result."""
    error = result.get('error', {}).get('errors', [{}])[0]
    raise BigqueryError.Create(error, result, [])

  @staticmethod
  def IsFailedJob(job):
    """Predicate to determine whether or not a job failed."""
    return 'errorResult' in job.get('status', {})

  @staticmethod
  def RaiseIfJobError(job):
    """Raises a BigQueryError if the job is in an error state.

    Args:
      job: a Job resource.

    Returns:
      job, if it is not in an error state.

    Raises:
      BigqueryError: A BigqueryError instance based on the job's error
      description.
    """
    if BigqueryClient.IsFailedJob(job):
      error = job['status']['errorResult']
      error_ls = job['status'].get('errors', [])
      raise BigqueryError.Create(
          error, error, error_ls,
          job_ref=BigqueryClient.ConstructObjectReference(job))
    return job

  @staticmethod
  def GetJobTypeName(job_info):
    """Helper for job printing code."""
    job_names = set(('extract', 'load', 'query', 'copy'))
    try:
      return set(job_info.get('configuration', {}).keys()).intersection(
          job_names).pop()
    except KeyError:
      return None

  @staticmethod
  def ProcessSources(source_string):
    """Take a source string and return a list of URIs.

    The list will consist of either a single local filename, which
    we check exists and is a file, or a list of gs:// uris.

    Args:
      source_string: A comma-separated list of URIs.

    Returns:
      List of one or more valid URIs, as strings.

    Raises:
      BigqueryClientError: if no valid list of sources can be determined.
    """
    sources = [source.strip() for source in source_string.split(',')]
    gs_uris = [
        source for source in sources if source.startswith(_GCS_SCHEME_PREFIX)
    ]
    if not sources:
      raise BigqueryClientError('No sources specified')
    if gs_uris:
      if len(gs_uris) != len(sources):
        raise BigqueryClientError(
            'All URIs must begin with "{}" if any do.'.format(
                _GCS_SCHEME_PREFIX))
      return sources
    else:
      source = sources[0]
      if len(sources) > 1:
        raise BigqueryClientError(
            'Local upload currently supports only one file, found %d' % (
                len(sources),))
      if not os.path.exists(source):
        raise BigqueryClientError('Source file not found: %s' % (source,))
      if not os.path.isfile(source):
        raise BigqueryClientError('Source path is not a file: %s' % (source,))
    return sources

  @staticmethod
  def ReadSchema(schema):
    """Create a schema from a string or a filename.

    If schema does not contain ':' and is the name of an existing
    file, read it as a JSON schema. If not, it must be a
    comma-separated list of fields in the form name:type.

    Args:
      schema: A filename or schema.

    Returns:
      The new schema (as a dict).

    Raises:
      BigquerySchemaError: If the schema is invalid or the filename does
          not exist.
    """

    if schema.startswith(_GCS_SCHEME_PREFIX):
      raise BigquerySchemaError('Cannot load schema files from GCS.')
    def NewField(entry):
      name, _, field_type = entry.partition(':')
      if entry.count(':') > 1 or not name.strip():
        raise BigquerySchemaError('Invalid schema entry: %s' % (entry,))
      return {
          'name': name.strip(),
          'type': field_type.strip().upper() or 'STRING',
          }

    if not schema:
      raise BigquerySchemaError('Schema cannot be empty')
    elif os.path.exists(schema):
      with open(schema) as f:
        try:
          loaded_json = json.load(f)
        except ValueError as e:
          raise BigquerySchemaError(
              ('Error decoding JSON schema from file %s: %s\n'
               'To specify a one-column schema, use "name:string".') % (
                   schema, e))
      if not isinstance(loaded_json, list):
        raise BigquerySchemaError(
            'Error in "%s": Table schemas must be specified as JSON lists.' %
            schema)
      return loaded_json
    elif re.match(r'[./\\]', schema) is not None:
      # We have something that looks like a filename, but we didn't
      # find it. Tell the user about the problem now, rather than wait
      # for a round-trip to the server.
      raise BigquerySchemaError(
          ('Error reading schema: "%s" looks like a filename, '
           'but was not found.') % (schema,))
    else:
      return [NewField(entry) for entry in schema.split(',')]

  @staticmethod
  def _KindToName(kind):
    """Convert a kind to just a type name."""
    return kind.partition('#')[2]

  @staticmethod
  def FormatInfoByType(object_info, object_type):
    """Format a single object_info (based on its 'kind' attribute)."""
    if object_type == ApiClientHelper.JobReference:
      return BigqueryClient.FormatJobInfo(object_info)
    elif object_type == ApiClientHelper.ProjectReference:
      return BigqueryClient.FormatProjectInfo(object_info)
    elif object_type == ApiClientHelper.DatasetReference:
      return BigqueryClient.FormatDatasetInfo(object_info)
    elif object_type == ApiClientHelper.TableReference:
      return BigqueryClient.FormatTableInfo(object_info)
    elif object_type == ApiClientHelper.ModelReference:
      return BigqueryClient.FormatModelInfo(object_info)
    elif object_type == ApiClientHelper.RoutineReference:
      return BigqueryClient.FormatRoutineInfo(object_info)
    elif object_type == ApiClientHelper.TransferConfigReference:
      return BigqueryClient.FormatTransferConfigInfo(object_info)
    elif object_type == ApiClientHelper.TransferRunReference:
      return BigqueryClient.FormatTransferRunInfo(object_info)
    elif object_type == ApiClientHelper.TransferLogReference:
      return BigqueryClient.FormatTrasferLogInfo(object_info)
    elif object_type == ApiClientHelper.EncryptionServiceAccount:
      return object_info
    elif issubclass(object_type, ApiClientHelper.ReservationReference):
      return BigqueryClient.FormatReservationInfo(
          reservation=object_info, reference_type=object_type)
    elif object_type == ApiClientHelper.CapacityCommitmentReference:
      return BigqueryClient.FormatCapacityCommitmentInfo(object_info)
    elif object_type == ApiClientHelper.ReservationAssignmentReference:
      return BigqueryClient.FormatReservationAssignmentInfo(object_info)
    elif object_type == ApiClientHelper.ConnectionReference:
      return BigqueryClient.FormatConnectionInfo(object_info)
    else:
      raise ValueError('Unknown object type: %s' % (object_type,))

  @staticmethod
  def FormatJobInfo(job_info):
    """Prepare a job_info for printing.

    Arguments:
      job_info: Job dict to format.

    Returns:
      The new job_info.
    """
    result = job_info.copy()
    reference = BigqueryClient.ConstructObjectReference(result)
    result.update(dict(reference))
    stats = result.get('statistics', {})

    result['Job Type'] = BigqueryClient.GetJobTypeName(result)

    result['State'] = result['status']['state']
    if 'user_email' in result:
      result['User Email'] = result['user_email']
    if result['State'] == 'DONE':
      try:
        BigqueryClient.RaiseIfJobError(result)
        result['State'] = 'SUCCESS'
      except BigqueryError:
        result['State'] = 'FAILURE'

    if 'startTime' in stats:
      start = int(stats['startTime']) / 1000
      if 'endTime' in stats:
        duration_seconds = int(stats['endTime']) / 1000 - start
        result['Duration'] = str(datetime.timedelta(seconds=duration_seconds))
      result['Start Time'] = BigqueryClient.FormatTime(start)

    query_stats = stats.get('query', {})
    if 'totalBytesProcessed' in query_stats:
      result['Bytes Processed'] = query_stats['totalBytesProcessed']
    if 'totalBytesBilled' in query_stats:
      result['Bytes Billed'] = query_stats['totalBytesBilled']
    if 'billingTier' in query_stats:
      result['Billing Tier'] = query_stats['billingTier']
    config = result.get('configuration', {})
    if 'labels' in config:
      result['Labels'] = _FormatLabels(config['labels'])
    if 'numDmlAffectedRows' in query_stats:
      result['Affected Rows'] = query_stats['numDmlAffectedRows']
    if 'ddlOperationPerformed' in query_stats:
      result['DDL Operation Performed'] = query_stats['ddlOperationPerformed']
    if 'ddlTargetTable' in query_stats:
      result['DDL Target Table'] = dict(query_stats['ddlTargetTable'])
    if 'ddlTargetRoutine' in query_stats:
      result['DDL Target Routine'] = dict(query_stats['ddlTargetRoutine'])
    if 'ddlTargetRowAccessPolicy' in query_stats:
      result['DDL Target Row Access Policy'] = dict(
          query_stats['ddlTargetRowAccessPolicy'])
    if 'ddlAffectedRowAccessPolicyCount' in query_stats:
      result['DDL Affected Row Access Policy Count'] = query_stats[
          'ddlAffectedRowAccessPolicyCount']
    if ('statementType' in query_stats and
        query_stats['statementType'] == 'ASSERT'):
      result['Assertion'] = True
    return result

  @staticmethod
  def FormatProjectInfo(project_info):
    """Prepare a project_info for printing.

    Arguments:
      project_info: Project dict to format.

    Returns:
      The new project_info.
    """
    result = project_info.copy()
    reference = BigqueryClient.ConstructObjectReference(result)
    result.update(dict(reference))
    return result

  @staticmethod
  def FormatModelInfo(model_info):
    """Prepare a model for printing.

    Arguments:
      model_info: Model dict to format.

    Returns:
      A dictionary of model properties.
    """
    result = {}
    result['Id'] = model_info['modelReference']['modelId']
    result['Model Type'] = ''
    if 'modelType' in model_info:
      result['Model Type'] = model_info['modelType']
    if 'labels' in model_info:
      result['Labels'] = _FormatLabels(model_info['labels'])
    if 'creationTime' in model_info:
      result['Creation Time'] = BigqueryClient.FormatTime(
          int(model_info['creationTime']) / 1000)
    if 'expirationTime' in model_info:
      result['Expiration Time'] = BigqueryClient.FormatTime(
          int(model_info['expirationTime']) / 1000)
    if 'featureColumns' in model_info:
      result['Feature Columns'] = _FormatStandardSqlFields(
          model_info['featureColumns'])
    if 'labelColumns' in model_info:
      result['Label Columns'] = _FormatStandardSqlFields(
          model_info['labelColumns'])
    if 'encryptionConfiguration' in model_info:
      result['kmsKeyName'] = model_info['encryptionConfiguration']['kmsKeyName']
    return result

  @staticmethod
  def FormatRoutineDataType(data_type):
    """Converts a routine data type to a pretty string representation.

    Arguments:
      data_type: Routine data type dict to format.

    Returns:
      A formatted string.
    """
    type_kind = data_type['typeKind']
    if type_kind == 'ARRAY':
      return '{}<{}>'.format(
          type_kind,
          BigqueryClient.FormatRoutineDataType(data_type['arrayElementType']))
    elif type_kind == 'STRUCT':
      struct_fields = [
          '{} {}'.format(field['name'],
                         BigqueryClient.FormatRoutineDataType(field['type']))
          for field in data_type['structType']['fields']
      ]
      return '{}<{}>'.format(type_kind, ', '.join(struct_fields))
    else:
      return type_kind

  @staticmethod
  def FormatRoutineArgumentInfo(argument):
    """Converts a routine argument to a pretty string representation.

    Arguments:
      argument: Routine argument dict to format.

    Returns:
      A formatted string.
    """
    if 'dataType' in argument:
      display_type = BigqueryClient.FormatRoutineDataType(argument['dataType'])
    elif argument.get('argumentKind') == 'ANY_TYPE':
      display_type = 'ANY TYPE'

    if 'name' in argument:
      argument_mode = ''
      if ('mode' in argument):
        argument_mode = argument['mode'] + ' '
      return '{}{} {}'.format(argument_mode, argument['name'], display_type)
    else:
      return display_type

  @staticmethod
  def FormatRoutineInfo(routine_info):
    """Prepare a routine for printing.

    Arguments:
      routine_info: Routine dict to format.

    Returns:
      A dictionary of routine properties.
    """
    result = {}
    result['Id'] = routine_info['routineReference']['routineId']
    result['Routine Type'] = routine_info['routineType']
    result['Language'] = routine_info.get('language', '')
    signature = '()'
    return_type = routine_info.get('returnType')
    if 'arguments' in routine_info:
      argument_list = routine_info['arguments']
      signature = '({})'.format(', '.join(
          BigqueryClient.FormatRoutineArgumentInfo(argument)
          for argument in argument_list))
    if return_type:
      signature = '{} -> {}'.format(
          signature, BigqueryClient.FormatRoutineDataType(return_type))
    if return_type or ('arguments' in routine_info):
      result['Signature'] = signature
    if 'definitionBody' in routine_info:
      result['Definition'] = routine_info['definitionBody']
    if 'creationTime' in routine_info:
      result['Creation Time'] = BigqueryClient.FormatTime(
          int(routine_info['creationTime']) / 1000)
    if 'lastModifiedTime' in routine_info:
      result['Last Modified Time'] = BigqueryClient.FormatTime(
          int(routine_info['lastModifiedTime']) / 1000)
    return result

  @staticmethod
  def FormatDatasetInfo(dataset_info):
    """Prepare a dataset_info for printing.

    Arguments:
      dataset_info: Dataset dict to format.

    Returns:
      The new dataset_info.
    """
    result = dataset_info.copy()
    reference = BigqueryClient.ConstructObjectReference(result)
    result.update(dict(reference))
    if 'lastModifiedTime' in result:
      result['Last modified'] = BigqueryClient.FormatTime(
          int(result['lastModifiedTime']) / 1000)
    if 'access' in result:
      result['ACLs'] = BigqueryClient.FormatAcl(result['access'])
    if 'labels' in result:
      result['Labels'] = _FormatLabels(result['labels'])
    if 'defaultEncryptionConfiguration' in result:
      result['kmsKeyName'] = result['defaultEncryptionConfiguration'][
          'kmsKeyName']
    return result

  @staticmethod
  def FormatTableInfo(table_info):
    """Prepare a table_info for printing.

    Arguments:
      table_info: Table dict to format.

    Returns:
      The new table_info.
    """
    result = table_info.copy()
    reference = BigqueryClient.ConstructObjectReference(result)
    result.update(dict(reference))
    if 'lastModifiedTime' in result:
      result['Last modified'] = BigqueryClient.FormatTime(
          int(result['lastModifiedTime']) / 1000)
    if 'schema' in result:
      result['Schema'] = BigqueryClient.FormatSchema(result['schema'])
    if 'numBytes' in result:
      result['Total Bytes'] = result['numBytes']
    if 'numRows' in result:
      result['Total Rows'] = result['numRows']
    if 'expirationTime' in result:
      result['Expiration'] = BigqueryClient.FormatTime(
          int(result['expirationTime']) / 1000)
    if 'labels' in result:
      result['Labels'] = _FormatLabels(result['labels'])
    if 'timePartitioning' in result:
      if 'type' in result['timePartitioning']:
        result['Time Partitioning'] = result['timePartitioning']['type']
      else:
        result['Time Partitioning'] = 'DAY'
      extra_info = []
      if 'field' in result['timePartitioning']:
        partitioning_field = result['timePartitioning']['field']
        extra_info.append('field: %s' % partitioning_field)
      if 'expirationMs' in result['timePartitioning']:
        expiration_ms = int(result['timePartitioning']['expirationMs'])
        extra_info.append('expirationMs: %d' % (expiration_ms,))
      if extra_info:
        result['Time Partitioning'] += (' (%s)' % (', '.join(extra_info),))
    if 'clustering' in result:
      if 'fields' in result['clustering']:
        result['Clustered Fields'] = ', '.join(result['clustering']['fields'])
    if 'type' in result:
      result['Type'] = result['type']
      if 'view' in result and 'query' in result['view']:
        result['Query'] = result['view']['query']
      if 'materializedView' in result and 'query' in result['materializedView']:
        result['Query'] = result['materializedView']['query']
        if 'enableRefresh' in result['materializedView']:
          result['Enable Refresh'] = result['materializedView']['enableRefresh']
        if 'refreshIntervalMs' in result['materializedView']:
          result['Refresh Interval Ms'] = result['materializedView'][
              'refreshIntervalMs']
        if ('lastRefreshTime' in result['materializedView'] and
            result['materializedView']['lastRefreshTime'] != '0'):
          result['Last Refresh Time'] = BigqueryClient.FormatTime(
              int(result['materializedView']['lastRefreshTime']) / 1000)
      if result['type'] == 'EXTERNAL':
        if 'externalDataConfiguration' in result:
          result['Total URIs'] = len(
              result['externalDataConfiguration']['sourceUris'])
    if 'encryptionConfiguration' in result:
      result['kmsKeyName'] = result['encryptionConfiguration']['kmsKeyName']
    if 'snapshotDefinition' in result:
      result['Base Table'] = result['snapshotDefinition']['baseTableReference']
      result['Snapshot TimeStamp'] = result['snapshotDefinition'][
          'snapshotTime']
    return result


  @staticmethod
  def FormatTransferConfigInfo(transfer_config_info):
    """Prepare transfer config info for printing.

    Arguments:
      transfer_config_info: transfer config info to format.

    Returns:
      The new transfer config info.
    """

    result = {}
    for key, value in six.iteritems(transfer_config_info):
      result[key] = value

    return result

  @staticmethod
  def FormatTrasferLogInfo(transfer_log_info):
    """Prepare transfer log info for printing.

    Arguments:
      transfer_log_info: transfer log info to format.

    Returns:
      The new transfer config log.
    """
    result = {}
    for key, value in six.iteritems(transfer_log_info):
      result[key] = value

    return result

  @staticmethod
  def FormatTransferRunInfo(transfer_run_info):
    """Prepare transfer run info for printing.

    Arguments:
      transfer_run_info: transfer run info to format.

    Returns:
      The new transfer run info.
    """
    result = {}
    for key, value in six.iteritems(transfer_run_info):
      if key in BigqueryClient.columns_to_include_for_transfer_run:
        result[key] = value
    return result

  @staticmethod
  def FormatReservationInfo(reservation, reference_type):
    """Prepare a reservation for printing.

    Arguments:
      reservation: reservation to format.
      reference_type: Type of reservation.

    Returns:
      A dictionary of reservation properties.
    """
    result = {}
    for key, value in six.iteritems(reservation):
      if key == 'name':
        project_id, location, reservation_id = _ParseReservationPath(value)
        reference = ApiClientHelper.ReservationReference.Create(
            projectId=project_id,
            location=location,
            reservationId=reservation_id)
        result[key] = reference.__str__()
      else:
        result[key] = value
    # Default values not passed along in the response.
    if 'slotCapacity' not in list(result.keys()):
      result['slotCapacity'] = '0'
    if 'ignoreIdleSlots' not in list(result.keys()):
      result['ignoreIdleSlots'] = 'False'
    if (reference_type == ApiClientHelper.AutoscaleAlphaReservationReference and
        'autoscale' in list(result.keys())):
      if 'maxSlots' in list(result['autoscale'].keys()):
        result['autoscaleMaxSlots'] = result['autoscale']['maxSlots']
      result['autoscaleCurrentSlots'] = '0'
      if 'currentSlots' in list(result['autoscale'].keys()):
        result['autoscaleCurrentSlots'] = result['autoscale']['currentSlots']
      result.pop('autoscale', None)
    return result

  @staticmethod
  def FormatCapacityCommitmentInfo(capacity_commitment):
    """Prepare a capacity commitment for printing.

    Arguments:
      capacity_commitment: capacity commitment to format.

    Returns:
      A dictionary of capacity commitment properties.
    """
    result = {}
    for key, value in six.iteritems(capacity_commitment):
      if key == 'name':
        project_id, location, capacity_commitment_id = _ParseCapacityCommitmentPath(
            value)
        reference = ApiClientHelper.CapacityCommitmentReference.Create(
            projectId=project_id,
            location=location,
            capacityCommitmentId=capacity_commitment_id)
        result[key] = reference.__str__()
      else:
        result[key] = value
    # Default values not passed along in the response.
    if 'slotCount' not in list(result.keys()):
      result['slotCount'] = '0'
    return result

  @staticmethod
  def FormatReservationAssignmentInfo(reservation_assignment):
    """Prepare a reservation_assignment for printing.

    Arguments:
      reservation_assignment: reservation_assignment to format.

    Returns:
      A dictionary of reservation_assignment properties.
    """
    result = {}
    for key, value in six.iteritems(reservation_assignment):
      if key == 'name':
        project_id, location, reservation_id, reservation_assignment_id = _ParseReservationAssignmentPath(
            value)
        reference = ApiClientHelper.ReservationAssignmentReference.Create(
            projectId=project_id,
            location=location,
            reservationId=reservation_id,
            reservationAssignmentId=reservation_assignment_id)
        result[key] = reference.__str__()
      else:
        result[key] = value
    return result

  @staticmethod
  def GetConnectionType(connection):
    for t, p in six.iteritems(CONNECTION_TYPE_TO_PROPERTY_MAP):
      if p in connection:
        return t
    return None

  @staticmethod
  def FormatConnectionInfo(connection):
    """Prepare a connection object for printing.

    Arguments:
      connection: connection to format.

    Returns:
      A dictionary of connection properties.
    """
    result = {}
    for key, value in six.iteritems(connection):
      if key == 'name':
        project_id, location, connection_id = _ParseConnectionPath(value)
        reference = ApiClientHelper.ConnectionReference.Create(
            projectId=project_id,
            location=location,
            connectionId=connection_id)
        result[key] = reference.__str__()
      elif key == 'lastModifiedTime':
        result['Last modified'] = BigqueryClient.FormatTime(int(value) / 1000)
      elif key in CONNECTION_PROPERTY_TO_TYPE_MAP:
        result['type'] = CONNECTION_PROPERTY_TO_TYPE_MAP.get(key)
        result['properties'] = json.dumps(value)
      else:
        result[key] = value
    result['hasCredential'] = connection.get('hasCredential', False)
    return result

  @staticmethod
  def ConstructObjectReference(object_info):
    """Construct a Reference from a server response."""
    if 'kind' in object_info:
      typename = BigqueryClient._KindToName(object_info['kind'])
      lower_camel = typename + 'Reference'
      if lower_camel not in object_info:
        raise ValueError('Cannot find %s in object of type %s: %s' % (
            lower_camel, typename, object_info))
    else:
      keys = [k for k in object_info if k.endswith('Reference')]
      if len(keys) != 1:
        raise ValueError('Expected one Reference, found %s: %s' % (
            len(keys), keys))
      lower_camel = keys[0]
    upper_camel = lower_camel[0].upper() + lower_camel[1:]
    reference_type = getattr(ApiClientHelper, upper_camel, None)
    if reference_type is None:
      raise ValueError('Unknown reference type: %s' % (typename,))
    return reference_type.Create(**object_info[lower_camel])

  @staticmethod
  def ConstructObjectInfo(reference):
    """Construct an Object from an ObjectReference."""
    typename = reference.__class__.__name__
    lower_camel = typename[0].lower() + typename[1:]
    return {lower_camel: dict(reference)}

  def _PrepareListRequest(self,
                          reference,
                          max_results=None,
                          page_token=None,
                          filter_expression=None):
    """Create and populate a list request."""
    request = dict(reference)
    if max_results is not None:
      request['maxResults'] = max_results
    if filter_expression is not None:
      request['filter'] = filter_expression
    if page_token is not None:
      request['pageToken'] = page_token
    return request

  def _PrepareTransferListRequest(self,
                                  reference,
                                  location,
                                  page_size=None,
                                  page_token=None,
                                  data_source_ids=None):
    """Create and populate a list request."""
    request = dict(parent=_FormatProjectIdentifierForTransfers(
        reference, location))
    if page_size is not None:
      request['pageSize'] = page_size
    if page_token is not None:
      request['pageToken'] = page_token
    if data_source_ids is not None:
      data_source_ids = data_source_ids.split(':')
      if data_source_ids[0] == 'dataSourceIds':
        data_source_ids = data_source_ids[1].split(',')
        request['dataSourceIds'] = data_source_ids
      else:
        raise BigqueryError(
            'Invalid filter flag values: \'%s\'. '
            'Expected format: \'--filter=dataSourceIds:id1,id2\''
            % data_source_ids[0])

    return request

  def _PrepareTransferRunListRequest(self,
                                     reference,
                                     run_attempt,
                                     max_results=None,
                                     page_token=None,
                                     states=None):
    """Create and populate a transfer run list request."""
    request = dict(parent=reference)
    request['runAttempt'] = run_attempt
    if max_results is not None:
      if max_results > _MAX_RESULTS:
        max_results = _MAX_RESULTS
      request['pageSize'] = max_results
    if states is not None:
      if 'states:' in states:
        try:
          states = states.split(':')[1].split(',')
          request['states'] = states
        except IndexError:
          raise BigqueryError('Invalid flag argument "' + states + '"')
      else:
        raise BigqueryError('Invalid flag argument "' + states + '"')
    if page_token is not None:
      request['pageToken'] = page_token
    return request

  def _PrepareListTransferLogRequest(self,
                                     reference,
                                     max_results=None,
                                     page_token=None,
                                     message_type=None):
    """Create and populate a transfer log list request."""
    request = dict(parent=reference)
    if max_results is not None:
      if max_results > _MAX_RESULTS:
        max_results = _MAX_RESULTS
      request['pageSize'] = max_results
    if page_token is not None:
      request['pageToken'] = page_token
    if message_type is not None:
      if 'messageTypes:' in message_type:
        try:
          message_type = message_type.split(':')[1].split(',')
          request['messageTypes'] = message_type
        except IndexError:
          raise BigqueryError('Invalid flag argument "' + message_type + '"')
      else:
        raise BigqueryError('Invalid flag argument "' + message_type + '"')
    return request

  def _NormalizeProjectReference(self, reference):
    if reference is None:
      try:
        return self.GetProjectReference()
      except BigqueryClientError:
        raise BigqueryClientError(
            'Project reference or a default project is required')
    return reference

  def ListJobRefs(self, **kwds):
    return list(
        map(  # pylint: disable=g-long-lambda
            BigqueryClient.ConstructObjectReference, self.ListJobs(**kwds)))

  def ListJobs(
      self,
      reference=None,
      max_results=None,
      page_token=None,
      state_filter=None,
      min_creation_time=None,
      max_creation_time=None,
      all_users=None,
      parent_job_id=None
  ):
    # pylint: disable=g-doc-args
    """Return a list of jobs.

    Args:
      reference: The ProjectReference to list jobs for.
      max_results: The maximum number of jobs to return.
      page_token: Current page token (optional).
      state_filter: A single state filter or a list of filters to
        apply. If not specified, no filtering is applied.
      min_creation_time: Timestamp in milliseconds. Only return jobs created
        after or at this timestamp.
      max_creation_time: Timestamp in milliseconds. Only return jobs created
        before or at this timestamp.
      all_users: Whether to list jobs for all users of the project. Requesting
        user must be an owner of the project to list all jobs.
      parent_job_id: Retrieve only child jobs belonging to this parent; None to
        retrieve top-level jobs.
    Returns:
      A list of jobs.
    """
    return self.ListJobsAndToken(
        reference,
        max_results,
        page_token,
        state_filter,
        min_creation_time,
        max_creation_time,
        all_users,
        parent_job_id
    )['results']

  def ListJobsAndToken(
      self,
      reference=None,
      max_results=None,
      page_token=None,
      state_filter=None,
      min_creation_time=None,
      max_creation_time=None,
      all_users=None,
      parent_job_id=None
  ):
    # pylint: disable=g-doc-args
    """Return a list of jobs.

    Args:
      reference: The ProjectReference to list jobs for.
      max_results: The maximum number of jobs to return.
      page_token: Current page token (optional).
      state_filter: A single state filter or a list of filters to
        apply. If not specified, no filtering is applied.
      min_creation_time: Timestamp in milliseconds. Only return jobs created
        after or at this timestamp.
      max_creation_time: Timestamp in milliseconds. Only return jobs created
        before or at this timestamp.
      all_users: Whether to list jobs for all users of the project. Requesting
        user must be an owner of the project to list all jobs.
      parent_job_id: Retrieve only child jobs belonging to this parent; None to
        retrieve top-level jobs.

    Returns:
      A dict that contains enytries:
        'results': a list of jobs
        'token': nextPageToken for the last page, if present.
    """
    reference = self._NormalizeProjectReference(reference)
    _Typecheck(reference, ApiClientHelper.ProjectReference, method='ListJobs')
    if max_results is not None:
      if max_results > _MAX_RESULTS:
        max_results = _MAX_RESULTS
    request = self._PrepareListRequest(reference, max_results, page_token)
    if state_filter is not None:
      # The apiclient wants enum values as lowercase strings.
      if isinstance(state_filter, six.string_types):
        state_filter = state_filter.lower()
      else:
        state_filter = [s.lower() for s in state_filter]
    _ApplyParameters(
        request,
        projection='full',
        state_filter=state_filter,
        all_users=all_users,
        parent_job_id=parent_job_id
    )
    if min_creation_time is not None:
      request['minCreationTime'] = min_creation_time
    if max_creation_time is not None:
      request['maxCreationTime'] = max_creation_time
    result = self.apiclient.jobs().list(**request).execute()
    results = result.get('jobs', [])
    if max_results is not None:
      while 'nextPageToken' in result and len(results) < max_results:
        request['maxResults'] = max_results - len(results)
        request['pageToken'] = result['nextPageToken']
        result = self.apiclient.jobs().list(**request).execute()
        results.extend(result.get('jobs', []))
    if 'nextPageToken' in result:
      return dict(results=results, token=result['nextPageToken'])
    return dict(results=results)

  def ListTransferConfigs(self,
                          reference=None,
                          location=None,
                          page_size=None,
                          page_token=None,
                          data_source_ids=None):
    """Return a list of transfer configurations.

    Args:
      reference: The ProjectReference to list transfer configurations for.
      location: The location id, e.g. 'us' or 'eu'.
      page_size: The maximum number of transfer configurations to return.
      page_token: Current page token (optional).
      data_source_ids: The dataSourceIds to display transfer configurations for.

    Returns:
      A list of transfer configurations.
    """
    results = None
    client = self.GetTransferV1ApiClient()
    _Typecheck(reference, ApiClientHelper.ProjectReference,
               method='ListTransferConfigs')
    if page_size is not None:
      if page_size > _MAX_RESULTS:
        page_size = _MAX_RESULTS
    request = self._PrepareTransferListRequest(reference, location, page_size,
                                               page_token, data_source_ids)
    if request:
      _ApplyParameters(request)
      result = client.projects().locations().transferConfigs().list(
          **request).execute()
      results = result.get('transferConfigs', [])
      if page_size is not None:
        while 'nextPageToken' in result and len(results) < page_size:
          request = self._PrepareTransferListRequest(
              reference, location, page_size - len(results),
              result['nextPageToken'], data_source_ids)
          if request:
            _ApplyParameters(request)
            result = client.projects().locations().transferConfigs().list(
                **request).execute()
            results.extend(result.get('nextPageToken', []))
          else:
            return
      if len(results) < 1:
        logging.info('There are no transfer configurations to be shown.')
      if result.get('nextPageToken'):
        return (results, result.get('nextPageToken'))
    return (results,)

  def ListTransferRuns(self, reference, run_attempt, max_results=None,
                       page_token=None, states=None):
    """Return a list of transfer runs.

    Args:
      reference: The ProjectReference to list transfer runs for.
      run_attempt: Which runs should be pulled. The default value is 'LATEST',
          which only returns the latest run per day. To return all runs,
          please specify 'RUN_ATTEMPT_UNSPECIFIED'.
      max_results: The maximum number of transfer runs to return (optional).
      page_token: Current page token (optional).
      states: States to filter transfer runs (optional).

    Returns:
      A list of transfer runs.
    """
    transfer_client = self.GetTransferV1ApiClient()
    _Typecheck(reference, ApiClientHelper.TransferRunReference,
               method='ListTransferRuns')
    reference = str(reference)
    request = self._PrepareTransferRunListRequest(reference, run_attempt,
                                                  max_results, page_token,
                                                  states)
    response = transfer_client.projects().locations().transferConfigs().runs(
    ).list(**request).execute()
    transfer_runs = response.get('transferRuns', [])
    if max_results is not None:
      while 'nextPageToken' in response and len(transfer_runs) < max_results:
        page_token = response.get('nextPageToken')
        max_results -= len(transfer_runs)
        request = self._PrepareTransferRunListRequest(reference, run_attempt,
                                                      max_results, page_token,
                                                      states)
        response = transfer_client.projects().locations().transferConfigs(
        ).runs().list(**request).execute()
        transfer_runs.extend(response.get('transferRuns', []))
      if response.get('nextPageToken'):
        return (transfer_runs, response.get('nextPageToken'))
    return (transfer_runs,)

  def ListTransferLogs(self, reference, message_type=None, max_results=None,
                       page_token=None):
    """Return a list of transfer run logs.

    Args:
      reference: The ProjectReference to list transfer run logs for.
      message_type: Message types to return.
      max_results: The maximum number of transfer run logs to return.
      page_token: Current page token (optional).

    Returns:
      A list of transfer run logs.
    """
    transfer_client = self.GetTransferV1ApiClient()
    reference = str(reference)
    request = self._PrepareListTransferLogRequest(reference,
                                                  max_results=max_results,
                                                  page_token=page_token,
                                                  message_type=message_type)
    response = (transfer_client.projects().locations().transferConfigs().runs()
                .transferLogs().list(**request).execute())
    transfer_logs = response.get('transferMessages', [])
    if max_results is not None:
      while 'nextPageToken' in response and len(transfer_logs) < max_results:
        page_token = response['nextPageToken']
        max_results -= len(transfer_logs)
        request = self._PrepareListTransferLogRequest(reference,
                                                      max_results=max_results,
                                                      page_token=page_token,
                                                      message_type=message_type)
        response = (
            transfer_client.projects().locations().transferConfigs().runs()
            .transferLogs().list(**request).execute())
        transfer_logs.extend(response.get('transferMessages', []))
    if response.get('nextPageToken'):
      return (transfer_logs, response.get('nextPageToken'))
    return (transfer_logs,)

  def ListProjectRefs(self, **kwds):
    """List the project references this user has access to."""
    return list(
        map(  # pylint: disable=g-long-lambda
            BigqueryClient.ConstructObjectReference, self.ListProjects(**kwds)))

  def ListProjects(self, max_results=None, page_token=None):
    """List the projects this user has access to."""
    request = self._PrepareListRequest({}, max_results, page_token)
    result = self.apiclient.projects().list(**request).execute()
    return result.get('projects', [])

  def ListDatasetRefs(self, **kwds):
    return list(
        map(  # pylint: disable=g-long-lambda
            BigqueryClient.ConstructObjectReference, self.ListDatasets(**kwds)))

  def ListDatasets(self,
                   reference=None,
                   max_results=None,
                   page_token=None,
                   list_all=None,
                   filter_expression=None):
    """List the datasets associated with this reference."""
    reference = self._NormalizeProjectReference(reference)
    _Typecheck(reference, ApiClientHelper.ProjectReference,
               method='ListDatasets')
    request = self._PrepareListRequest(reference,
                                       max_results,
                                       page_token,
                                       filter_expression)
    if list_all is not None:
      request['all'] = list_all
    result = self.apiclient.datasets().list(**request).execute()
    results = result.get('datasets', [])
    if max_results is not None:
      while 'nextPageToken' in result and len(results) < max_results:
        request['maxResults'] = max_results - len(results)
        request['pageToken'] = result['nextPageToken']
        result = self.apiclient.datasets().list(**request).execute()
        results.extend(result.get('datasets', []))
    return results

  def ListTableRefs(self, **kwds):
    return list(
        map(  # pylint: disable=g-long-lambda
            BigqueryClient.ConstructObjectReference, self.ListTables(**kwds)))

  def ListTables(self, reference, max_results=None, page_token=None):
    """List the tables associated with this reference."""
    _Typecheck(reference, ApiClientHelper.DatasetReference,
               method='ListTables')
    request = self._PrepareListRequest(reference, max_results, page_token)
    result = self.apiclient.tables().list(**request).execute()
    results = result.get('tables', [])
    if max_results is not None:
      while 'nextPageToken' in result and len(results) < max_results:
        request['maxResults'] = max_results - len(results)
        request['pageToken'] = result['nextPageToken']
        result = self.apiclient.tables().list(**request).execute()
        results.extend(result.get('tables', []))
    return results

  def ListModels(self, reference, max_results, page_token):
    """Lists models for the given dataset reference.

    Arguments:
      reference: Reference to the dataset.
      max_results: Number of results to return.
      page_token: Token to retrieve the next page of results.

    Returns:
      A dict that contains entries:
        'results': a list of models
        'token': nextPageToken for the last page, if present.
    """
    return self.GetModelsApiClient().models().list(
        projectId=reference.projectId,
        datasetId=reference.datasetId,
        maxResults=max_results,
        pageToken=page_token).execute()

  def ListRoutines(self, reference, max_results, page_token, filter_expression):
    """Lists routines for the given dataset reference.

    Arguments:
      reference: Reference to the dataset.
      max_results: Number of results to return.
      page_token: Token to retrieve the next page of results.
      filter_expression: An expression for filtering routines.

    Returns:
      A dict that contains entries:
        'routines': a list of routines.
        'token': nextPageToken for the last page, if present.
    """
    return self.GetRoutinesApiClient().routines().list(
        projectId=reference.projectId,
        datasetId=reference.datasetId,
        maxResults=max_results,
        pageToken=page_token,
        filter=filter_expression).execute()

  def GetDatasetIAMPolicy(self, reference):
    """Gets IAM policy for the given dataset resource.

    Arguments:
      reference: the DatasetReference for the dataset resource.

    Returns:
      The IAM policy attached to the given dataset resource.

    Raises:
      TypeError: if reference is not a DatasetReference.
    """
    _Typecheck(
        reference,
        ApiClientHelper.DatasetReference,
        method='GetDatasetIAMPolicy')
    formatted_resource = ('projects/%s/datasets/%s' %
                          (reference.projectId, reference.datasetId))
    return self.GetIAMPolicyApiClient().datasets().getIamPolicy(
        resource=formatted_resource).execute()

  def GetTableIAMPolicy(self, reference):
    """Gets IAM policy for the given table resource.

    Arguments:
      reference: the TableReference for the table resource.

    Returns:
      The IAM policy attached to the given table resource.

    Raises:
      TypeError: if reference is not a TableReference.
    """
    _Typecheck(
        reference, ApiClientHelper.TableReference, method='GetTableIAMPolicy')
    formatted_resource = (
        'projects/%s/datasets/%s/tables/%s' %
        (reference.projectId, reference.datasetId, reference.tableId))
    return self.GetIAMPolicyApiClient().tables().getIamPolicy(
        resource=formatted_resource).execute()

  def SetDatasetIAMPolicy(self, reference, policy):
    """Sets IAM policy for the given dataset resource.

    Arguments:
      reference: the DatasetReference for the dataset resource.
      policy: The policy string in JSON format.

    Returns:
      The updated IAM policy attached to the given dataset resource.

    Raises:
      TypeError: if reference is not a DatasetReference.
    """
    _Typecheck(
        reference,
        ApiClientHelper.DatasetReference,
        method='SetDatasetIAMPolicy')
    formatted_resource = ('projects/%s/datasets/%s' %
                          (reference.projectId, reference.datasetId))
    request = {'policy': policy}
    return self.GetIAMPolicyApiClient().datasets().setIamPolicy(
        body=request, resource=formatted_resource).execute()

  def SetTableIAMPolicy(self, reference, policy):
    """Sets IAM policy for the given table resource.

    Arguments:
      reference: the TableReference for the table resource.
      policy: The policy string in JSON format.

    Returns:
      The updated IAM policy attached to the given table resource.

    Raises:
      TypeError: if reference is not a TableReference.
    """
    _Typecheck(
        reference, ApiClientHelper.TableReference, method='SetTableIAMPolicy')
    formatted_resource = (
        'projects/%s/datasets/%s/tables/%s' %
        (reference.projectId, reference.datasetId, reference.tableId))
    request = {'policy': policy}
    return self.GetIAMPolicyApiClient().tables().setIamPolicy(
        body=request, resource=formatted_resource).execute()

  #################################
  ##       Transfer run
  #################################
  def StartManualTransferRuns(self, reference, start_time, end_time, run_time):
    """Starts manual transfer runs.

    Args:
      reference: Transfer configuration name for the run.
      start_time: Start time of the range of transfer runs.
      end_time: End time of the range of transfer runs.
      run_time: Specific time for a transfer run.

    Returns:
      The list of started transfer runs.
    """
    _Typecheck(reference, ApiClientHelper.TransferConfigReference,
               method='StartManualTransferRuns')
    transfer_client = self.GetTransferV1ApiClient()
    parent = str(reference)

    if run_time:
      body = {'requestedRunTime': run_time}
    else:
      body = {
          'requestedTimeRange': {
              'startTime': start_time,
              'endTime': end_time
          }
      }

    configs_request = transfer_client.projects().locations().transferConfigs()
    response = configs_request.startManualRuns(parent=parent,
                                               body=body).execute()

    return response.get('runs')

  #################################
  ## Table and dataset management
  #################################

  def CopyTable(
      self,
      source_references,
      dest_reference,
      create_disposition=None,
      write_disposition=None,
      ignore_already_exists=False,
      encryption_configuration=None,
      operation_type='COPY',
      destination_expiration_time=None,
      **kwds):
    """Copies a table.

    Args:
      source_references: TableReferences of source tables.
      dest_reference: TableReference of destination table.
      create_disposition: Optional. Specifies the create_disposition for
          the dest_reference.
      write_disposition: Optional. Specifies the write_disposition for
          the dest_reference.
      ignore_already_exists: Whether to ignore "already exists" errors.
      encryption_configuration: Optional. Allows user to encrypt the table from
          the copy table command with Cloud KMS key. Passed as a dictionary in
          the following format: {'kmsKeyName': 'destination_kms_key'}
      **kwds: Passed on to ExecuteJob.

    Returns:
      The job description, or None for ignored errors.

    Raises:
      BigqueryDuplicateError: when write_disposition 'WRITE_EMPTY' is
        specified and the dest_reference table already exists.
    """
    for src_ref in source_references:
      _Typecheck(src_ref, ApiClientHelper.TableReference,
                 method='CopyTable')
    _Typecheck(dest_reference, ApiClientHelper.TableReference,
               method='CopyTable')
    copy_config = {
        'destinationTable': dict(dest_reference),
        'sourceTables': [dict(src_ref) for src_ref in source_references],
        }
    if encryption_configuration:
      copy_config[
          'destinationEncryptionConfiguration'] = encryption_configuration

    if operation_type:
      copy_config['operationType'] = operation_type

    if destination_expiration_time:
      copy_config['destinationExpirationTime'] = destination_expiration_time

    _ApplyParameters(
        copy_config,
        create_disposition=create_disposition,
        write_disposition=write_disposition)

    try:
      return self.ExecuteJob({'copy': copy_config}, **kwds)
    except BigqueryDuplicateError as e:
      if ignore_already_exists:
        return None
      raise e

  def DatasetExists(self, reference):
    _Typecheck(reference, ApiClientHelper.DatasetReference,
               method='DatasetExists')
    try:
      self.apiclient.datasets().get(**dict(reference)).execute()
      return True
    except BigqueryNotFoundError:
      return False

  def TableExists(self, reference):
    _Typecheck(reference, ApiClientHelper.TableReference, method='TableExists')
    try:
      return self.apiclient.tables().get(**dict(reference)).execute()
    except BigqueryNotFoundError:
      return False

  def ModelExists(self, reference):
    _Typecheck(reference, ApiClientHelper.ModelReference, method='ModelExists')
    try:
      return self.GetModelsApiClient().models().get(
          projectId=reference.projectId,
          datasetId=reference.datasetId,
          modelId=reference.modelId).execute()
    except BigqueryNotFoundError:
      return False

  def RoutineExists(self, reference):
    _Typecheck(reference, ApiClientHelper.RoutineReference,
               method='RoutineExists')
    try:
      return self.GetRoutinesApiClient().routines().get(
          projectId=reference.projectId,
          datasetId=reference.datasetId,
          routineId=reference.routineId).execute()
    except BigqueryNotFoundError:
      return False

  def TransferExists(self, reference):
    _Typecheck(reference, ApiClientHelper.TransferConfigReference,
               method='TransferExists')
    try:
      transfer_client = self.GetTransferV1ApiClient()
      transfer_client.projects().locations().transferConfigs().get(
          name=reference.transferConfigName).execute()
      return True
    except BigqueryNotFoundError:
      return False

  def CreateDataset(self,
                    reference,
                    ignore_existing=False,
                    description=None,
                    display_name=None,
                    acl=None,
                    default_table_expiration_ms=None,
                    default_partition_expiration_ms=None,
                    data_location=None,
                    labels=None,
                    default_kms_key=None):
    """Create a dataset corresponding to DatasetReference.

    Args:
      reference: the DatasetReference to create.
      ignore_existing: (boolean, default False) If False, raise
        an exception if the dataset already exists.
      description: an optional dataset description.
      display_name: an optional friendly name for the dataset.
      acl: an optional ACL for the dataset, as a list of dicts.
      default_table_expiration_ms: Default expiration time to apply to
        new tables in this dataset.
      default_partition_expiration_ms: Default partition expiration time to
        apply to new partitioned tables in this dataset.
      data_location: Location where the data in this dataset should be
        stored. Must be either 'EU' or 'US'. If specified, the project that
        owns the dataset must be enabled for data location.
      labels: An optional dict of labels.
      default_kms_key: An optional kms dey that will apply to all newly created
        tables in the dataset, if no explicit key is supplied in the creating
        request.

    Raises:
      TypeError: if reference is not a DatasetReference.
      BigqueryDuplicateError: if reference exists and ignore_existing
         is False.
    """
    _Typecheck(reference, ApiClientHelper.DatasetReference,
               method='CreateDataset')

    body = BigqueryClient.ConstructObjectInfo(reference)
    if display_name is not None:
      body['friendlyName'] = display_name
    if description is not None:
      body['description'] = description
    if acl is not None:
      body['access'] = acl
    if default_table_expiration_ms is not None:
      body['defaultTableExpirationMs'] = default_table_expiration_ms
    if default_partition_expiration_ms is not None:
      body['defaultPartitionExpirationMs'] = default_partition_expiration_ms
    if default_kms_key is not None:
      body['defaultEncryptionConfiguration'] = {'kmsKeyName': default_kms_key}
    if data_location is not None:
      body['location'] = data_location
    if labels:
      body['labels'] = {}
      for label_key, label_value in labels.items():
        body['labels'][label_key] = label_value
    try:
      self.apiclient.datasets().insert(
          body=body,
          **dict(reference.GetProjectReference())).execute()
    except BigqueryDuplicateError:
      if not ignore_existing:
        raise

  def CreateTable(self,
                  reference,
                  ignore_existing=False,
                  schema=None,
                  description=None,
                  display_name=None,
                  expiration=None,
                  view_query=None,
                  materialized_view_query=None,
                  enable_refresh=None,
                  refresh_interval_ms=None,
                  external_data_config=None,
                  view_udf_resources=None,
                  use_legacy_sql=None,
                  labels=None,
                  time_partitioning=None,
                  clustering=None,
                  range_partitioning=None,
                  require_partition_filter=None,
                  destination_kms_key=None,
                  location=None):
    """Create a table corresponding to TableReference.

    Args:
      reference: the TableReference to create.
      ignore_existing: (boolean, default False) If False, raise
        an exception if the dataset already exists.
      schema: an optional schema for tables.
      description: an optional description for tables or views.
      display_name: an optional friendly name for the table.
      expiration: optional expiration time in milliseconds since the epoch for
        tables or views.
      view_query: an optional Sql query for views.
      materialized_view_query: an optional standard SQL query for materialized
        views.
      enable_refresh: for materialized views, an optional toggle to enable /
        disable automatic refresh when the base table is updated.
      refresh_interval_ms: for materialized views, an optional maximum frequency
        for automatic refreshes.
      external_data_config: defines a set of external resources used to create
        an external table. For example, a BigQuery table backed by CSV files
        in GCS.
      view_udf_resources: optional UDF resources used in a view.
      use_legacy_sql: Whether to use Legacy SQL. If not set, the default
        behavior is true.
      labels: an optional dict of labels to set on the table.
      time_partitioning: if set, enables time based partitioning on the table
        and configures the partitioning.
      clustering: if set, enables and configures clustering on the table.
      range_partitioning: if set, enables range partitioning on the table and
        configures the partitioning.
      require_partition_filter: if set, partition filter is required for
        queiries over this table.
      destination_kms_key: User specified KMS key for encryption.
      location: an optional location for which to create tables or views.

    Raises:
      TypeError: if reference is not a TableReference.
      BigqueryDuplicateError: if reference exists and ignore_existing
        is False.
    """
    _Typecheck(reference, ApiClientHelper.TableReference, method='CreateTable')

    try:
      body = BigqueryClient.ConstructObjectInfo(reference)
      if schema is not None:
        body['schema'] = {'fields': schema}
      if display_name is not None:
        body['friendlyName'] = display_name
      if description is not None:
        body['description'] = description
      if expiration is not None:
        body['expirationTime'] = expiration
      if view_query is not None:
        view_args = {'query': view_query}
        if view_udf_resources is not None:
          view_args['userDefinedFunctionResources'] = view_udf_resources
        body['view'] = view_args
        if use_legacy_sql is not None:
          view_args['useLegacySql'] = use_legacy_sql
      if materialized_view_query is not None:
        materialized_view_args = {'query': materialized_view_query}
        if enable_refresh is not None:
          materialized_view_args.update({'enableRefresh': enable_refresh})
        if refresh_interval_ms is not None:
          materialized_view_args.update(
              {'refreshIntervalMs': refresh_interval_ms})
        body['materializedView'] = materialized_view_args
      if external_data_config is not None:
        body['externalDataConfiguration'] = external_data_config
      if labels is not None:
        body['labels'] = labels
      if time_partitioning is not None:
        body['timePartitioning'] = time_partitioning
      if clustering is not None:
        body['clustering'] = clustering
      if range_partitioning is not None:
        body['rangePartitioning'] = range_partitioning
      if require_partition_filter is not None:
        body['requirePartitionFilter'] = require_partition_filter
      if destination_kms_key is not None:
        body['encryptionConfiguration'] = {'kmsKeyName': destination_kms_key}
      if location is not None:
        body['location'] = location
      self.apiclient.tables().insert(
          body=body,
          **dict(reference.GetDatasetReference())).execute()
    except BigqueryDuplicateError:
      if not ignore_existing:
        raise

  def UpdateTransferConfig(self,
                           reference,
                           target_dataset=None,
                           display_name=None,
                           refresh_window_days=None,
                           params=None,
                           auth_info=None,
                           service_account_name=None,
                           notification_pubsub_topic=None,
                           schedule_args=None):
    """Updates a transfer config.

    Args:
      reference: the TransferConfigReference to update.
      target_dataset: Optional updated target dataset.
      display_name: Optional change to the display name.
      refresh_window_days: Optional update to the refresh window days. Some
        data sources do not support this.
      params: Optional parameters to update.
      auth_info: A dict contains authorization info which can be either an
        authorization_code or a version_info that the user input if they want to
        update credentials.
      service_account_name: The service account that the user could act as and
        used as the credential to create transfer runs from the transfer config.
      notification_pubsub_topic: The Pub/Sub topic where notifications will be
        sent after transfer runs associated with this transfer config finish.
      schedule_args: Optional parameters to customize data transfer schedule.

    Raises:
      TypeError: if reference is not a TransferConfigReference.
      BigqueryNotFoundError: if dataset is not found
      BigqueryError: required field not given.
    """

    _Typecheck(reference, ApiClientHelper.TransferConfigReference,
               method='UpdateTransferConfig')
    project_reference = 'projects/' + (self.GetProjectReference().projectId)
    transfer_client = self.GetTransferV1ApiClient()
    current_config = transfer_client.projects().locations().transferConfigs(
    ).get(name=reference.transferConfigName).execute()
    update_mask = []
    update_items = {}
    update_items['dataSourceId'] = current_config['dataSourceId']
    if target_dataset:
      dataset_reference = self.GetDatasetReference(target_dataset)
      if self.DatasetExists(dataset_reference):
        update_items['destinationDatasetId'] = target_dataset
        update_mask.append('transfer_config.destination_dataset_id')
      else:
        raise BigqueryNotFoundError(
            'Unknown %r' % (dataset_reference,), {'reason': 'notFound'}, [])
      update_items['destinationDatasetId'] = target_dataset

    if display_name:
      update_mask.append('transfer_config.display_name')
      update_items['displayName'] = display_name

    data_source_retrieval = (project_reference + '/locations/-/dataSources/' +
                             current_config['dataSourceId'])
    data_source_info = transfer_client.projects().locations().dataSources().get(
        name=data_source_retrieval).execute()

    if params:
      update_items = self.ProcessParamsFlag(params, update_items)
      update_mask.append('transfer_config.params')

    # if refresh window provided, check that data source supports it
    if refresh_window_days:
      if refresh_window_days:
        update_items = self.ProcessRefreshWindowDaysFlag(
            refresh_window_days, data_source_info,
            update_items, current_config['dataSourceId'])
        update_mask.append('transfer_config.data_refresh_window_days')

    if schedule_args:
      if schedule_args.schedule is not None:
        # update schedule if a custom string was provided
        update_items['schedule'] = schedule_args.schedule
        update_mask.append('transfer_config.schedule')

      update_items['scheduleOptions'] = schedule_args.ToScheduleOptionsPayload(
          options_to_copy=current_config.get('scheduleOptions'))
      update_mask.append('transfer_config.scheduleOptions')

    if notification_pubsub_topic:
      update_items['notification_pubsub_topic'] = notification_pubsub_topic
      update_mask.append('transfer_config.notification_pubsub_topic')

    if auth_info is not None and AUTHORIZATION_CODE in auth_info:
      update_mask.append(AUTHORIZATION_CODE)

    if auth_info is not None and VERSION_INFO in auth_info:
      update_mask.append(VERSION_INFO)

    if service_account_name:
      update_mask.append('service_account_name')

    transfer_client.projects().locations().transferConfigs().patch(
        body=update_items,
        name=reference.transferConfigName,
        updateMask=','.join(update_mask),
        authorizationCode=(None if auth_info is None else
                           auth_info.get(AUTHORIZATION_CODE)),
        versionInfo=None if auth_info is None else auth_info.get(VERSION_INFO),
        serviceAccountName=service_account_name,
        x__xgafv='2').execute()

  def CreateTransferConfig(self,
                           reference,
                           data_source,
                           target_dataset=None,
                           display_name=None,
                           refresh_window_days=None,
                           params=None,
                           auth_info=None,
                           service_account_name=None,
                           notification_pubsub_topic=None,
                           schedule_args=None,
                           location=None):
    """Create a transfer config corresponding to TransferConfigReference.

    Args:
      reference: the TransferConfigReference to create.
      data_source: The data source for the transfer config.
      target_dataset: The dataset where the new transfer config will exist.
      display_name: An display name for the transfer config.
      refresh_window_days: Refresh window days for the transfer config.
      params: Parameters for the created transfer config. The parameters should
        be in JSON format given as a string. Ex: --params="{'param':'value'}".
        The params should be the required values needed for each data source and
        will vary.
      auth_info: A dict contains authorization info which can be either an
        authorization_code or a version_info that the user input if they need
        credentials.
      service_account_name: The service account that the user could act as and
        used as the credential to create transfer runs from the transfer config.
      notification_pubsub_topic: The Pub/Sub topic where notifications will be
        sent after transfer runs associated with this transfer config finish.
      schedule_args: Optional parameters to customize data transfer schedule.
      location: The location where the new transfer config will run.

    Raises:
      BigqueryNotFoundError: if a requested item is not found.
      BigqueryError: if a required field isn't provided.

    Returns:
      The generated transfer configuration name.
    """
    create_items = {}
    transfer_client = self.GetTransferV1ApiClient()

    # The backend will check if the dataset exists.
    if target_dataset:
      create_items['destinationDatasetId'] = target_dataset

    if display_name:
      create_items['displayName'] = display_name
    else:
      raise BigqueryError(
          'A display name must be provided.')
    data_sources_reference = (reference + '/locations/-/dataSources/'
                              + data_source)
    data_source_info = transfer_client.projects().locations().dataSources().get(
        name=data_sources_reference).execute()
    create_items['dataSourceId'] = data_source

    # if refresh window provided, check that data source supports it
    if refresh_window_days:
      create_items = self.ProcessRefreshWindowDaysFlag(
          refresh_window_days, data_source_info, create_items, data_source)

    # checks that all required params are given
    # if a param that isn't required is provided, it is ignored.
    if params:
      create_items = self.ProcessParamsFlag(params, create_items)
    else:
      raise BigqueryError(
          'Parameters must be provided.')

    if location:
      parent = reference + '/locations/' + location
    else:
      # The location is infererred by the data transfer service from the
      # dataset location.
      parent = reference + '/locations/-'

    if schedule_args:
      if schedule_args.schedule is not None:
        create_items['schedule'] = schedule_args.schedule
      create_items['scheduleOptions'] = schedule_args.ToScheduleOptionsPayload()

    if notification_pubsub_topic:
      create_items['notification_pubsub_topic'] = notification_pubsub_topic

    new_transfer_config = transfer_client.projects().locations(
    ).transferConfigs().create(
        parent=parent,
        body=create_items,
        authorizationCode=(None if auth_info is None else
                           auth_info.get(AUTHORIZATION_CODE)),
        versionInfo=None if auth_info is None else auth_info.get(VERSION_INFO),
        serviceAccountName=service_account_name).execute()

    return new_transfer_config['name']

  def ProcessParamsFlag(self, params, items):
    """Processes the params flag.

    Args:
      params: The user specified parameters. The parameters should be in JSON
      format given as a string. Ex: --params="{'param':'value'}".
      items: The body that contains information of all the flags set.

    Returns:
      items: The body after it has been updated with the params flag.

    Raises:
      BigqueryError: If there is an error with the given params.
    """
    try:
      parsed_params = json.loads(params)
    except:
      raise BigqueryError(
          'Parameters should be specified in JSON format'
          ' when creating the transfer configuration.')
    items['params'] = parsed_params
    return items

  def ProcessRefreshWindowDaysFlag(
      self, refresh_window_days, data_source_info, items, data_source):
    """Processes the Refresh Window Days flag.

    Args:
      refresh_window_days: The user specified refresh window days.
      data_source_info: The data source of the transfer config.
      items: The body that contains information of all the flags set.
      data_source: The data source of the transfer config.

    Returns:
      items: The body after it has been updated with the
      refresh window days flag.
    Raises:
      BigqueryError: If the data source does not support (custom) window days.

    """
    if 'dataRefreshType' in data_source_info:
      if data_source_info['dataRefreshType'] == 'CUSTOM_SLIDING_WINDOW':
        items['data_refresh_window_days'] = refresh_window_days
        return items
      else:
        raise BigqueryError(
            'Data source \'%s\' does not'
            ' support custom refresh window days.' % data_source)
    else:
      raise BigqueryError(
          'Data source \'%s\' does not'
          ' support refresh window days.' % data_source)

  def UpdateTable(self,
                  reference,
                  schema=None,
                  description=None,
                  display_name=None,
                  expiration=None,
                  view_query=None,
                  materialized_view_query=None,
                  enable_refresh=None,
                  refresh_interval_ms=None,
                  external_data_config=None,
                  view_udf_resources=None,
                  use_legacy_sql=None,
                  labels_to_set=None,
                  label_keys_to_remove=None,
                  time_partitioning=None,
                  range_partitioning=None,
                  clustering=None,
                  require_partition_filter=None,
                  etag=None,
                  encryption_configuration=None,
                  location=None):
    """Updates a table.

    Args:
      reference: the TableReference to update.
      schema: an optional schema for tables.
      description: an optional description for tables or views.
      display_name: an optional friendly name for the table.
      expiration: optional expiration time in milliseconds since the epoch for
        tables or views. Specifying 0 removes expiration time.
      view_query: an optional Sql query to update a view.
      materialized_view_query: an optional Standard SQL query for materialized
        views.
      enable_refresh: for materialized views, an optional toggle to enable /
        disable automatic refresh when the base table is updated.
      refresh_interval_ms: for materialized views, an optional maximum frequency
        for automatic refreshes.
      external_data_config: defines a set of external resources used to create
        an external table. For example, a BigQuery table backed by CSV files
        in GCS.
      view_udf_resources: optional UDF resources used in a view.
      use_legacy_sql: Whether to use Legacy SQL. If not set, the default
        behavior is true.
      labels_to_set: an optional dict of labels to set on this table.
      label_keys_to_remove: an optional list of label keys to remove from this
        table.
      time_partitioning: if set, enables time based partitioning on the table
        and configures the partitioning.
      range_partitioning: if set, enables range partitioning on the table and
        configures the partitioning.
      clustering: if set, enables clustering on the table and configures the
        clustering spec.
      require_partition_filter: if set, partition filter is required for
        queiries over this table.
      etag: if set, checks that etag in the existing table matches.
      encryption_configuration: Updates the encryption configuration.
      location: an optional location for which to update tables or views.

    Raises:
      TypeError: if reference is not a TableReference.
    """
    _Typecheck(reference, ApiClientHelper.TableReference, method='UpdateTable')

    table = BigqueryClient.ConstructObjectInfo(reference)

    if schema is not None:
      table['schema'] = {'fields': schema}
    else:
      table['schema'] = None

    if encryption_configuration is not None:
      table['encryptionConfiguration'] = encryption_configuration
    if display_name is not None:
      table['friendlyName'] = display_name
    if description is not None:
      table['description'] = description
    if expiration is not None:
      if expiration == 0:
        table['expirationTime'] = None
      else:
        table['expirationTime'] = expiration
    if view_query is not None:
      view_args = {'query': view_query}
      if view_udf_resources is not None:
        view_args['userDefinedFunctionResources'] = view_udf_resources
      if use_legacy_sql is not None:
        view_args['useLegacySql'] = use_legacy_sql
      table['view'] = view_args
    materialized_view_args = {}
    if materialized_view_query is not None:
      materialized_view_args.update({'query': materialized_view_query})
    if enable_refresh is not None:
      materialized_view_args.update({'enableRefresh': enable_refresh})
    if refresh_interval_ms is not None:
      materialized_view_args.update({'refreshIntervalMs': refresh_interval_ms})
    if materialized_view_args:
      table['materializedView'] = materialized_view_args
    if external_data_config is not None:
      table['externalDataConfiguration'] = external_data_config
    if 'labels' not in table:
      table['labels'] = {}
    if labels_to_set:
      for label_key, label_value in six.iteritems(labels_to_set):
        table['labels'][label_key] = label_value
    if label_keys_to_remove:
      for label_key in label_keys_to_remove:
        table['labels'][label_key] = None
    if time_partitioning is not None:
      table['timePartitioning'] = time_partitioning
    if range_partitioning is not None:
      table['rangePartitioning'] = range_partitioning
    if clustering is not None:
      if clustering == {}:  # pylint: disable=g-explicit-bool-comparison
        table['clustering'] = None
      else:
        table['clustering'] = clustering
    if require_partition_filter is not None:
      table['requirePartitionFilter'] = require_partition_filter
    if location is not None:
      table['location'] = location

    request = self.apiclient.tables().patch(body=table, **dict(reference))

    # Perform a conditional update to protect against concurrent
    # modifications to this table. If there is a conflicting
    # change, this update will fail with a "Precondition failed"
    # error.
    if etag:
      request.headers['If-Match'] = etag if etag else table['etag']
    request.execute()

  def UpdateModel(self,
                  reference,
                  description=None,
                  expiration=None,
                  labels_to_set=None,
                  label_keys_to_remove=None,
                  etag=None):
    """Updates a Model.

    Args:
      reference: the ModelReference to update.
      description: an optional description for model.
      expiration: optional expiration time in milliseconds since the epoch.
        Specifying 0 clears the expiration time for the model.
      labels_to_set: an optional dict of labels to set on this model.
      label_keys_to_remove: an optional list of label keys to remove from this
        model.
      etag: if set, checks that etag in the existing model matches.

    Raises:
      TypeError: if reference is not a ModelReference.
    """
    _Typecheck(reference, ApiClientHelper.ModelReference, method='UpdateModel')

    updated_model = {}
    if description is not None:
      updated_model['description'] = description
    if expiration is not None:
      updated_model['expirationTime'] = expiration or None
    if 'labels' not in updated_model:
      updated_model['labels'] = {}
    if labels_to_set:
      for label_key, label_value in six.iteritems(labels_to_set):
        updated_model['labels'][label_key] = label_value
    if label_keys_to_remove:
      for label_key in label_keys_to_remove:
        updated_model['labels'][label_key] = None

    request = self.GetModelsApiClient().models().patch(
        body=updated_model,
        projectId=reference.projectId,
        datasetId=reference.datasetId,
        modelId=reference.modelId)

    # Perform a conditional update to protect against concurrent
    # modifications to this model. If there is a conflicting
    # change, this update will fail with a "Precondition failed"
    # error.
    if etag:
      request.headers['If-Match'] = etag if etag else updated_model['etag']
    request.execute()

  def UpdateDataset(
      self,
      reference,
      description=None,
      display_name=None,
      acl=None,
      default_table_expiration_ms=None,
      default_partition_expiration_ms=None,
      labels_to_set=None,
      label_keys_to_remove=None,
      etag=None,
      default_kms_key=None):
    """Updates a dataset.

    Args:
      reference: the DatasetReference to update.
      description: an optional dataset description.
      display_name: an optional friendly name for the dataset.
      acl: an optional ACL for the dataset, as a list of dicts.
      default_table_expiration_ms: optional number of milliseconds for the
        default expiration duration for new tables created in this dataset.
      default_partition_expiration_ms: optional number of milliseconds for the
        default partition expiration duration for new partitioned tables created
        in this dataset.
      labels_to_set: an optional dict of labels to set on this dataset.
      label_keys_to_remove: an optional list of label keys to remove from this
        dataset.
      etag: if set, checks that etag in the existing dataset matches.
      default_kms_key: An optional kms dey that will apply to all newly created
        tables in the dataset, if no explicit key is supplied in the creating
        request.

    Raises:
      TypeError: if reference is not a DatasetReference.
    """
    _Typecheck(reference, ApiClientHelper.DatasetReference,
               method='UpdateDataset')

    # Get the existing dataset and associated ETag.
    get_request = self.apiclient.datasets().get(**dict(reference))
    if etag:
      get_request.headers['If-Match'] = etag
    dataset = get_request.execute()

    # Merge in the changes.
    if display_name is not None:
      dataset['friendlyName'] = display_name
    if description is not None:
      dataset['description'] = description
    if acl is not None:
      dataset['access'] = acl
    if default_table_expiration_ms is not None:
      dataset['defaultTableExpirationMs'] = default_table_expiration_ms
    if default_partition_expiration_ms is not None:
      if default_partition_expiration_ms == 0:
        dataset['defaultPartitionExpirationMs'] = None
      else:
        dataset['defaultPartitionExpirationMs'] = (
            default_partition_expiration_ms)
    if default_kms_key is not None:
      dataset['defaultEncryptionConfiguration'] = {
          'kmsKeyName': default_kms_key
      }
    if 'labels' not in dataset:
      dataset['labels'] = {}
    if labels_to_set:
      for label_key, label_value in six.iteritems(labels_to_set):
        dataset['labels'][label_key] = label_value
    if label_keys_to_remove:
      for label_key in label_keys_to_remove:
        dataset['labels'].pop(label_key, None)

    request = self.apiclient.datasets().update(body=dataset, **dict(reference))

    # Perform a conditional update to protect against concurrent
    # modifications to this dataset.  By placing the ETag returned in
    # the get operation into the If-Match header, the API server will
    # make sure the dataset hasn't changed.  If there is a conflicting
    # change, this update will fail with a "Precondition failed"
    # error.
    if etag or dataset['etag']:
      request.headers['If-Match'] = etag if etag else dataset['etag']
    request.execute()

  def DeleteDataset(self, reference, ignore_not_found=False,
                    delete_contents=None):
    """Deletes DatasetReference reference.

    Args:
      reference: the DatasetReference to delete.
      ignore_not_found: Whether to ignore "not found" errors.
      delete_contents: [Boolean] Whether to delete the contents of
        non-empty datasets. If not specified and the dataset has
        tables in it, the delete will fail. If not specified,
        the server default applies.

    Raises:
      TypeError: if reference is not a DatasetReference.
      BigqueryNotFoundError: if reference does not exist and
        ignore_not_found is False.
    """
    _Typecheck(reference, ApiClientHelper.DatasetReference,
               method='DeleteDataset')

    args = dict(reference)
    if delete_contents is not None:
      args['deleteContents'] = delete_contents
    try:
      self.apiclient.datasets().delete(**args).execute()
    except BigqueryNotFoundError:
      if not ignore_not_found:
        raise

  def DeleteTable(self, reference, ignore_not_found=False):
    """Deletes TableReference reference.

    Args:
      reference: the TableReference to delete.
      ignore_not_found: Whether to ignore "not found" errors.

    Raises:
      TypeError: if reference is not a TableReference.
      BigqueryNotFoundError: if reference does not exist and
        ignore_not_found is False.
    """
    _Typecheck(reference, ApiClientHelper.TableReference, method='DeleteTable')
    try:
      self.apiclient.tables().delete(**dict(reference)).execute()
    except BigqueryNotFoundError:
      if not ignore_not_found:
        raise

  def DeleteModel(self, reference, ignore_not_found=False):
    """Deletes ModelReference reference.

    Args:
      reference: the ModelReference to delete.
      ignore_not_found: Whether to ignore "not found" errors.

    Raises:
      TypeError: if reference is not a ModelReference.
      BigqueryNotFoundError: if reference does not exist and
        ignore_not_found is False.
    """
    _Typecheck(reference, ApiClientHelper.ModelReference, method='DeleteModel')
    try:
      self.GetModelsApiClient().models().delete(
          projectId=reference.projectId,
          datasetId=reference.datasetId,
          modelId=reference.modelId).execute()
    except BigqueryNotFoundError:
      if not ignore_not_found:
        raise

  def DeleteRoutine(self, reference, ignore_not_found=False):
    """Deletes RoutineReference reference.

    Args:
      reference: the RoutineReference to delete.
      ignore_not_found: Whether to ignore "not found" errors.

    Raises:
      TypeError: if reference is not a RoutineReference.
      BigqueryNotFoundError: if reference does not exist and
        ignore_not_found is False.
    """
    _Typecheck(
        reference, ApiClientHelper.RoutineReference, method='DeleteRoutine')
    try:
      self.GetRoutinesApiClient().routines().delete(**dict(reference)).execute()
    except BigqueryNotFoundError:
      if not ignore_not_found:
        raise

  def DeleteTransferConfig(self, reference, ignore_not_found=False):
    """Deletes TransferConfigReference reference.

    Args:
      reference: the TransferConfigReference to delete.
      ignore_not_found: Whether to ignore "not found" errors.

    Raises:
      TypeError: if reference is not a TransferConfigReference.
      BigqueryNotFoundError: if reference does not exist and
        ignore_not_found is False.
    """

    _Typecheck(reference, ApiClientHelper.TransferConfigReference,
               method='DeleteTransferConfig')
    try:
      transfer_client = self.GetTransferV1ApiClient()
      transfer_client.projects().locations().transferConfigs().delete(
          name=reference.transferConfigName).execute()
    except BigqueryNotFoundError:
      if not ignore_not_found:
        raise BigqueryNotFoundError(
            'Not found: %r' % (reference,), {'reason': 'notFound'}, [])

  #################################
  ## Job control
  #################################

  @staticmethod
  def _ExecuteInChunksWithProgress(request):
    """Run an apiclient request with a resumable upload, showing progress.

    Args:
      request: an apiclient request having a media_body that is a
        MediaFileUpload(resumable=True).

    Returns:
      The result of executing the request, if it succeeds.

    Raises:
      BigQueryError: on a non-retriable error or too many retriable errors.
    """
    result = None
    retriable_errors = 0
    output_token = None
    status = None
    while result is None:
      try:
        status, result = request.next_chunk()
      except googleapiclient.errors.HttpError as e:
        logging.error('HTTP Error %d during resumable media upload',
                      e.resp.status)
        # Log response headers, which contain debug info for GFEs.
        for key, value in e.resp.items():
          logging.info('  %s: %s', key, value)
        if e.resp.status in [502, 503, 504]:
          sleep_sec = 2 ** retriable_errors
          retriable_errors += 1
          if retriable_errors > 3:
            raise
          print('Error %d, retry #%d' % (e.resp.status, retriable_errors))
          time.sleep(sleep_sec)
          # Go around and try again.
        else:
          BigqueryHttp.RaiseErrorFromHttpError(e)
      except (httplib2.HttpLib2Error, IOError) as e:
        BigqueryHttp.RaiseErrorFromNonHttpError(e)
      if status:
        output_token = _OverwriteCurrentLine(
            'Uploaded %d%%... ' % int(status.progress() * 100),
            output_token)
    _OverwriteCurrentLine('Upload complete.', output_token)
    sys.stderr.write('\n')
    return result

  def StartJob(self,
               configuration,
               project_id=None,
               upload_file=None,
               job_id=None,
               location=None):
    """Start a job with the given configuration.

    Args:
      configuration: The configuration for a job.
      project_id: The project_id to run the job under. If None,
        self.project_id is used.
      upload_file: A file to include as a media upload to this request.
        Only valid on job requests that expect a media upload file.
      job_id: A unique job_id to use for this job. If a
        JobIdGenerator, a job id will be generated from the job configuration.
        If None, a unique job_id will be created for this request.
      location: Optional. The geographic location where the job should run.

    Returns:
      The job resource returned from the insert job request. If there is an
      error, the jobReference field will still be filled out with the job
      reference used in the request.

    Raises:
      BigqueryClientConfigurationError: if project_id and
        self.project_id are None.
    """
    project_id = project_id or self.project_id
    if not project_id:
      raise BigqueryClientConfigurationError(
          'Cannot start a job without a project id.')
    configuration = configuration.copy()
    if self.job_property:
      configuration['properties'] = dict(
          prop.partition('=')[0::2] for prop in self.job_property)
    job_request = {'configuration': configuration}

    # Use the default job id generator if no job id was supplied.
    job_id = job_id or self.job_id_generator

    if isinstance(job_id, JobIdGenerator):
      job_id = job_id.Generate(configuration)

    if job_id is not None:
      job_reference = {'jobId': job_id, 'projectId': project_id}
      job_request['jobReference'] = job_reference
      if location:
        job_reference['location'] = location
    media_upload = ''
    if upload_file:
      resumable = True
      # There is a bug in apiclient http lib that make uploading resumable files
      # with 0 length broken.
      if os.stat(upload_file).st_size == 0:
        resumable = False
      media_upload = http_request.MediaFileUpload(
          filename=upload_file, mimetype='application/octet-stream',
          resumable=resumable)
    request = self.apiclient.jobs().insert(
        body=job_request, media_body=media_upload,
        projectId=project_id)
    if upload_file and resumable:
      result = BigqueryClient._ExecuteInChunksWithProgress(request)
    else:
      result = request.execute()
    return result

  def _StartQueryRpc(
      self,
      query,
      dry_run=None,
      use_cache=None,
      preserve_nulls=None,
      request_id=None,
      maximum_bytes_billed=None,
      max_results=None,
      timeout_ms=None,
      min_completion_ratio=None,
      project_id=None,
      external_table_definitions_json=None,
      udf_resources=None,
      use_legacy_sql=None,
      location=None,
      **kwds):
    """Executes the given query using the rpc-style query api.

    Args:
      query: Query to execute.
      dry_run: Optional. Indicates whether the query will only be validated and
          return processing statistics instead of actually running.
      use_cache: Optional. Whether to use the query cache.
          Caching is best-effort only and you should not make
          assumptions about whether or how long a query result will be cached.
      preserve_nulls: Optional. Indicates whether to preserve nulls in input
          data. Temporary flag; will be removed in a future version.
      request_id: Optional. The idempotency token for jobs.query
      maximum_bytes_billed: Optional. Upper limit on the number of billed bytes.
      max_results: Maximum number of results to return.
      timeout_ms: Timeout, in milliseconds, for the call to query().
      min_completion_ratio: Optional. Specifies the minimum fraction of
          data that must be scanned before a query returns. This value should be
          between 0.0 and 1.0 inclusive.
      project_id: Project id to use.
      external_table_definitions_json: Json representation of external table
          definitions.
      udf_resources: Array of inline and external UDF code resources.
      use_legacy_sql: Whether to use Legacy SQL. If not set, the default value
          is true.
      location: Optional. The geographic location where the job should run.
      **kwds: Extra keyword arguments passed directly to jobs.Query().

    Returns:
      The query response.

    Raises:
      BigqueryClientConfigurationError: if project_id and
        self.project_id are None.
      BigqueryError: if query execution fails.
    """
    project_id = project_id or self.project_id
    if not project_id:
      raise BigqueryClientConfigurationError(
          'Cannot run a query without a project id.')
    request = {'query': query}
    if external_table_definitions_json:
      request['tableDefinitions'] = external_table_definitions_json
    if udf_resources:
      request['userDefinedFunctionResources'] = udf_resources
    if self.dataset_id:
      request['defaultDataset'] = self.GetQueryDefaultDataset(self.dataset_id)

    # If the request id flag is set, generate a random one if it is not provided
    # explicitly.
    if request_id is None and flags.FLAGS.jobs_query_use_request_id:
      request_id = str(uuid.uuid4())

    _ApplyParameters(
        request,
        preserve_nulls=preserve_nulls,
        request_id=request_id,
        maximum_bytes_billed=maximum_bytes_billed,
        use_query_cache=use_cache,
        timeout_ms=timeout_ms,
        max_results=max_results,
        use_legacy_sql=use_legacy_sql,
        min_completion_ratio=min_completion_ratio,
        location=location)
    _ApplyParameters(request, dry_run=dry_run)
    return self.apiclient.jobs().query(
        body=request, projectId=project_id, **kwds).execute()

  def GetQueryResults(self,
                      job_id=None,
                      project_id=None,
                      max_results=None,
                      timeout_ms=None,
                      location=None):
    """Waits for a query to complete, once.

    Args:
      job_id: The job id of the query job that we are waiting to complete.
      project_id: The project id of the query job.
      max_results: The maximum number of results.
      timeout_ms: The number of milliseconds to wait for the query to complete.
      location: Optional. The geographic location of the job.

    Returns:
      The getQueryResults() result.

    Raises:
      BigqueryClientConfigurationError: if project_id and
        self.project_id are None.
    """
    project_id = project_id or self.project_id
    if not project_id:
      raise BigqueryClientConfigurationError(
          'Cannot get query results without a project id.')
    kwds = {}
    _ApplyParameters(kwds,
                     job_id=job_id,
                     project_id=project_id,
                     timeout_ms=timeout_ms,
                     max_results=max_results,
                     location=location)
    return self.apiclient.jobs().getQueryResults(**kwds).execute()

  def RunJobSynchronously(self,
                          configuration,
                          project_id=None,
                          upload_file=None,
                          job_id=None,
                          location=None):
    """Starts a job and waits for it to complete.

    Args:
      configuration: The configuration for a job.
      project_id: The project_id to run the job under. If None,
        self.project_id is used.
      upload_file: A file to include as a media upload to this request.
        Only valid on job requests that expect a media upload file.
      job_id: A unique job_id to use for this job. If a
        JobIdGenerator, a job id will be generated from the job configuration.
        If None, a unique job_id will be created for this request.
      location: Optional. The geographic location where the job should run.

    Returns:
      job, if it did not fail.

    Raises:
      BigQueryError: if the job fails.
    """
    result = self.StartJob(
        configuration,
        project_id=project_id,
        upload_file=upload_file,
        job_id=job_id,
        location=location)
    if result['status']['state'] != 'DONE':
      job_reference = BigqueryClient.ConstructObjectReference(result)
      result = self.WaitJob(job_reference)
    return self.RaiseIfJobError(result)

  def ExecuteJob(self,
                 configuration,
                 sync=None,
                 project_id=None,
                 upload_file=None,
                 job_id=None,
                 location=None):
    """Execute a job, possibly waiting for results."""
    if sync is None:
      sync = self.sync

    if sync:
      job = self.RunJobSynchronously(
          configuration,
          project_id=project_id,
          upload_file=upload_file,
          job_id=job_id,
          location=location)
    else:
      job = self.StartJob(
          configuration,
          project_id=project_id,
          upload_file=upload_file,
          job_id=job_id,
          location=location)
      self.RaiseIfJobError(job)
    return job

  def CancelJob(self,
                project_id=None,
                job_id=None,
                location=None):
    """Attempt to cancel the specified job if it is running.

    Args:
      project_id: The project_id to the job is running under. If None,
        self.project_id is used.
      job_id: The job id for this job.
      location: Optional. The geographic location of the job.

    Returns:
      The job resource returned for the job for which cancel is being requested.

    Raises:
      BigqueryClientConfigurationError: if project_id or job_id are None.
    """
    project_id = project_id or self.project_id
    if not project_id:
      raise BigqueryClientConfigurationError(
          'Cannot cancel a job without a project id.')
    if not job_id:
      raise BigqueryClientConfigurationError(
          'Cannot cancel a job without a job id.')

    job_reference = ApiClientHelper.JobReference.Create(
        projectId=project_id,
        jobId=job_id,
        location=location)
    result = (self.apiclient.jobs().cancel(**dict(job_reference))
              .execute()['job'])
    if result['status']['state'] != 'DONE' and self.sync:
      job_reference = BigqueryClient.ConstructObjectReference(result)
      result = self.WaitJob(job_reference=job_reference)
    return result

  class WaitPrinter(object):
    """Base class that defines the WaitPrinter interface."""

    def Print(self, job_id, wait_time, status):
      """Prints status for the current job we are waiting on.

      Args:
        job_id: the identifier for this job.
        wait_time: the number of seconds we have been waiting so far.
        status: the status of the job we are waiting for.
      """
      raise NotImplementedError('Subclass must implement Print')

    def Done(self):
      """Waiting is done and no more Print calls will be made.

      This function should handle the case of Print not being called.
      """
      raise NotImplementedError('Subclass must implement Done')

  class WaitPrinterHelper(WaitPrinter):
    """A Done implementation that prints based off a property."""

    print_on_done = False

    def Done(self):
      if self.print_on_done:
        sys.stderr.write('\n')

  class QuietWaitPrinter(WaitPrinterHelper):
    """A WaitPrinter that prints nothing."""

    def Print(self, unused_job_id, unused_wait_time, unused_status):
      pass

  class VerboseWaitPrinter(WaitPrinterHelper):
    """A WaitPrinter that prints every update."""

    def __init__(self):
      self.output_token = None

    def Print(self, job_id, wait_time, status):
      self.print_on_done = True
      self.output_token = _OverwriteCurrentLine(
          'Waiting on %s ... (%ds) Current status: %-7s' % (
              job_id, wait_time, status),
          self.output_token)

  class TransitionWaitPrinter(VerboseWaitPrinter):
    """A WaitPrinter that only prints status change updates."""

    _previous_status = None

    def Print(self, job_id, wait_time, status):
      if status != self._previous_status:
        self._previous_status = status
        super(BigqueryClient.TransitionWaitPrinter, self).Print(
            job_id, wait_time, status)

  def WaitJob(self,
              job_reference,
              status='DONE',
              wait=sys.maxsize,
              wait_printer_factory=None):
    """Poll for a job to run until it reaches the requested status.

    Arguments:
      job_reference: JobReference to poll.
      status: (optional, default 'DONE') Desired job status.
      wait: (optional, default maxint) Max wait time.
      wait_printer_factory: (optional, defaults to
        self.wait_printer_factory) Returns a subclass of WaitPrinter
        that will be called after each job poll.

    Returns:
      The job object returned by the final status call.

    Raises:
      StopIteration: If polling does not reach the desired state before
        timing out.
      ValueError: If given an invalid wait value.
    """
    _Typecheck(job_reference, ApiClientHelper.JobReference, method='WaitJob')
    start_time = time.time()
    job = None
    if wait_printer_factory:
      printer = wait_printer_factory()
    else:
      printer = self.wait_printer_factory()

    # This is a first pass at wait logic: we ping at 1s intervals a few
    # times, then increase to max(3, max_wait), and then keep waiting
    # that long until we've run out of time.
    waits = itertools.chain(
        itertools.repeat(1, 8), range(2, 30, 3), itertools.repeat(30))
    current_wait = 0
    current_status = 'UNKNOWN'
    in_error_state = False
    while current_wait <= wait:
      try:
        done, job = self.PollJob(job_reference, status=status, wait=wait)
        current_status = job['status']['state']
        in_error_state = False
        if done:
          printer.Print(job_reference.jobId, current_wait, current_status)
          break
      except BigqueryCommunicationError as e:
        # Communication errors while waiting on a job are okay.
        logging.warning('Transient error during job status check: %s', e)
      except BigqueryBackendError as e:
        # Temporary server errors while waiting on a job are okay.
        logging.warning('Transient error during job status check: %s', e)
      except BigqueryServiceError as e:
        # Among this catch-all class, some kinds are permanent
        # errors, so we don't want to retry indefinitely, but if
        # the error is transient we'd like "wait" to get past it.
        if in_error_state: raise
        in_error_state = True
      for _ in range(next(waits)):
        current_wait = time.time() - start_time
        printer.Print(job_reference.jobId, current_wait, current_status)
        time.sleep(1)
    else:
      raise StopIteration(
          'Wait timed out. Operation not finished, in state %s' % (
              current_status,))
    printer.Done()
    return job

  def PollJob(self, job_reference, status='DONE', wait=0):
    """Poll a job once for a specific status.

    Arguments:
      job_reference: JobReference to poll.
      status: (optional, default 'DONE') Desired job status.
      wait: (optional, default 0) Max server-side wait time for one poll call.

    Returns:
      Tuple (in_state, job) where in_state is True if job is
      in the desired state.

    Raises:
      ValueError: If given an invalid wait value.
    """
    _Typecheck(job_reference, ApiClientHelper.JobReference, method='PollJob')
    wait = BigqueryClient.NormalizeWait(wait)
    job = self.apiclient.jobs().get(**dict(job_reference)).execute()
    current = job['status']['state']
    return (current == status, job)

  #################################
  ## Wrappers for job types
  #################################

  def RunQuery(self, start_row, max_rows, **kwds):
    """Run a query job synchronously, and return the result.

    Args:
      start_row: first row to read.
      max_rows: number of rows to read.
      **kwds: Passed on to self.Query.

    Returns:
      A tuple where the first item is the list of fields and the
      second item a list of rows.
    """
    new_kwds = dict(kwds)
    new_kwds['sync'] = True
    job = self.Query(**new_kwds)

    return self.ReadSchemaAndJobRows(job['jobReference'],
                                     start_row=start_row,
                                     max_rows=max_rows)

  def RunQueryRpc(
      self,
      query,
      dry_run=None,
      use_cache=None,
      preserve_nulls=None,
      request_id=None,
      maximum_bytes_billed=None,
      max_results=None,
      wait=sys.maxsize,
      min_completion_ratio=None,
      wait_printer_factory=None,
      max_single_wait=None,
      external_table_definitions_json=None,
      udf_resources=None,
      location=None,
      **kwds):
    """Executes the given query using the rpc-style query api.

    Args:
      query: Query to execute.
      dry_run: Optional. Indicates whether the query will only be validated and
          return processing statistics instead of actually running.
      use_cache: Optional. Whether to use the query cache.
          Caching is best-effort only and you should not make
          assumptions about whether or how long a query result will be cached.
      preserve_nulls: Optional. Indicates whether to preserve nulls in input
          data. Temporary flag; will be removed in a future version.
      request_id: Optional. Specifies the idempotency token for the request.
      maximum_bytes_billed: Optional. Upper limit on maximum bytes billed.
      max_results: Optional. Maximum number of results to return.
      wait: (optional, default maxint) Max wait time in seconds.
      min_completion_ratio: Optional. Specifies the minimum fraction of
          data that must be scanned before a query returns. This value should be
          between 0.0 and 1.0 inclusive.
      wait_printer_factory: (optional, defaults to
          self.wait_printer_factory) Returns a subclass of WaitPrinter
          that will be called after each job poll.
      max_single_wait: Optional. Maximum number of seconds to wait for each call
          to query() / getQueryResults().
      external_table_definitions_json: Json representation of external table
          definitions.
      udf_resources: Array of inline and remote UDF resources.
      location: Optional. The geographic location where the job should run.
      **kwds: Passed directly to self.ExecuteSyncQuery.

    Raises:
      BigqueryClientError: if no query is provided.
      StopIteration: if the query does not complete within wait seconds.
      BigqueryError: if query fails.

    Returns:
      A tuple (schema fields, row results, execution metadata).
        For regular queries, the execution metadata dict contains
        the 'State' and 'status' elements that would be in a job result
        after FormatJobInfo().
        For dry run queries schema and rows are empty, the execution metadata
        dict contains statistics

    """
    if not self.sync:
      raise BigqueryClientError('Running RPC-style query asynchronously is '
                                'not supported')
    if not query:
      raise BigqueryClientError('No query string provided')

    if request_id is not None and not flags.FLAGS.jobs_query_use_request_id:
      raise BigqueryClientError('request_id is not yet supported')

    if wait_printer_factory:
      printer = wait_printer_factory()
    else:
      printer = self.wait_printer_factory()

    start_time = time.time()
    elapsed_time = 0
    job_reference = None
    current_wait_ms = None
    while True:
      try:
        elapsed_time = 0 if job_reference is None else time.time() - start_time
        remaining_time = wait - elapsed_time
        if max_single_wait is not None:
          # Compute the current wait, being careful about overflow, since
          # remaining_time may be counting down from sys.maxint.
          current_wait_ms = int(min(remaining_time, max_single_wait) * 1000)
          if current_wait_ms < 0:
            current_wait_ms = sys.maxsize
        if remaining_time < 0:
          raise StopIteration('Wait timed out. Query not finished.')
        if job_reference is None:
          # We haven't yet run a successful Query(), so we don't
          # have a job id to check on.
          rows_to_read = max_results
          if self.max_rows_per_request is not None:
            if rows_to_read is None:
              rows_to_read = self.max_rows_per_request
            else:
              rows_to_read = min(self.max_rows_per_request, int(rows_to_read))
          result = self._StartQueryRpc(
              query=query,
              preserve_nulls=preserve_nulls,
              request_id=request_id,
              maximum_bytes_billed=maximum_bytes_billed,
              use_cache=use_cache,
              dry_run=dry_run,
              min_completion_ratio=min_completion_ratio,
              timeout_ms=current_wait_ms,
              max_results=rows_to_read,
              external_table_definitions_json=external_table_definitions_json,
              udf_resources=udf_resources,
              location=location,
              **kwds)
          if dry_run:
            execution = dict(statistics=dict(query=dict(
                totalBytesProcessed=result['totalBytesProcessed'],
                cacheHit=['cacheHit'])))
            return ([], [], execution)
          job_reference = ApiClientHelper.JobReference.Create(
              **result['jobReference'])
        else:
          # The query/getQueryResults methods do not return the job state,
          # so we just print 'RUNNING' while we are actively waiting.
          printer.Print(job_reference.jobId, elapsed_time, 'RUNNING')
          result = self.GetQueryResults(
              job_reference.jobId,
              max_results=max_results,
              timeout_ms=current_wait_ms,
              location=location)
        if result['jobComplete']:
          (schema, rows) = self.ReadSchemaAndJobRows(
              dict(job_reference),
              start_row=0,
              max_rows=max_results,
              result_first_page=result)
          # If we get here, we must have succeeded.  We could still have
          # non-fatal errors though.
          status = {}
          if 'errors' in result:
            status['errors'] = result['errors']
          execution = {'State': 'SUCCESS',
                       'status': status,
                       'jobReference': job_reference}
          return (schema, rows, execution)
      except BigqueryCommunicationError as e:
        # Communication errors while waiting on a job are okay.
        logging.warning('Transient error during query: %s', e)
      except BigqueryBackendError as e:
        # Temporary server errors while waiting on a job are okay.
        logging.warning('Transient error during query: %s', e)

  def Query(
      self,
      query,
      destination_table=None,
      create_disposition=None,
      write_disposition=None,
      priority=None,
      preserve_nulls=None,
      allow_large_results=None,
      dry_run=None,
      use_cache=None,
      min_completion_ratio=None,
      flatten_results=None,
      external_table_definitions_json=None,
      udf_resources=None,
      maximum_billing_tier=None,
      maximum_bytes_billed=None,
      use_legacy_sql=None,
      schema_update_options=None,
      labels=None,
      query_parameters=None,
      time_partitioning=None,
      destination_encryption_configuration=None,
      clustering=None,
      range_partitioning=None,
      script_options=None,
      job_timeout_ms=None,
      **kwds):
    # pylint: disable=g-doc-args
    """Execute the given query, returning the created job.

    The job will execute synchronously if sync=True is provided as an
    argument or if self.sync is true.

    Args:
      query: Query to execute.
      destination_table: (default None) If provided, send the results to the
          given table.
      create_disposition: Optional. Specifies the create_disposition for
          the destination_table.
      write_disposition: Optional. Specifies the write_disposition for
          the destination_table.
      priority: Optional. Priority to run the query with. Either
          'INTERACTIVE' (default) or 'BATCH'.
      preserve_nulls: Optional. Indicates whether to preserve nulls in input
          data. Temporary flag; will be removed in a future version.
      allow_large_results: Enables larger destination table sizes.
      dry_run: Optional. Indicates whether the query will only be validated and
          return processing statistics instead of actually running.
      use_cache: Optional. Whether to use the query cache. If create_disposition
          is CREATE_NEVER, will only run the query if the result is already
          cached. Caching is best-effort only and you should not make
          assumptions about whether or how long a query result will be cached.
      min_completion_ratio: Optional. Specifies the minimum fraction of
          data that must be scanned before a query returns. This value should be
          between 0.0 and 1.0 inclusive.
      flatten_results: Whether to flatten nested and repeated fields in the
        result schema. If not set, the default behavior is to flatten.
      external_table_definitions_json: Json representation of external table
        definitions.
      udf_resources: Array of inline and remote UDF resources.
      maximum_billing_tier: Upper limit for billing tier.
      maximum_bytes_billed: Upper limit for bytes billed.
      use_legacy_sql: Whether to use Legacy SQL. If not set, the default value
          is true.
      schema_update_options: schema update options when appending to the
          destination table or truncating a table partition.
      labels: an optional dict of labels to set on the query job.
      query_parameters: parameter values for use_legacy_sql=False queries.
      time_partitioning: Optional. Provides time based partitioning
          specification for the destination table.
      clustering: Optional. Provides clustering specification for the
          destination table.
      destination_encryption_configuration: Optional. Allows user to encrypt the
          table created from a query job with a Cloud KMS key.
      range_partitioning: Optional. Provides range partitioning specification
          for the destination table.
      script_options: Optional. Options controlling script execution.
      job_timeout_ms: Optional. How long to let the job run.
      **kwds: Passed on to self.ExecuteJob.

    Raises:
      BigqueryClientError: if no query is provided.

    Returns:
      The resulting job info.
    """
    if not query:
      raise BigqueryClientError('No query string provided')
    query_config = {'query': query}
    if self.dataset_id:
      query_config['defaultDataset'] = self.GetQueryDefaultDataset(
          self.dataset_id)
    if external_table_definitions_json:
      query_config['tableDefinitions'] = external_table_definitions_json
    if udf_resources:
      query_config['userDefinedFunctionResources'] = udf_resources
    if destination_table:
      try:
        reference = self.GetTableReference(destination_table)
      except BigqueryError as e:
        raise BigqueryError('Invalid value %s for destination_table: %s' % (
            destination_table, e))
      query_config['destinationTable'] = dict(reference)
    if destination_encryption_configuration:
      query_config['destinationEncryptionConfiguration'] = (
          destination_encryption_configuration)
    if script_options:
      query_config['scriptOptions'] = script_options
    _ApplyParameters(
        query_config,
        allow_large_results=allow_large_results,
        create_disposition=create_disposition,
        preserve_nulls=preserve_nulls,
        priority=priority,
        write_disposition=write_disposition,
        use_query_cache=use_cache,
        flatten_results=flatten_results,
        maximum_billing_tier=maximum_billing_tier,
        maximum_bytes_billed=maximum_bytes_billed,
        use_legacy_sql=use_legacy_sql,
        schema_update_options=schema_update_options,
        query_parameters=query_parameters,
        time_partitioning=time_partitioning,
        clustering=clustering,
        min_completion_ratio=min_completion_ratio,
        range_partitioning=range_partitioning)
    request = {'query': query_config}
    _ApplyParameters(request, dry_run=dry_run, labels=labels,
                     job_timeout_ms=job_timeout_ms)
    return self.ExecuteJob(request, **kwds)

  def Load(
      self,
      destination_table_reference,
      source,
      schema=None,
      create_disposition=None,
      write_disposition=None,
      field_delimiter=None,
      skip_leading_rows=None,
      encoding=None,
      quote=None,
      max_bad_records=None,
      allow_quoted_newlines=None,
      source_format=None,
      allow_jagged_rows=None,
      ignore_unknown_values=None,
      projection_fields=None,
      autodetect=None,
      schema_update_options=None,
      null_marker=None,
      time_partitioning=None,
      clustering=None,
      destination_encryption_configuration=None,
      use_avro_logical_types=None,
      range_partitioning=None,
      hive_partitioning_options=None,
      **kwds):
    """Load the given data into BigQuery.

    The job will execute synchronously if sync=True is provided as an
    argument or if self.sync is true.

    Args:
      destination_table_reference: TableReference to load data into.
      source: String specifying source data to load.
      schema: (default None) Schema of the created table. (Can be left blank
          for append operations.)
      create_disposition: Optional. Specifies the create_disposition for
          the destination_table_reference.
      write_disposition: Optional. Specifies the write_disposition for
          the destination_table_reference.
      field_delimiter: Optional. Specifies the single byte field delimiter.
      skip_leading_rows: Optional. Number of rows of initial data to skip.
      encoding: Optional. Specifies character encoding of the input data.
          May be "UTF-8" or "ISO-8859-1". Defaults to UTF-8 if not specified.
      quote: Optional. Quote character to use. Default is '"'. Note that
          quoting is done on the raw binary data before encoding is applied.
      max_bad_records: Optional. Maximum number of bad records that should
          be ignored before the entire job is aborted.
      allow_quoted_newlines: Optional. Whether to allow quoted newlines in CSV
          import data.
      source_format: Optional. Format of source data. May be "CSV",
          "DATASTORE_BACKUP", or "NEWLINE_DELIMITED_JSON".
      allow_jagged_rows: Optional. Whether to allow missing trailing optional
          columns in CSV import data.
      ignore_unknown_values: Optional. Whether to allow extra, unrecognized
          values in CSV or JSON data.
      projection_fields: Optional. If sourceFormat is set to "DATASTORE_BACKUP",
          indicates which entity properties to load into BigQuery from a Cloud
          Datastore backup.
      autodetect: Optional. If true, then we automatically infer the schema
          and options of the source files if they are CSV or JSON formats.
      schema_update_options: schema update options when appending to the
          destination table or truncating a table partition.
      null_marker: Optional. String that will be interpreted as a NULL value.
      time_partitioning: Optional. Provides time based partitioning
          specification for the destination table.
      clustering: Optional. Provides clustering specification for the
          destination table.
      destination_encryption_configuration: Optional. Allows user to encrypt the
          table created from a load job with Cloud KMS key.
      use_avro_logical_types: Optional. Allows user to override default
          behaviour for Avro logical types. If this is set, Avro fields with
          logical types will be interpreted into their corresponding types (ie.
          TIMESTAMP), instead of only using their raw types (ie. INTEGER).
      range_partitioning: Optional. Provides range partitioning specification
          for the destination table.
      hive_partitioning_options: (experimental) Options for configuring hive
          partitioning.  'mode' determines the partitioning mode. It accepts
          three strings: AUTO (automatic type inference), STRINGS (treat all
          partition key types as strings) and CUSTOM (customer provided schema).
          No other values are accepted.
          'sourceUriPrefix' is the shared prefix after which partition encoding
          is expected to begin across all uris.
      **kwds: Passed on to self.ExecuteJob.

    Returns:
      The resulting job info.
    """
    _Typecheck(destination_table_reference, ApiClientHelper.TableReference)
    load_config = {'destinationTable': dict(destination_table_reference)}
    sources = BigqueryClient.ProcessSources(source)
    if sources[0].startswith(_GCS_SCHEME_PREFIX):
      load_config['sourceUris'] = sources
      upload_file = None
    else:
      upload_file = sources[0]
    if schema is not None:
      load_config['schema'] = {'fields': BigqueryClient.ReadSchema(schema)}
    if use_avro_logical_types is not None:
      load_config['useAvroLogicalTypes'] = use_avro_logical_types
    if destination_encryption_configuration:
      load_config['destinationEncryptionConfiguration'] = (
          destination_encryption_configuration)
    _ApplyParameters(
        load_config,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        field_delimiter=field_delimiter,
        skip_leading_rows=skip_leading_rows,
        encoding=encoding,
        quote=quote,
        max_bad_records=max_bad_records,
        source_format=source_format,
        allow_quoted_newlines=allow_quoted_newlines,
        allow_jagged_rows=allow_jagged_rows,
        ignore_unknown_values=ignore_unknown_values,
        projection_fields=projection_fields,
        schema_update_options=schema_update_options,
        null_marker=null_marker,
        time_partitioning=time_partitioning,
        clustering=clustering,
        autodetect=autodetect,
        range_partitioning=range_partitioning,
        hive_partitioning_options=hive_partitioning_options)
    return self.ExecuteJob(configuration={'load': load_config},
                           upload_file=upload_file, **kwds)

  def Extract(
      self,
      reference,
      destination_uris,
      print_header=None,
      field_delimiter=None,
      destination_format=None,
      compression=None,
      use_avro_logical_types=None,
      **kwds):
    """Extract the given table from BigQuery.

    The job will execute synchronously if sync=True is provided as an
    argument or if self.sync is true.

    Args:
      reference: TableReference to read data from.
      destination_uris: String specifying one or more destination locations,
         separated by commas.
      print_header: Optional. Whether to print out a header row in the results.
      field_delimiter: Optional. Specifies the single byte field delimiter.
      destination_format: Optional. Format to extract table to. May be "CSV",
         "AVRO" or "NEWLINE_DELIMITED_JSON".
      compression: Optional. The compression type to use for exported files.
        Possible values include "GZIP" and "NONE". The default value is NONE.
      use_avro_logical_types: Optional. Whether to use avro logical types for
        applicable column types on extract jobs.
      **kwds: Passed on to self.ExecuteJob.

    Returns:
      The resulting job info.

    Raises:
      BigqueryClientError: if required parameters are invalid.
    """
    _Typecheck(
        reference,
        (ApiClientHelper.TableReference, ApiClientHelper.ModelReference),
        method='Extract')
    uris = destination_uris.split(',')
    for uri in uris:
      if not uri.startswith(_GCS_SCHEME_PREFIX):
        raise BigqueryClientError(
            'Illegal URI: {}. Extract URI must start with "{}".'.format(
                uri, _GCS_SCHEME_PREFIX))
    if isinstance(reference, ApiClientHelper.TableReference):
      extract_config = {'sourceTable': dict(reference)}
    elif isinstance(reference, ApiClientHelper.ModelReference):
      extract_config = {'sourceModel': dict(reference)}
    _ApplyParameters(
        extract_config, destination_uris=uris,
        destination_format=destination_format,
        print_header=print_header, field_delimiter=field_delimiter,
        compression=compression, use_avro_logical_types=use_avro_logical_types
    )
    return self.ExecuteJob(configuration={'extract': extract_config}, **kwds)


class _TableReader(object):
  """Base class that defines the TableReader interface.

  _TableReaders provide a way to read paginated rows and schemas from a table.
  """

  def ReadRows(self, start_row=0, max_rows=None, selected_fields=None):
    """Read at most max_rows rows from a table.

    Args:
      start_row: first row to return.
      max_rows: maximum number of rows to return.
      selected_fields: a subset of fields to return.

    Raises:
      BigqueryInterfaceError: when bigquery returns something unexpected.

    Returns:
      list of rows, each of which is a list of field values.
    """
    (_, rows) = self.ReadSchemaAndRows(start_row=start_row, max_rows=max_rows,
                                       selected_fields=selected_fields)
    return rows

  def ReadSchemaAndRows(self, start_row, max_rows, selected_fields=None):
    """Read at most max_rows rows from a table and the schema.

    Args:
      start_row: first row to read.
      max_rows: maximum number of rows to return.
      selected_fields: a subset of fields to return.

    Raises:
      BigqueryInterfaceError: when bigquery returns something unexpected.
      ValueError: when start_row is None.
      ValueError: when max_rows is None.

    Returns:
      A tuple where the first item is the list of fields and the
      second item a list of rows.
    """
    if start_row is None:
      raise ValueError('start_row is required')
    if max_rows is None:
      raise ValueError('max_rows is required')
    page_token = None
    rows = []
    schema = {}
    while len(rows) < max_rows:
      rows_to_read = max_rows - len(rows)
      if self.max_rows_per_request:
        rows_to_read = min(self.max_rows_per_request, rows_to_read)
      (more_rows, page_token, current_schema) = self._ReadOnePage(
          None if page_token else start_row,
          max_rows=rows_to_read,
          page_token=page_token, selected_fields=selected_fields)
      if not schema and current_schema:
        schema = current_schema.get('fields', [])
      for row in more_rows:
        rows.append(self._ConvertFromFV(schema, row))
        start_row += 1
      if not page_token or not more_rows:
        break
    return (schema, rows)

  def _ConvertFromFV(self, schema, row):
    """Converts from FV format to possibly nested lists of values."""
    if not row:
      return None
    values = [entry.get('v', '') for entry in row.get('f', [])]
    result = []
    for field, v in zip(schema, values):
      if 'type' not in field:
        raise BigqueryCommunicationError(
            'Invalid response: missing type property')
      if field['type'].upper() == 'RECORD':
        # Nested field.
        subfields = field.get('fields', [])
        if field.get('mode', 'NULLABLE').upper() == 'REPEATED':
          # Repeated and nested. Convert the array of v's of FV's.
          result.append([self._ConvertFromFV(
              subfields, subvalue.get('v', '')) for subvalue in v])
        else:
          # Nested non-repeated field. Convert the nested f from FV.
          result.append(self._ConvertFromFV(subfields, v))
      elif field.get('mode', 'NULLABLE').upper() == 'REPEATED':
        # Repeated but not nested: an array of v's.
        result.append([subvalue.get('v', '') for subvalue in v])
      else:
        # Normal flat field.
        result.append(v)
    return result

  def __str__(self):
    return self._GetPrintContext()

  def __repr__(self):
    return self._GetPrintContext()

  def _GetPrintContext(self):
    """Returns context for what is being read."""
    raise NotImplementedError('Subclass must implement GetPrintContext')

  def _ReadOnePage(self, start_row, max_rows, page_token=None,
                   selected_fields=None):
    """Read one page of data, up to max_rows rows.

    Assumes that the table is ready for reading. Will signal an error otherwise.

    Args:
      start_row: first row to read.
      max_rows: maximum number of rows to return.
      page_token: Optional. current page token.
      selected_fields: a subset of field to return.

    Returns:
      tuple of:
      rows: the actual rows of the table, in f,v format.
      page_token: the page token of the next page of results.
      schema: the schema of the table.
    """
    raise NotImplementedError('Subclass must implement _ReadOnePage')


class _TableTableReader(_TableReader):
  """A TableReader that reads from a table."""

  def __init__(self, local_apiclient, max_rows_per_request, table_ref):
    self.table_ref = table_ref
    self.max_rows_per_request = max_rows_per_request
    self._apiclient = local_apiclient


  def _GetPrintContext(self):
    return '%r' % (self.table_ref,)

  def _ReadOnePage(self, start_row, max_rows, page_token=None,
                   selected_fields=None):
    kwds = dict(self.table_ref)
    kwds['maxResults'] = max_rows
    if page_token:
      kwds['pageToken'] = page_token
    else:
      kwds['startIndex'] = start_row
    data = None
    if selected_fields is not None:
      kwds['selectedFields'] = selected_fields
    if data is None:
      data = self._apiclient.tabledata().list(**kwds).execute()
    page_token = data.get('pageToken', None)
    rows = data.get('rows', [])

    kwds = dict(self.table_ref)
    if selected_fields is not None:
      kwds['selectedFields'] = selected_fields
    table_info = self._apiclient.tables().get(**kwds).execute()
    schema = table_info.get('schema', {})

    return (rows, page_token, schema)


class _JobTableReader(_TableReader):
  """A TableReader that reads from a completed job."""

  def __init__(self, local_apiclient, max_rows_per_request, job_ref):
    self.job_ref = job_ref
    self.max_rows_per_request = max_rows_per_request
    self._apiclient = local_apiclient

  def _GetPrintContext(self):
    return '%r' % (self.job_ref,)

  def _ReadOnePage(self, start_row, max_rows, page_token=None,
                   selected_fields=None):
    kwds = dict(self.job_ref)
    kwds['maxResults'] = max_rows
    # Sets the timeout to 0 because we assume the table is already ready.
    kwds['timeoutMs'] = 0
    if page_token:
      kwds['pageToken'] = page_token
    else:
      kwds['startIndex'] = start_row
    data = self._apiclient.jobs().getQueryResults(**kwds).execute()
    if not data['jobComplete']:
      raise BigqueryError('Job %s is not done' % (self,))
    page_token = data.get('pageToken', None)
    schema = data.get('schema', None)
    rows = data.get('rows', [])
    return (rows, page_token, schema)


class _QueryTableReader(_TableReader):
  """A TableReader that reads from a completed query."""

  def __init__(self, local_apiclient, max_rows_per_request, job_ref, results):
    self.job_ref = job_ref
    self.max_rows_per_request = max_rows_per_request
    self._apiclient = local_apiclient
    self._results = results

  def _GetPrintContext(self):
    return '%r' % (self.job_ref,)

  def _ReadOnePage(self,
                   start_row,
                   max_rows,
                   page_token=None,
                   selected_fields=None):
    kwds = dict(self.job_ref)
    kwds['maxResults'] = max_rows
    # Sets the timeout to 0 because we assume the table is already ready.
    kwds['timeoutMs'] = 0
    if page_token:
      kwds['pageToken'] = page_token
    else:
      kwds['startIndex'] = start_row
    if not self._results['jobComplete']:
      raise BigqueryError('Job %s is not done' % (self,))
    # DDL and DML statements return no rows, just delegate them to
    # getQueryResults.
    result_rows = self._results.get('rows', None)
    total_rows = self._results.get('totalRows', None)
    if (total_rows is not None and result_rows is not None and
        start_row is not None and
        len(result_rows) >= min(int(total_rows), start_row + max_rows)):
      page_token = self._results.get('pageToken', None)
      if (len(result_rows) < int(total_rows) and page_token is None):
        raise BigqueryError('Synchronous query %s did not return all rows, '
                            'yet it did not return a page token' % (self,))
      schema = self._results.get('schema', None)
      rows = self._results.get('rows', [])
    else:
      data = self._apiclient.jobs().getQueryResults(**kwds).execute()
      if not data['jobComplete']:
        raise BigqueryError('Job %s is not done' % (self,))
      page_token = data.get('pageToken', None)
      schema = data.get('schema', None)
      rows = data.get('rows', [])
    return (rows, page_token, schema)


class ApiClientHelper(object):
  """Static helper methods and classes not provided by the discovery client."""

  def __init__(self, *unused_args, **unused_kwds):
    raise NotImplementedError('Cannot instantiate static class ApiClientHelper')

  class Reference(collections.Mapping):
    """Base class for Reference objects returned by apiclient."""
    _required_fields = frozenset()
    _optional_fields = frozenset()
    _format_str = ''

    def __init__(self, **kwds):
      if type(self) == ApiClientHelper.Reference:
        raise NotImplementedError(
            'Cannot instantiate abstract class ApiClientHelper.Reference')
      for name in self._required_fields:
        if not kwds.get(name, ''):
          raise ValueError('Missing required argument %s to %s' % (
              name, self.__class__.__name__))
        setattr(self, name, kwds[name])
      for name in self._optional_fields:
        if kwds.get(name, ''):
          setattr(self, name, kwds[name])

    @classmethod
    def Create(cls, **kwds):
      """Factory method for this class."""
      args = dict((k, v)
                  for k, v in six.iteritems(kwds)
                  if k in cls._required_fields.union(cls._optional_fields))
      return cls(**args)

    def __iter__(self):
      return iter(self._required_fields.union(self._optional_fields))

    def __getitem__(self, key):
      if key in self._optional_fields:
        if key in self.__dict__:
          return self.__dict__[key]
        else:
          return None
      if key in self._required_fields:
        return self.__dict__[key]
      raise KeyError(key)

    def __hash__(self):
      return hash(str(self))

    def __len__(self):
      return len(self._required_fields.union(self._optional_fields))

    def __str__(self):
      return six.ensure_str(self._format_str % dict(self))

    def __repr__(self):
      return "%s '%s'" % (self.typename, self)

    def __eq__(self, other):
      d = dict(other)
      return all(
          getattr(self, name, None) == d.get(name, None)
          for name in self._required_fields.union(self._optional_fields))

  class JobReference(Reference):
    _required_fields = frozenset(('projectId', 'jobId'))
    _optional_fields = frozenset(('location',))
    _format_str = '%(projectId)s:%(jobId)s'
    typename = 'job'

    def GetProjectReference(self):
      return ApiClientHelper.ProjectReference.Create(
          projectId=self.projectId)

  class ProjectReference(Reference):
    _required_fields = frozenset(('projectId',))
    _format_str = '%(projectId)s'
    typename = 'project'

    def GetDatasetReference(self, dataset_id):
      return ApiClientHelper.DatasetReference.Create(
          projectId=self.projectId, datasetId=dataset_id)

    def GetTableReference(self, dataset_id, table_id):
      return ApiClientHelper.TableReference.Create(
          projectId=self.projectId, datasetId=dataset_id, tableId=table_id)

  class DatasetReference(Reference):
    _required_fields = frozenset(('projectId', 'datasetId'))
    _format_str = '%(projectId)s:%(datasetId)s'
    typename = 'dataset'

    def GetProjectReference(self):
      return ApiClientHelper.ProjectReference.Create(
          projectId=self.projectId)

    def GetTableReference(self, table_id):
      return ApiClientHelper.TableReference.Create(
          projectId=self.projectId, datasetId=self.datasetId, tableId=table_id)

  class TableReference(Reference):
    _required_fields = frozenset(('projectId', 'datasetId', 'tableId'))
    _format_str = '%(projectId)s:%(datasetId)s.%(tableId)s'
    typename = 'table'

    def GetDatasetReference(self):
      return ApiClientHelper.DatasetReference.Create(
          projectId=self.projectId, datasetId=self.datasetId)

    def GetProjectReference(self):
      return ApiClientHelper.ProjectReference.Create(
          projectId=self.projectId)

  class ModelReference(Reference):
    _required_fields = frozenset(('projectId', 'datasetId', 'modelId'))
    _format_str = '%(projectId)s:%(datasetId)s.%(modelId)s'
    typename = 'model'

  class RoutineReference(Reference):
    _required_fields = frozenset(('projectId', 'datasetId', 'routineId'))
    _format_str = '%(projectId)s:%(datasetId)s.%(routineId)s'
    typename = 'routine'

  class TransferConfigReference(Reference):
    _required_fields = frozenset(('transferConfigName',))
    _format_str = '%(transferConfigName)s'
    typename = 'transfer config'

  class TransferRunReference(Reference):
    _required_fields = frozenset(('transferRunName',))
    _format_str = '%(transferRunName)s'
    typename = 'transfer run'

  class NextPageTokenReference(Reference):
    _required_fields = frozenset(('pageTokenId',))
    _format_str = '%(pageTokenId)s'
    typename = 'page token'

  class TransferLogReference(TransferRunReference):
    pass

  class EncryptionServiceAccount(Reference):
    _required_fields = frozenset(('serviceAccount',))
    _format_str = '%(serviceAccount)s'
    # typename is set to none because the EncryptionServiceAccount does not
    # store a 'reference', so when the object info is printed, it will omit
    # an unnecessary line that would have tried to print a reference in other
    # cases, i.e. datasets, tables, etc.
    typename = None

  class ReservationReference(Reference):
    _required_fields = frozenset(('projectId', 'location', 'reservationId'))
    _format_str = '%(projectId)s:%(location)s.%(reservationId)s'
    _path_str = 'projects/%(projectId)s/locations/%(location)s/reservations/%(reservationId)s'
    typename = 'reservation'

    def path(self):
      return self._path_str % dict(self)

  class AutoscaleAlphaReservationReference(ReservationReference):
    """Reference for autoscale_alpha, which has more features than stable versions."""
    pass

  class CapacityCommitmentReference(Reference):
    """Helper class to provide a reference to capacity commitment."""
    _required_fields = frozenset(
        ('projectId', 'location', 'capacityCommitmentId'))
    _format_str = '%(projectId)s:%(location)s.%(capacityCommitmentId)s'
    _path_str = 'projects/%(projectId)s/locations/%(location)s/capacityCommitments/%(capacityCommitmentId)s'
    typename = 'capacity commitment'

    def path(self):
      return self._path_str % dict(self)

  class ReservationAssignmentReference(Reference):
    """Helper class to provide a reference to reservation assignment."""
    _required_fields = frozenset(
        ('projectId', 'location', 'reservationId', 'reservationAssignmentId'))
    _format_str = '%(projectId)s:%(location)s.%(reservationId)s.%(reservationAssignmentId)s'
    _path_str = 'projects/%(projectId)s/locations/%(location)s/reservations/%(reservationId)s/assignments/%(reservationAssignmentId)s'
    _reservation_format_str = '%(projectId)s:%(location)s.%(reservationId)s'
    typename = 'reservation assignment'

    def path(self):
      return self._path_str % dict(self)

    def reservation_path(self):
      return self._reservation_format_str % dict(self)

  class BiReservationReference(Reference):
    """ Helper class to provide a reference to bi reservation. """
    _required_fields = frozenset(('projectId', 'location'))
    _format_str = '%(projectId)s:%(location)s'
    _path_str = 'projects/%(projectId)s/locations/%(location)s/biReservation'
    _create_path_str = 'projects/%(projectId)s/locations/%(location)s'
    typename = 'bi reservation'

    def path(self):
      return self._path_str % dict(self)

    def create_path(self):
      return self._create_path_str % dict(self)

  class ConnectionReference(Reference):
    _required_fields = frozenset(('projectId', 'location', 'connectionId'))
    _format_str = '%(projectId)s.%(location)s.%(connectionId)s'
    _path_str = 'projects/%(projectId)s/locations/%(location)s/connections/%(connectionId)s'
    typename = 'connection'

    def path(self):
      return self._path_str % dict(self)
