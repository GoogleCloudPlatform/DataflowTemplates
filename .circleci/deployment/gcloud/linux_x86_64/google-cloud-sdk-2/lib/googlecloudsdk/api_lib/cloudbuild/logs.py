# -*- coding: utf-8 -*- #
# Copyright 2016 Google LLC. All Rights Reserved.
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
"""Manage and stream build logs from Cloud Builds."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import re
import time

from apitools.base.py import exceptions as api_exceptions

from googlecloudsdk.api_lib.cloudbuild import cloudbuild_util
from googlecloudsdk.calliope import base
from googlecloudsdk.core import exceptions
from googlecloudsdk.core import log
from googlecloudsdk.core import properties
from googlecloudsdk.core.console import console_attr_os
from googlecloudsdk.core.credentials import http
from googlecloudsdk.core.credentials import requests as creds_requests
from googlecloudsdk.core.util import encoding

import httplib2
import requests


class NoLogsBucketException(exceptions.Error):

  def __init__(self):
    msg = 'Build does not specify logsBucket, unable to stream logs'
    super(NoLogsBucketException, self).__init__(msg)


Response = collections.namedtuple('Response', ['status', 'headers', 'body'])


class Httplib2LogTailer(object):
  """LogTailer transport to make HTTP requests using httplib2."""

  def __init__(self):
    self.http = http.Http()

  def Request(self, url, cursor):
    try:
      (res, body) = self.http.request(
          url,
          method='GET',
          headers={'Range': 'bytes={0}-'.format(cursor)})
      return Response(res.status, res, body)
    except httplib2.HttpLib2Error as e:
      raise api_exceptions.CommunicationError('Failed to connect: %s' % e)


class RequestsLogTailer(object):
  """LogTailer transport to make HTTP requests using requests."""

  def __init__(self):
    self.session = creds_requests.GetSession()

  def Request(self, url, cursor):
    try:
      response = self.session.request(
          'GET', url, headers={'Range': 'bytes={0}-'.format(cursor)})
      return Response(response.status_code, response.headers, response.content)
    except requests.exceptions.RequestException as e:
      raise api_exceptions.CommunicationError('Failed to connect: %s' % e)


def GetLogTailerTransport():
  if base.UseRequests():
    return RequestsLogTailer()
  return Httplib2LogTailer()


class LogTailer(object):
  """Helper class to tail a GCS logfile, printing content as available."""

  LOG_OUTPUT_BEGIN = ' REMOTE BUILD OUTPUT '
  LOG_OUTPUT_INCOMPLETE = ' (possibly incomplete) '
  OUTPUT_LINE_CHAR = '-'
  GCS_URL_PATTERN = (
      'https://www.googleapis.com/storage/v1/b/{bucket}/o/{obj}?alt=media')

  def __init__(self, bucket, obj, out=log.status, url_pattern=None):
    self.transport = GetLogTailerTransport()
    url_pattern = url_pattern or self.GCS_URL_PATTERN
    self.url = url_pattern.format(bucket=bucket, obj=obj)
    log.debug('GCS logfile url is ' + self.url)
    # position in the file being read
    self.cursor = 0
    self.out = out

  @classmethod
  def FromBuild(cls, build, out=log.out):
    """Build a LogTailer from a build resource.

    Args:
      build: Build resource, The build whose logs shall be streamed.
      out: The output stream to write the logs to.

    Raises:
      NoLogsBucketException: If the build does not specify a logsBucket.

    Returns:
      LogTailer, the tailer of this build's logs.
    """
    if not build.logsBucket:
      raise NoLogsBucketException()

    # remove gs:// prefix from bucket
    log_stripped = build.logsBucket
    gcs_prefix = 'gs://'
    if log_stripped.startswith(gcs_prefix):
      log_stripped = log_stripped[len(gcs_prefix):]

    if '/' not in log_stripped:
      log_bucket = log_stripped
      log_object_dir = ''
    else:
      [log_bucket, log_object_dir] = log_stripped.split('/', 1)
      log_object_dir += '/'

    log_object = '{object}log-{id}.txt'.format(
        object=log_object_dir,
        id=build.id,
    )

    return cls(
        bucket=log_bucket,
        obj=log_object,
        out=out,
        url_pattern='https://storage.googleapis.com/{bucket}/{obj}')

  def Poll(self, is_last=False):
    """Poll the GCS object and print any new bytes to the console.

    Args:
      is_last: True if this is the final poll operation.

    Raises:
      api_exceptions.HttpError: if there is trouble connecting to GCS.
      api_exceptions.CommunicationError: if there is trouble reaching the server
          and is_last=True.
    """
    try:
      res = self.transport.Request(self.url, self.cursor)
    except api_exceptions.CommunicationError:
      # Sometimes this request fails due to read timeouts (b/121307719). When
      # this happens we should just proceed and rely on the next poll to pick
      # up any missed logs. If this is the last request, there won't be another
      # request, and we can just fail.
      if is_last:
        raise
      return

    if res.status == 404:  # Not Found
      # Logfile hasn't been written yet (ie, build hasn't started).
      log.debug('Reading GCS logfile: 404 (no log yet; keep polling)')
      return

    if res.status == 416:  # Requested Range Not Satisfiable
      # We have consumed all available data. We'll get this a lot as we poll.
      log.debug('Reading GCS logfile: 416 (no new content; keep polling)')
      if is_last:
        self._PrintLastLine()
      return

    if res.status == 206 or res.status == 200:  # Partial Content
      # New content available. Print it!
      log.debug('Reading GCS logfile: {code} (read {count} bytes)'.format(
          code=res.status, count=len(res.body)))
      if self.cursor == 0:
        self._PrintFirstLine()
      self.cursor += len(res.body)
      decoded = encoding.Decode(res.body)
      if decoded is not None:
        decoded = self._ValidateScreenReader(decoded)
      self._PrintLogLine(decoded.rstrip('\n'))

      if is_last:
        self._PrintLastLine()
      return

    # For 429/503, there isn't much to do other than retry on the next poll.
    # If we get a 429 after the build has completed, the user may get incomplete
    # logs. This is expected to be rare enough to not justify building a complex
    # exponential retry system.
    if res.status == 429:  # Too Many Requests
      log.warning('Reading GCS logfile: 429 (server is throttling us)')
      if is_last:
        self._PrintLastLine(self.LOG_OUTPUT_INCOMPLETE)
      return

    if res.status >= 500 and res.status < 600:  # Server Error
      log.warning('Reading GCS logfile: got {0}, retrying'.format(res.status))
      if is_last:
        self._PrintLastLine(self.LOG_OUTPUT_INCOMPLETE)
      return

    # Default: any other codes are treated as errors.
    headers = dict(res.headers)
    headers['status'] = res.status
    raise api_exceptions.HttpError(headers, res.body, self.url)

  def _ValidateScreenReader(self, text):
    """Modify output for better screen reader experience."""
    screen_reader = properties.VALUES.accessibility.screen_reader.GetBool()
    if screen_reader:
      return re.sub('---> ', '', text)
    return text

  def _PrintLogLine(self, text):
    """Testing Hook: This method enables better verification of output."""
    self.out.Print(text)

  def _PrintFirstLine(self):
    width, _ = console_attr_os.GetTermSize()
    self._PrintLogLine(
        self.LOG_OUTPUT_BEGIN.center(width, self.OUTPUT_LINE_CHAR))

  def _PrintLastLine(self, msg=''):
    width, _ = console_attr_os.GetTermSize()
    # We print an extra blank visually separating the log from other output.
    self._PrintLogLine(msg.center(width, self.OUTPUT_LINE_CHAR) + '\n')


class CloudBuildClient(object):
  """Client for interacting with the Cloud Build API (and Cloud Build logs)."""

  def __init__(self, client=None, messages=None):
    self.client = client or cloudbuild_util.GetClientInstance()
    self.messages = messages or cloudbuild_util.GetMessagesModule()

  def GetBuild(self, build_ref):
    """Get a Build message.

    Args:
      build_ref: Build reference

    Returns:
      Build resource
    """
    return self.client.projects_builds.Get(
        self.messages.CloudbuildProjectsBuildsGetRequest(
            projectId=build_ref.projectId, id=build_ref.id))

  def Stream(self, build_ref, out=log.out):
    """Streams the logs for a build if available.

    Regardless of whether logs are available for streaming, awaits build
    completion before returning.

    Args:
      build_ref: Build reference, The build whose logs shall be streamed.
      out: The output stream to write the logs to.

    Raises:
      NoLogsBucketException: If the build is expected to specify a logsBucket
      but does not.

    Returns:
      Build message, The completed or terminated build as read for the final
      poll.
    """
    build = self.GetBuild(build_ref)
    if not build.options or build.options.logging not in [
        self.messages.BuildOptions.LoggingValueValuesEnum.NONE,
        self.messages.BuildOptions.LoggingValueValuesEnum.STACKDRIVER_ONLY,
        self.messages.BuildOptions.LoggingValueValuesEnum.CLOUD_LOGGING_ONLY,
    ]:
      log_tailer = LogTailer.FromBuild(build, out=out)
    else:
      log.info('Not streaming logs: requested logging mode is {0}.'.format(
          build.options.logging))
      log_tailer = None

    statuses = self.messages.Build.StatusValueValuesEnum
    working_statuses = [
        statuses.QUEUED,
        statuses.WORKING,
    ]

    while build.status in working_statuses:
      if log_tailer:
        log_tailer.Poll()
      time.sleep(1)
      build = self.GetBuild(build_ref)

    # Poll the logs one final time to ensure we have everything. We know this
    # final poll will get the full log contents because GCS is strongly
    # consistent and Cloud Build waits for logs to finish pushing before
    # marking the build complete.
    if log_tailer:
      log_tailer.Poll(is_last=True)

    return build

  def PrintLog(self, build_ref):
    """Print the logs for a build.

    Args:
      build_ref: Build reference, The build whose logs shall be streamed.

    Raises:
      NoLogsBucketException: If the build does not specify a logsBucket.
    """
    build = self.GetBuild(build_ref)
    if build.options and build.options.logging in [
        self.messages.BuildOptions.LoggingValueValuesEnum.NONE,
        self.messages.BuildOptions.LoggingValueValuesEnum.STACKDRIVER_ONLY,
        self.messages.BuildOptions.LoggingValueValuesEnum.CLOUD_LOGGING_ONLY,
    ]:
      log.info('GCS logs not available: build logging mode is {0}.'.format(
          build.options.logging))
      return

    log_tailer = LogTailer.FromBuild(build)
    log_tailer.Poll(is_last=True)
