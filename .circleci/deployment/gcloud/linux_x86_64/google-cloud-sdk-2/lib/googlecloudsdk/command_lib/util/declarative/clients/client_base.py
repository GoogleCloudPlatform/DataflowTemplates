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
"""Library for retrieving declarative clients."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import abc

from googlecloudsdk.core import exceptions as c_except
import six


class ClientException(c_except.Error):
  """General Purpose Exception."""


class ResourceNotFoundException(ClientException):
  """Exception for when a requested resource doesn't exist."""


@six.add_metaclass(abc.ABCMeta)
class DeclarativeClient(object):
  """Abstract class representing a high level declarative service."""

  @abc.abstractmethod
  def Parse(self, file_path):
    raise NotImplementedError(
        'DeclarativeService Parse function must be implemented.')

  @abc.abstractmethod
  def Serialize(self, file_path):
    raise NotImplementedError(
        'DeclarativeService Export function must be implemented.')

  @abc.abstractmethod
  def SerializeAll(self, file_path):
    raise NotImplementedError(
        'DeclarativeService Export function must be implemented.')

  @abc.abstractmethod
  def Get(self):
    raise NotImplementedError(
        'DeclarativeService Get function must be implemented.')

  @abc.abstractmethod
  def GetAll(self):
    raise NotImplementedError(
        'DeclarativeService GetAll function must be implemented.')

  @abc.abstractmethod
  def Apply(self):
    raise NotImplementedError(
        'DeclarativeService Apply function must be implemented.')

  def Delete(self):
    raise NotImplementedError(
        'DeclarativeService Delete function must be implemented.')
