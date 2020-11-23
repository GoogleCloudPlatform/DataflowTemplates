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

"""Abstract operation class that command operations will inherit from.

Should typically be executed in a task iterator through
googlecloudsdk.command_lib.storage.tasks.task_executor.

Manual execution example:

>>> class CopyTask(Task):
...   def __init__(self, src, dest):
...     ...
>>> my_copy_task = new CopyTask('~/Desktop/memes.jpg', '/kernel/')
>>> my_copy_task.Execute()
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import abc
import six


class Task(six.with_metaclass(abc.ABCMeta, object)):
  """Abstract class to represent one command operation."""

  @abc.abstractmethod
  def execute(self, callback=None):
    """Performs some work based on class attributes.

    Args:
      callback (Callable): Called after execute completes.

    Returns:
      An Optional[Iterable[Iterable[Task]]], which should be executed such that
      all tasks in each Iterable[Task] are executed before any tasks
      in the next Iterable[Task] can begin. Tasks within each Iterable[Task] are
      unordered. For example, if the execute method returned the following:

      [
        [UploadObjectTask(), UploadObjectTask(), UploadObjectTask()],
        [ComposeObjectsTask()]
      ]

      All UploadObjectTasks should be completed before the ComposeObjectTask
      can begin, but the UploadObjectTasks can be executed in parallel.

      Note that because the results of execute are sent between processes, the
      return value has to be picklable, which means execute cannot return a
      generator.
    """
    pass
