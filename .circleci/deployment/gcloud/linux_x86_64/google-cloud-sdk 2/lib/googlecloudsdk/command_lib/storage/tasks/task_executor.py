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

"""Function for executing the tasks contained in a Task Iterator.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals


# TODO(b/159812354): Update placeholder value.
TASKS_PER_THREAD_THRESHOLD = 8


# TODO(b/159812354): Complete function implementation.
def _ExecuteTaskParallel(task_iterator):
  """Executes tasks in parallel.

  1) Create concurrent futures executor.
  2) Iterate over each task and submit the task to the executor

  Args:
    task_iterator: An iterator for task objects.
  """
  tasks_on_thread = 0
  stats_estimator_started = False
  for _ in task_iterator:
    # Add task to concurrent.futures executor.
    tasks_on_thread += 1

    # If the count is created than some predefined threshold,
    # start the stats_estimator, which runs on a separate thread.
    if (tasks_on_thread > TASKS_PER_THREAD_THRESHOLD
        and not stats_estimator_started):
      task_iterator.stats_estimator.start()
      stats_estimator_started = True
      tasks_on_thread = 0


def _ExecuteTasksSequential(task_iterator):
  """Executes task objects sequentially.

  Args:
    task_iterator (Iterable[Task]): An iterator for task objects.
  """
  for task in task_iterator:
    additional_task_iterators = task.execute()
    if additional_task_iterators is not None:
      for new_task_iterator in additional_task_iterators:
        _ExecuteTasksSequential(new_task_iterator)


def ExecuteTasks(task_iterator, is_parallel=False):
  """Call appropriate executor.

  Args:
    task_iterator: An iterator for task objects.
    is_parallel (boolean): Should tasks be executed in parallel.
  """
  if is_parallel:
    _ExecuteTaskParallel(task_iterator)
  else:
    _ExecuteTasksSequential(task_iterator)
