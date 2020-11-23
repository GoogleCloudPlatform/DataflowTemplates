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
"""Implements logic for tracking task dependencies in task_graph_executor.

See go/parallel-processing-in-gcloud-storage for more information.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import threading


class TaskWrapper:
  """Embeds a task.Task instance in a dependency graph.

  Attributes:
    id (int): A unique identifier for this task wrapper.
    task (task.Task): An instance of a task class .
    dependency_count (int): The number of unexecuted dependencies this task has,
      i.e. this node's in-degree in a graph where an edge from A to B indicates
      that A must be executed before B.
    dependent_task_ids (Optional[Iterable[int]]): The id of the tasks that
      require this task to be completed for their own completion. This value
      should be None if no tasks depend on this one.
  """

  def __init__(self, task_id, task, dependent_task_ids):
    self.id = task_id
    self.task = task
    self.dependency_count = 0
    self.dependent_task_ids = dependent_task_ids


class TaskGraph:
  """Tracks dependencies between task.Task instances.

  The public methods in this class are thread safe.
  """

  def __init__(self):
    # Used to synchronize graph updates.
    self._lock = threading.Lock()

    # Incremented every time a task is added.
    self._id_counter = 0

    # A dict[int, TaskWrapper]. Maps ids to task wrapper instances for tasks
    # currently in the graph.
    self._task_wrappers_in_graph = {}

  def add(self, task, dependent_task_ids=None):
    """Adds a task to the graph.

    Args:
      task (task.Task): The task to be added.
      dependent_task_ids (Optional[List[int]]): TaskWrapper.id attributes for
        tasks already in the graph that require the task being added to complete
        before being executed. This argument should be None for top-level tasks,
        which no other tasks depend on.

    Returns:
      a TaskWrapper instance for the task passed into this function.

    Raises:
      KeyError if any id in dependent_task_ids is not in the graph, or if a
      self-dependency would have been created.
    """
    # TODO(b/171297219): Limit the number of top level tasks (dependent_task_ids
    # is None) by acquiring a semaphore.
    with self._lock:
      task_wrapper = TaskWrapper(self._id_counter, task, dependent_task_ids)
      self._id_counter += 1

      if dependent_task_ids is not None:
        for task_id in dependent_task_ids:
          self._task_wrappers_in_graph[task_id].dependency_count += 1

      self._task_wrappers_in_graph[task_wrapper.id] = task_wrapper
    return task_wrapper
