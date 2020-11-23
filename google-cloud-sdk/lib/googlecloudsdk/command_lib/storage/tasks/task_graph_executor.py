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
"""Implements parallel task execution for the storage surface.

See go/parallel-processing-in-gcloud-storage for more information.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import multiprocessing
import threading

_SHUTDOWN = 'SHUTDOWN'


def _thread_worker(task_queue, task_output_queue):
  """A consumer thread run in a child process.

  Args:
    task_queue (multiprocessing.Queue): A queue to get
      command_lib.storage.tasks.task.Task instances from.
    task_output_queue (multiprocessing.Queue): A queue used to send the output
      of Task.execute back to the main process.
  """
  while True:
    task = task_queue.get()

    # Addressing b/171297037 will involve adding tasks to a task queue in a
    # separate producer thread. This means that we will not be able to rely on
    # multiprocessing.Queue.empty() as an indicator of when the executor is able
    # to halt, since it's possible that threads could be scheduled such that the
    # main task queue would be cleared before the producer thread could add more
    # tasks. This could lead to consumer threads exiting early.
    # TODO(b/171297037): Reword the comment above removing mention of this bug.
    # TODO(b/171297438): Add a note about another potential problem:
    # If the last task in the task_queue spawns additional child tasks,
    # undesired shutdowns could occur before those child tasks are added back to
    # the task queue.
    if task == _SHUTDOWN:
      task_queue.put(_SHUTDOWN)
      break

    additional_task_iterators = task.execute()
    task_output_queue.put(additional_task_iterators)


def _process_worker(task_queue, task_output_queue, thread_count):
  """Starts a consumer thread pool.

  Args:
    task_queue (multiprocessing.Queue): A queue to get
      command_lib.storage.tasks.task.Task instances from.
    task_output_queue (multiprocessing.Queue): A queue used to send the output
      of Task.execute back to the main process.
    thread_count (int): The number of threads to use.
  """
  # TODO(b/171299704): Add logic from gcloud_main.py to initialize GCE and
  # DevShell credentials in processes started with spawn.
  threads = []
  for _ in range(thread_count):
    thread = threading.Thread(
        target=_thread_worker, args=(task_queue, task_output_queue))
    thread.start()
    threads.append(thread)

  for thread in threads:
    thread.join()


def task_graph_executor(task_iterator,
                        process_count=multiprocessing.cpu_count(),
                        thread_count=4):
  """Executes tasks from task_iterator in parallel.

  Uses process_count * thread_count workers to execute
  command_lib.storage.tasks.task.Task instances.

  Args:
    task_iterator (Iterable[Task]): The command_lib.storage.tasks.task.Task
      instances to execute.
    process_count (int): The number of processes to use.
    thread_count (int): The number of threads to start per process.
  """
  task_queue = multiprocessing.Queue()
  task_output_queue = multiprocessing.Queue()

  # TODO(b/171297037): Move this to a separate producer thread and set
  # task_queue's max size equal to process_count * thread_count.
  for task in task_iterator:
    task_queue.put(task)
  task_queue.put(_SHUTDOWN)

  processes = []
  for _ in range(process_count):
    process = multiprocessing.Process(
        target=_process_worker,
        args=(task_queue, task_output_queue, thread_count))
    process.start()
    processes.append(process)

  for process in processes:
    process.join()

  # TODO(b/171297037): Start a thread that moves tasks from task_iterator to
  # task_buffer.

  # TODO(b/171297037): Start a thread that moves tasks from task_buffer to
  # task_queue.

  # TODO(b/171297438): Start a thread that updates a graph of task dependencies
  # based on the return value of Task.execute calls.
