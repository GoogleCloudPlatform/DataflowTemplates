# -*- coding: utf-8 -*- #
# Copyright 2015 Google LLC. All Rights Reserved.
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
"""The command group for cloud dataproc clusters."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base


@base.ReleaseTracks(base.ReleaseTrack.ALPHA, base.ReleaseTrack.BETA,
                    base.ReleaseTrack.GA)
class Clusters(base.Group):
  """Create and manage Google Cloud Dataproc clusters.

  Create and manage Google Cloud Dataproc clusters.

  ## EXAMPLES

  To create a cluster, run:

    $ {command} create my_cluster --region=us-central1

  To resize a cluster, run:

    $ {command} update my_cluster --region=us-central1 --num_workers 5

  To delete a cluster, run:

    $ {command} delete my_cluster --region=us-central1

  To view the details of a cluster, run:

    $ {command} describe my_cluster --region=us-central1

  To see the list of all clusters, run:

    $ {command} list
  """
