# -*- coding: utf-8 -*- #
# Copyright 2013 Google LLC. All Rights Reserved.
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

"""The gcloud datastore group."""

from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from googlecloudsdk.calliope import base


class Datastore(base.Group):
  """Manage your Cloud Datastore resources.

    Cloud Datastore is a highly-scalable NoSQL database for your applications.
    Cloud Datastore automatically handles sharding and replication, providing
    you with a highly available and durable database that scales automatically
    to handle your applications' load.

    More information on Cloud Datastore can be found here:
    https://cloud.google.com/datastore and detailed documentation can be
    found here: https://cloud.google.com/datastore/docs

    export -- Export data to Google Cloud Storage

    import -- Import data from Google Cloud Storage

    indexes -- Manage your Cloud Firestore indexes.

    operations -- Manage Long Running Operations for Cloud Firestore.
  """

  category = base.DATABASES_CATEGORY

  def Filter(self, context, args):
    del context, args
    base.DisableUserProjectQuota()
