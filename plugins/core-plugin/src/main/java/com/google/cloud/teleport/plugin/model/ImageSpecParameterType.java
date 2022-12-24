/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.plugin.model;

/** Parameter types that are allowed in the UI. */
public enum ImageSpecParameterType {

  /** Generic text parameter. */
  TEXT,

  /** Cloud Storage glob to read file(s). */
  GCS_READ_FILE,

  /** Cloud Storage folder to read. */
  GCS_READ_FOLDER,

  /** Cloud Storage file to write. */
  GCS_WRITE_FILE,

  /** Cloud Storage folder to write. */
  GCS_WRITE_FOLDER,

  /** Pub/Sub Subscription to read. */
  PUBSUB_SUBSCRIPTION,

  /** Pub/Sub Topic to read or write. */
  PUBSUB_TOPIC;
}
