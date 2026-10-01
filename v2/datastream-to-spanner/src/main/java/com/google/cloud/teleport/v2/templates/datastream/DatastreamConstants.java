/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.templates.datastream;

/** A single class to store all constants related to Datastream. */
public class DatastreamConstants {

  /* The key for the event change type in the event json */
  public static final String EVENT_CHANGE_TYPE_KEY = "_metadata_change_type";

  public static final String INSERT_EVENT = "INSERT";

  public static final String UPDATE_EVENT = "UPDATE";

  public static final String UPDATE_INSERT_EVENT = "UPDATE-INSERT";

  public static final String UPDATE_DELETE_EVENT = "UPDATE-DELETE";

  public static final String DELETE_EVENT = "DELETE";

  public static final String EMPTY_EVENT = "";

  /* The key for the source database type in the event json */
  public static final String EVENT_SOURCE_TYPE_KEY = "_metadata_source_type";

  /* The key for the schema name in the event json */
  public static final String EVENT_SCHEMA_KEY = "_metadata_schema";

  /* The key for the table name in the event json */
  public static final String EVENT_TABLE_NAME_KEY = "_metadata_table";

  /* The key for the uuid field of the change event */
  public static final String EVENT_UUID_KEY = "_metadata_uuid";

  /* The key for the read method in the event json */
  public static final String EVENT_READ_METHOD_KEY = "_metadata_read_method";

  /* The prefix for all metadata keys in the event json */
  public static final String EVENT_METADATA_KEY_PREFIX = "_metadata";

  /* The key for stream name in the event json */
  public static final String EVENT_STREAM_NAME = "_metadata_stream";
}
