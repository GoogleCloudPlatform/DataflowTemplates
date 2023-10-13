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
package com.google.cloud.teleport.v2.values;

/** Useful enums used in Dataplex API calls. */
public interface DataplexEnums {

  /** Enum for the ResourceSpec types in Dataplex Assets. */
  enum DataplexAssetResourceSpec {
    STORAGE_BUCKET,
    BIGQUERY_DATASET,
  }

  /** Enum for the entity types. */
  enum EntityType {
    TYPE_UNSPECIFIED,
    TABLE,
    FILESET
  }

  /** Enum for the storage system types. */
  enum StorageSystem {
    STORAGE_SYSTEM_UNSPECIFIED,
    CLOUD_STORAGE,
    BIGQUERY
  }

  /** Enum for the storage format. */
  enum StorageFormat {
    FORMAT_UNSPECIFIED(null),
    PARQUET("application/x-parquet"),
    AVRO("application/x-avro"),
    ORC("application/x-orc"),
    CSV("text/csv"),
    JSON("application/json"),
    TFRECORD("application/x-tfrecord"),
    OTHER(null),
    UNKNOWN(null);

    private final String mimeType;

    StorageFormat(String mimeType) {
      this.mimeType = mimeType;
    }

    public String getMimeType() {
      return mimeType;
    }
  }

  /** Enum for the compression format. */
  enum CompressionFormat {
    COMPRESSION_FORMAT_UNSPECIFIED,
    GZIP,
    BZIP2
  }

  /** Enum for the schema field types. */
  enum FieldType {
    TYPE_UNSPECIFIED,
    BOOLEAN,
    BYTE,
    INT16,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    DECIMAL,
    STRING,
    BINARY,
    TIMESTAMP,
    DATE,
    TIME,
    RECORD,
    NULL
  }

  /** Enum for the schema field modes. */
  enum FieldMode {
    MODE_UNSPECIFIED,
    REQUIRED,
    NULLABLE,
    REPEATED
  }

  /** Enum for the partition styles. */
  enum PartitionStyle {
    PARTITION_STYLE_UNSPECIFIED,
    HIVE_COMPATIBLE
  }
}
