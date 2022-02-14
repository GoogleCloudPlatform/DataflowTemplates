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
package com.google.cloud.teleport.v2.values;

import com.google.api.services.bigquery.model.TableReference;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.utils.SerializableSchemaSupplier;
import java.io.Serializable;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/** BigQuery table metadata. */
@AutoValue
@DefaultCoder(SchemaCoder.class)
@DefaultSchema(AutoValueSchema.class)
public abstract class BigQueryTable implements Serializable {
  public abstract String getProject();

  public abstract String getDataset();

  public abstract String getTableName();

  @Nullable
  public abstract String getPartitioningColumn();

  @Nullable
  public abstract List<BigQueryTablePartition> getPartitions();

  /** @return timestamp in microseconds since epoch (UNIX time) */
  public abstract long getLastModificationTime();

  public abstract SerializableSchemaSupplier getSchemaSupplier();

  public Schema getSchema() {
    return getSchemaSupplier().get();
  }

  public static Builder builder() {
    return new AutoValue_BigQueryTable.Builder();
  }

  public boolean isPartitioned() {
    return getPartitioningColumn() != null;
  }

  public TableReference toTableReference() {
    return new TableReference()
        .setDatasetId(getDataset())
        .setProjectId(getProject())
        .setTableId(getTableName());
  }

  public abstract Builder toBuilder();

  /** Builder for {@link BigQueryTable}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract String getProject();

    public abstract Builder setProject(String value);

    public abstract String getDataset();

    public abstract Builder setDataset(String value);

    public abstract String getTableName();

    public abstract Builder setTableName(String value);

    public abstract String getPartitioningColumn();

    public abstract Builder setPartitioningColumn(String value);

    public abstract List<BigQueryTablePartition> getPartitions();

    public abstract Builder setPartitions(List<BigQueryTablePartition> partitions);

    /**
     * Returns the partition last modification time in <b>microseconds</b> since epoch (UNIX time).
     */
    public abstract long getLastModificationTime();

    /** @param value timestamp in microseconds since epoch (UNIX time) */
    public abstract Builder setLastModificationTime(long value);

    public abstract SerializableSchemaSupplier getSchemaSupplier();

    public abstract Builder setSchemaSupplier(SerializableSchemaSupplier schema);

    public abstract BigQueryTable build();

    public Schema getSchema() {
      return getSchemaSupplier().get();
    }

    public Builder setSchema(Schema schema) {
      return setSchemaSupplier(SerializableSchemaSupplier.of(schema));
    }
  }
}
