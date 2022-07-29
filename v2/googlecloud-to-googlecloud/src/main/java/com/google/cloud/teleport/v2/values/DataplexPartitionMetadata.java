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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

/**
 * Partition metadata for Dataplex.
 *
 * <p>All values are necessary.
 */
@AutoValue
@DefaultCoder(SchemaCoder.class)
@DefaultSchema(AutoValueSchema.class)
public abstract class DataplexPartitionMetadata implements Serializable {
  public abstract String getLocation();

  public abstract List<String> getValues();

  public static Builder builder() {
    return new AutoValue_DataplexPartitionMetadata.Builder();
  }

  // Special method for constructing new objects for AutoValueSchema/SchemaCoder.
  // Required as they support only the hard-coded "build()" method name in the generated builder
  // class, while we use "autoBuild()" method name here, to implement our own build().
  @SchemaCreate
  public static DataplexPartitionMetadata create(String location, List<String> values) {
    return builder().setLocation(location).setValues(values).build();
  }

  public GoogleCloudDataplexV1Partition toDataplexPartition() {
    return new GoogleCloudDataplexV1Partition().setLocation(getLocation()).setValues(getValues());
  }

  /** Builder for {@link DataplexPartitionMetadata}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setLocation(String value);

    public abstract Builder setValues(List<String> value);

    abstract DataplexPartitionMetadata autoBuild();

    public DataplexPartitionMetadata build() {
      DataplexPartitionMetadata metadata = autoBuild();
      checkState(!metadata.getLocation().isEmpty(), "Location cannot be empty");

      List<String> values = metadata.getValues();
      checkState(!values.isEmpty(), "Values cannot be empty");

      return metadata;
    }
  }
}
