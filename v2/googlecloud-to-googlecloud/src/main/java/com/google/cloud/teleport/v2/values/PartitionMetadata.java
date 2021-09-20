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
import com.google.common.collect.ImmutableList;

/**
 * Partition metadata for Dataplex.
 *
 * <p>All values are necessary.
 */
@AutoValue
public abstract class PartitionMetadata {
  public abstract String location();

  public abstract ImmutableList<String> values();

  public static Builder builder() {
    return new AutoValue_PartitionMetadata.Builder();
  }

  public GoogleCloudDataplexV1Partition toDataplexPartition() {
    return new GoogleCloudDataplexV1Partition().setLocation(location()).setValues(values());
  }

  /** Builder for {@link PartitionMetadata}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setLocation(String value);

    public abstract Builder setValues(ImmutableList<String> value);

    abstract PartitionMetadata autoBuild();

    public PartitionMetadata build() {
      PartitionMetadata metadata = autoBuild();
      checkState(!metadata.location().isEmpty(), "Location cannot be empty");

      ImmutableList<String> values = metadata.values();
      checkState(!values.isEmpty(), "Values cannot be empty");

      return metadata;
    }
  }
}
