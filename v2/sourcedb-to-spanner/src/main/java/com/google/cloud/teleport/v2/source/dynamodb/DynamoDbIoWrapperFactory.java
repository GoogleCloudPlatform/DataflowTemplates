/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.source.dynamodb;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.reader.IoWrapperFactory;
import com.google.cloud.teleport.v2.reader.io.IoWrapper;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.DynamoDbConnectionConfig;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.SourceConnectionConfig;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.Wait.OnSignal;

/** Factory for creating {@link DynamoDbIoWrapper} instances. */
@AutoValue
public abstract class DynamoDbIoWrapperFactory implements IoWrapperFactory {

  public abstract DynamoDbConnectionConfig dynamoDbConnectionConfig();

  @Nullable
  public abstract Integer numPartitions();

  public static DynamoDbIoWrapperFactory fromConfig(
      SourceConnectionConfig sourceConnectionConfig, @Nullable Integer numPartitions) {
    if (!(sourceConnectionConfig instanceof DynamoDbConnectionConfig)) {
      throw new IllegalArgumentException(
          "Unsupported source connection config type for DynamoDB: " + sourceConnectionConfig);
    }
    return new AutoValue_DynamoDbIoWrapperFactory(
        (DynamoDbConnectionConfig) sourceConnectionConfig, numPartitions);
  }

  @Override
  public IoWrapper getIOWrapper(List<String> sourceTables, OnSignal<?> waitOnSignal) {
    return new DynamoDbIoWrapper(dynamoDbConnectionConfig(), sourceTables, numPartitions());
  }
}
