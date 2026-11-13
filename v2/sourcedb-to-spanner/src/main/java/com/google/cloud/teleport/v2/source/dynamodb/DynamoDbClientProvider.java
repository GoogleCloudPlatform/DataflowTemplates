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

import com.google.cloud.teleport.v2.spanner.migrations.source.config.DynamoDbConnectionConfig;
import java.net.URI;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/** Utility class for constructing DynamoDB SDK clients and Beam {@link ClientConfiguration}. */
public final class DynamoDbClientProvider {

  public static final String DEFAULT_REGION = "us-east-1";

  private DynamoDbClientProvider() {}

  /** Builds a synchronous {@link DynamoDbClient} from {@link DynamoDbConnectionConfig}. */
  public static DynamoDbClient createClient(DynamoDbConnectionConfig config) {
    Region region = getRegion(config);
    AwsCredentialsProvider credentialsProvider = getCredentialsProvider(config);
    DynamoDbClientBuilder builder =
        DynamoDbClient.builder().region(region).credentialsProvider(credentialsProvider);
    if (config.getEndpoint() != null && !config.getEndpoint().isBlank()) {
      builder.endpointOverride(URI.create(config.getEndpoint().trim()));
    }
    return builder.build();
  }

  /** Builds a Beam {@link ClientConfiguration} from {@link DynamoDbConnectionConfig}. */
  public static ClientConfiguration createBeamClientConfiguration(DynamoDbConnectionConfig config) {
    Region region = getRegion(config);
    AwsCredentialsProvider credentialsProvider = getCredentialsProvider(config);
    URI endpointUri = null;
    if (config.getEndpoint() != null && !config.getEndpoint().isBlank()) {
      endpointUri = URI.create(config.getEndpoint().trim());
    }
    return ClientConfiguration.create(credentialsProvider, region, endpointUri);
  }

  static Region getRegion(DynamoDbConnectionConfig config) {
    if (config != null && config.getRegion() != null && !config.getRegion().isBlank()) {
      return Region.of(config.getRegion().trim());
    }
    return Region.of(DEFAULT_REGION);
  }

  static AwsCredentialsProvider getCredentialsProvider(DynamoDbConnectionConfig config) {
    if (config != null
        && config.getAccessKey() != null
        && !config.getAccessKey().isBlank()
        && config.getSecretKey() != null
        && !config.getSecretKey().isBlank()) {
      return StaticCredentialsProvider.create(
          AwsBasicCredentials.create(config.getAccessKey().trim(), config.getSecretKey().trim()));
    }
    if (config != null && config.getEndpoint() != null && !config.getEndpoint().isBlank()) {
      // DynamoDB Local accepts any non-empty credentials when an explicit endpoint is set.
      return StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"));
    }
    return DefaultCredentialsProvider.create();
  }
}
