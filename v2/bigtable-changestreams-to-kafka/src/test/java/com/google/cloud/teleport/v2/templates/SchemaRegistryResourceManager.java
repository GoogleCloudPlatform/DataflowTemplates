/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.testcontainers.TestContainerResourceManager;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryResourceManager extends TestContainerResourceManager<GenericContainer<?>>
    implements ResourceManager {
  private static final String IMAGE_NAME = "confluentinc/cp-schema-registry";
  private static final String IMAGE_TAG = "7.3.1";
  private static final String ADVERTISED_HOST_NAME = "whatever";
  private static final Integer INTERNAL_PORT = 8081;

  private SchemaRegistryResourceManager(Builder b) {
    super(
        new GenericContainer<>(DockerImageName.parse(IMAGE_NAME).withTag(IMAGE_TAG))
            .withEnv(
                Map.of(
                    "SCHEMA_REGISTRY_HOST_NAME",
                    ADVERTISED_HOST_NAME,
                    "SCHEMA_REGISTRY_LISTENERS",
                    "http://0.0.0.0:" + INTERNAL_PORT,
                    "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                    Objects.requireNonNull(b.kafkaBootstrapServers)))
            .withExposedPorts(INTERNAL_PORT)
            .withAccessToHost(true)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200)),
        b);
  }

  public String getConnectionString() {
    return "http://" + getHost() + ":" + getPort(INTERNAL_PORT);
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  public static final class Builder
      extends TestContainerResourceManager.Builder<SchemaRegistryResourceManager> {
    private String kafkaBootstrapServers;

    private Builder(String testId) {
      super(testId, IMAGE_NAME, IMAGE_TAG);
    }

    public Builder withTestcontainerKafkaBootstrapServers(Set<Integer> hostPorts) {
      if (hostPorts.isEmpty()) {
        throw new IllegalArgumentException("At least one Kafka port needed.");
      }
      kafkaBootstrapServers =
          hostPorts.stream()
              .map(p -> "PLAINTEXT://" + GenericContainer.INTERNAL_HOST_HOSTNAME + ":" + p)
              .collect(Collectors.joining(","));
      Testcontainers.exposeHostPorts(
          hostPorts.stream().collect(Collectors.toMap(Function.identity(), Function.identity())));
      return this;
    }

    @Override
    public SchemaRegistryResourceManager build() {
      return new SchemaRegistryResourceManager(this);
    }
  }
}
