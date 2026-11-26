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
package com.google.cloud.teleport.v2.neo4j.model.connection;

import com.google.cloud.teleport.v2.neo4j.model.InputValidator;
import com.google.cloud.teleport.v2.neo4j.model.Json;
import com.google.cloud.teleport.v2.neo4j.utils.FileSystemUtils;
import com.google.cloud.teleport.v2.neo4j.utils.SerializableSupplier;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import java.io.Serial;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;

/**
 * Lazily resolves the Neo4j connection settings.
 *
 * <p>Only the connection reference (a GCS URI or a Secret Manager secret id) is captured by this
 * provider, so the actual settings - including passwords and tokens - are neither read by the
 * launcher nor embedded in the serialized Dataflow pipeline. Resolution happens on the first call
 * to {@link #get()}, that is, on a worker.
 */
public class ConnectionParamsProvider implements SerializableSupplier<ConnectionParams> {

  @Serial private static final long serialVersionUID = 1L;

  // Workers are short-lived and may host many DoFn instances; sharing the resolved settings avoids
  // a Secret Manager or GCS call per DoFn. Nothing is serialized: this cache lives in the worker
  // JVM only.
  private static final Map<String, ConnectionParams> CACHE = new ConcurrentHashMap<>();

  private final String secretId;

  private final String uri;

  public ConnectionParamsProvider(String secretId, String uri) {
    this.secretId = StringUtils.isNotEmpty(secretId) ? secretId : null;
    this.uri = uri;
  }

  @Override
  public ConnectionParams get() {
    return CACHE.computeIfAbsent(cacheKey(), key -> resolve());
  }

  private String cacheKey() {
    return secretId != null ? "secret:" + secretId : "uri:" + uri;
  }

  private ConnectionParams resolve() {
    String connectionJson = readConnectionSettings();
    Json.ParsingResult parsingResult = InputValidator.validateNeo4jConnection(connectionJson);
    if (!parsingResult.isSuccessful()) {
      throw new IllegalArgumentException(
          String.join(
              System.lineSeparator(),
              parsingResult.formatErrors("Could not validate connection JSON")));
    }
    return Json.map(parsingResult, ConnectionParams.class);
  }

  private String readConnectionSettings() {
    if (secretId != null) {
      return SecretManagerUtils.getSecret(secretId);
    }
    try {
      return FileSystemUtils.getPathContents(uri);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Unable to read Neo4j configuration at URI %s: ", uri), e);
    }
  }
}
