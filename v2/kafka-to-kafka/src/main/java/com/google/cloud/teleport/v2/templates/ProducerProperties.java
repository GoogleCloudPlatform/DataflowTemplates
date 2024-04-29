/*
 * Copyright (C) 2024 Google LLC
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

import com.google.cloud.ServiceOptions;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.cloud.teleport.v2.options.KafkaToKafkaOptions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProducerProperties {
  private static final Logger LOGG = LoggerFactory.getLogger(ProducerProperties.class);

  public static List<String> accessSecretVersion(String projectId, KafkaToKafkaOptions options) {
    List<String> saslCredentials = new ArrayList<>();
    String sinkUsername = options.getSecretIdSinkUsername();
    String sinkUsernameVersionId = options.getVersionIdSinkUsername();
    String sinkPassword = options.getSecretIdSinkPassword();
    String sinkPasswordVersionId = options.getVersionIdSinkPassword();
    String usernameSink = null;
    String passwordSink = null;
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      SecretVersionName secretVersionUsername = SecretVersionName.of(projectId, sinkUsername, sinkUsernameVersionId);
      AccessSecretVersionResponse responseUsername = client.accessSecretVersion(secretVersionUsername);
      usernameSink = responseUsername.getPayload().getData().toStringUtf8();
      saslCredentials.add(usernameSink);
      SecretVersionName secretVersionPassword = SecretVersionName.of(projectId, sinkPassword, sinkPasswordVersionId);
      AccessSecretVersionResponse responsePassword = client.accessSecretVersion(secretVersionPassword);
      passwordSink = responsePassword.getPayload().getData().toStringUtf8();
      saslCredentials.add(passwordSink);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return saslCredentials;

  }


  public static ImmutableMap<String, Object> get(KafkaToKafkaOptions options) {


    String projectId = ServiceOptions.getDefaultProjectId();
    String projectNumber = options.getProjectNumber();
    LOGG.info("Got default ProjectId" + projectId);
    String[] saslCredentials = accessSecretVersion(projectNumber, options).toArray(new String[0]);
    ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
    properties.put(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        options.getSinkBootstrapServer());
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    //         Note: in other languages, set sasl.username and sasl.password instead.
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    properties.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required"
            + " username=\'"
            + saslCredentials[0]
            + "\'"
            + " password=\'"
            + saslCredentials[1]
            + "\';");

    return properties.buildOrThrow();
  }
}
