/*
 * Copyright (C) 2023 Google LLC
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

import static com.google.common.truth.Truth.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.neo4j.model.Json;
import com.google.cloud.teleport.v2.neo4j.model.Json.ParsingResult;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class ConnectionParamsTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void parsesMinimalBasicAuthInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json(
                "{\"server_url\": \"bolt://example.com\", \"username\": \"neo4j\", \"pwd\": \"password\"}"),
            ConnectionParams.class);

    assertThat(connectionParams).isInstanceOf(BasicConnectionParams.class);
    assertThat(connectionParams)
        .isEqualTo(
            new BasicConnectionParams("bolt://example.com", null, null, "neo4j", "password"));
  }

  @Test
  public void parsesFullBasicAuthInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json(
                "{\"auth_type\": \"basic\", \"server_url\": \"bolt://example.com\", \"custom_ca_certificate_path\": \"gs://example.com/my/cert\", \"database\": \"db\", \"username\": \"neo4j\", \"pwd\": \"password\"}"),
            ConnectionParams.class);

    assertThat(connectionParams)
        .isEqualTo(
            new BasicConnectionParams(
                "bolt://example.com", "db", "gs://example.com/my/cert", "neo4j", "password"));
  }

  @Test
  public void parsesMinimalNoAuthInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json("{\"auth_type\": \"none\", \"server_url\": \"bolt://example.com\"}"),
            ConnectionParams.class);

    assertThat(connectionParams).isInstanceOf(NoAuthConnectionParams.class);
    assertThat(connectionParams)
        .isEqualTo(new NoAuthConnectionParams("bolt://example.com", null, null));
  }

  @Test
  public void parsesFullNoAuthInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json(
                "{\"auth_type\": \"none\", \"server_url\": \"bolt://example.com\", \"database\": \"db\", \"custom_ca_certificate_path\": \"gs://example.com/my/cert\"}"),
            ConnectionParams.class);

    assertThat(connectionParams)
        .isEqualTo(
            new NoAuthConnectionParams("bolt://example.com", "db", "gs://example.com/my/cert"));
  }

  @Test
  public void parsesMinimalKerberosInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json(
                "{\"auth_type\": \"kerberos\", \"server_url\": \"bolt://example.com\", \"ticket\": \"dGhpcyBpcyBhIHRpY2tldA==\"}"),
            ConnectionParams.class);

    assertThat(connectionParams).isInstanceOf(KerberosConnectionParams.class);
    assertThat(connectionParams)
        .isEqualTo(
            new KerberosConnectionParams(
                "bolt://example.com", null, null, "dGhpcyBpcyBhIHRpY2tldA=="));
  }

  @Test
  public void parsesFullKerberosInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json(
                "{\"auth_type\": \"kerberos\", \"server_url\": \"bolt://example.com\", \"database\": \"db\", \"custom_ca_certificate_path\": \"gs://example.com/my/cert\", \"ticket\": \"dGhpcyBpcyBhIHRpY2tldA==\"}"),
            ConnectionParams.class);

    assertThat(connectionParams)
        .isEqualTo(
            new KerberosConnectionParams(
                "bolt://example.com",
                "db",
                "gs://example.com/my/cert",
                "dGhpcyBpcyBhIHRpY2tldA=="));
  }

  @Test
  public void parsesMinimalBearerInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json(
                "{\"auth_type\": \"bearer\", \"server_url\": \"bolt://example.com\", \"token\": \"a-token\"}"),
            ConnectionParams.class);

    assertThat(connectionParams).isInstanceOf(BearerConnectionParams.class);
    assertThat(connectionParams)
        .isEqualTo(new BearerConnectionParams("bolt://example.com", null, null, "a-token"));
  }

  @Test
  public void parsesFullBearerInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json(
                "{\"auth_type\": \"bearer\", \"server_url\": \"bolt://example.com\", \"database\": \"db\", \"custom_ca_certificate_path\": \"gs://example.com/ca/cert\", \"token\": \"a-token\"}"),
            ConnectionParams.class);

    assertThat(connectionParams)
        .isEqualTo(
            new BearerConnectionParams(
                "bolt://example.com", "db", "gs://example.com/ca/cert", "a-token"));
  }

  @Test
  public void parsesMinimalCustomInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json(
                "{\"auth_type\": \"custom\", \"server_url\": \"bolt://example.com\", \"principal\": \"a-principal\", \"credentials\": \"some-credentials\", \"scheme\": \"a-scheme\"}"),
            ConnectionParams.class);

    assertThat(connectionParams).isInstanceOf(CustomConnectionParams.class);
    assertThat(connectionParams)
        .isEqualTo(
            new CustomConnectionParams(
                "bolt://example.com",
                null,
                null,
                "a-principal",
                "some-credentials",
                null,
                "a-scheme",
                null));
  }

  @Test
  public void parsesFullCustomInformation() throws Exception {
    ConnectionParams connectionParams =
        Json.map(
            json(
                "{\"auth_type\": \"custom\", \"server_url\": \"bolt://example.com\", \"database\": \"db\", \"custom_ca_certificate_path\": \"gs://example.com/my/cert\", \"principal\": \"a-principal\", \"credentials\": \"some-credentials\", \"realm\": \"a-realm\", \"scheme\": \"a-scheme\", \"parameters\": {\"foo\": \"bar\", \"baz\": true}}"),
            ConnectionParams.class);

    assertThat(connectionParams)
        .isEqualTo(
            new CustomConnectionParams(
                "bolt://example.com",
                "db",
                "gs://example.com/my/cert",
                "a-principal",
                "some-credentials",
                "a-realm",
                "a-scheme",
                Map.of("foo", "bar", "baz", true)));
  }

  @NotNull
  private ParsingResult json(String json) throws JsonProcessingException {
    return ParsingResult.success(mapper.readTree(json));
  }
}
