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
package com.google.cloud.teleport.v2.elasticsearch.utils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for the {@link ConnectionInformation} util. */
public class ConnectionInformationTest {

  /** Rule for exception testing. */
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParseIpHttp() {
    String testUrl = "http://127.0.0.1:443";

    ConnectionInformation connectionInformation = new ConnectionInformation(testUrl);

    assertThat(connectionInformation.getType(), is(equalTo(ConnectionInformation.Type.URL)));
    assertThat(connectionInformation.getElasticsearchURL().toString(), is(equalTo(testUrl)));
  }

  @Test
  public void testParseUrlHttps() {
    String testUrl = "https://host.domain:443";

    ConnectionInformation connectionInformation = new ConnectionInformation(testUrl);

    assertThat(connectionInformation.getType(), is(equalTo(ConnectionInformation.Type.URL)));
    assertThat(connectionInformation.getElasticsearchURL().toString(), is(equalTo(testUrl)));
  }

  @Test
  public void testParseUrlWithPath() {
    String testUrl = "https://host.domain:443/elasticsearch";

    ConnectionInformation connectionInformation = new ConnectionInformation(testUrl);

    assertThat(connectionInformation.getType(), is(equalTo(ConnectionInformation.Type.URL)));
    assertThat(connectionInformation.getElasticsearchURL().toString(), is(equalTo(testUrl)));
  }

  @Test
  public void testParseCloudId() {
    String testUrl =
        "deployment-name:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDNjYTgyYTBlMD"
            + "Q3MjQ0NjViNDQyYzA3N2E4NnVhc2Q1JDU4OWFlODhlZmFjMHFmZjdhMTQzOXR1MjdsYWFmZnIz";

    ConnectionInformation connectionInformation = new ConnectionInformation(testUrl);

    assertThat(connectionInformation.getType(), is(ConnectionInformation.Type.CLOUD_ID));
    assertThat(
        connectionInformation.getElasticsearchURL().toString(),
        is(equalTo("https://3ca82a0e04724465b442c077a86uasd5.us-central1.gcp.cloud.es.io:443")));
    assertThat(
        connectionInformation.getKibanaURL().toString(),
        is(equalTo("https://589ae88efac0qff7a1439tu27laaffr3.us-central1.gcp.cloud.es.io:443")));
  }

  @Test
  public void testParseMalformedUrl() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Illegal base64");

    new ConnectionInformation("htteps://host./hrfc.domain:443");
  }

  @Test
  public void testValidUrls() {
    assertTrue(ConnectionInformation.isValidUrlFormat("http://host.domain"));
    assertTrue(ConnectionInformation.isValidUrlFormat("https://host.domain"));
    assertTrue(ConnectionInformation.isValidUrlFormat("http://host.domain:443"));
    assertTrue(ConnectionInformation.isValidUrlFormat("https://host.domain:8443"));
    assertTrue(ConnectionInformation.isValidUrlFormat("http://host.domain:443/path"));
    assertTrue(ConnectionInformation.isValidUrlFormat("https://host.domain:8443/path"));
  }

  @Test
  public void testInvalidUrls() {
    assertFalse(ConnectionInformation.isValidUrlFormat("tcp://host.domain"));
    assertFalse(ConnectionInformation.isValidUrlFormat("https://host/.domain"));
    assertFalse(ConnectionInformation.isValidUrlFormat("http://host:a"));
  }
}
