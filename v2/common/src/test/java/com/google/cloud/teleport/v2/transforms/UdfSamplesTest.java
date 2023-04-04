/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptRuntime;
import com.google.common.io.Resources;
import javax.script.ScriptException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for JavaScript files under udf-samples. */
@RunWith(JUnit4.class)
public class UdfSamplesTest {

  @Test
  public void testUdfSamplesEnrich() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFileSystemPath(getSamplePath("enrich.js"))
            .setFunctionName("process")
            .build();
    assertEquals("{\"id\":5,\"source\":\"pos\"}", javascriptRuntime.invoke("{\"id\": 5}"));
    assertEquals("{\"source\":\"pos\"}", javascriptRuntime.invoke("{\"source\": \"sauce\"}"));
  }

  @Test
  public void testUdfSamplesEnrichLog() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFileSystemPath(getSamplePath("enrich_log.js"))
            .setFunctionName("process")
            .build();
    assertEquals(
        "{\"insertId\":5,\"inputSubscription\":\"audit_logs_subscription\"}",
        javascriptRuntime.invoke("{\"insertId\": 5}"));
    assertEquals(
        "{\"data\":{\"insertId\":5,\"inputSubscription\":\"audit_logs_subscription\"},\"attributes\":{}}",
        javascriptRuntime.invoke("{\"data\":{\"insertId\": 5}, \"attributes\":{}}"));
  }

  @Test
  public void testUdfSamplesFilter() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFileSystemPath(getSamplePath("filter.js"))
            .setFunctionName("process")
            .build();
    assertEquals(
        "{\"insertId\":5,\"severity\":\"INFO\"}",
        javascriptRuntime.invoke("{\"insertId\":5,\"severity\":\"INFO\"}"));
    assertNull(javascriptRuntime.invoke("{\"insertId\":5,\"severity\":\"DEBUG\"}"));
  }

  @Test
  public void testUdfSamplesRoute() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFileSystemPath(getSamplePath("route.js"))
            .setFunctionName("process")
            .build();
    assertEquals(
        "{\"insertId\":5,\"severity\":\"INFO\"}",
        javascriptRuntime.invoke("{\"insertId\":5,\"severity\":\"INFO\"}"));
    assertEquals(
        "{\"insertId\":5,\"severity\":\"DEBUG\"}",
        javascriptRuntime.invoke("{\"insertId\":5,\"severity\":\"DEBUG\"}"));

    ScriptException scriptException =
        assertThrows(ScriptException.class, () -> javascriptRuntime.invoke("{\"insertId\":5}"));
    assertThat(scriptException).hasMessageThat().contains("Unrecognized event. eventId=");
  }

  @Test
  public void testUdfSamplesTransform() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFileSystemPath(getSamplePath("transform.js"))
            .setFunctionName("process")
            .build();
    assertEquals(
        "{\"insertId\":5,\"source\":\"unknown\"}", javascriptRuntime.invoke("{\"insertId\":5}"));
    assertEquals(
        "{\"insertId\":5,\"source\":\"google\"}",
        javascriptRuntime.invoke("{\"insertId\":5,\"source\":\"GOOGLE\"}"));
    assertEquals(
        "{\"insertId\":5,\"sensitiveField\":\"REDACTED\",\"source\":\"unknown\"}",
        javascriptRuntime.invoke("{\"insertId\":5,\"sensitiveField\":\"GOOGLE LLC\"}"));
    assertEquals(
        "{\"insertId\":5,\"source\":\"unknown\"}",
        javascriptRuntime.invoke("{\"insertId\":5,\"redundantField\":\"5\"}"));
  }

  @Test
  public void testUdfSamplesTransformCsv() throws Exception {
    JavascriptRuntime javascriptRuntime =
        JavascriptRuntime.newBuilder()
            .setFileSystemPath(getSamplePath("transform_csv.js"))
            .setFunctionName("process")
            .build();
    assertEquals(
        "{"
            + "\"location\":\"FRA\","
            + "\"name\":\"French Roast\","
            + "\"age\":\"3\","
            + "\"color\":\"blue\","
            + "\"coffee\":\"true\""
            + "}",
        javascriptRuntime.invoke("FRA,French Roast,3,blue,true"));
    assertEquals(
        "{\"location\":\"RDU\",\"name\":\"Green Tea\"}", javascriptRuntime.invoke("RDU,Green Tea"));
  }

  private String getSamplePath(String fileName) {
    return Resources.getResource("udf-samples/" + fileName).getPath();
  }
}
