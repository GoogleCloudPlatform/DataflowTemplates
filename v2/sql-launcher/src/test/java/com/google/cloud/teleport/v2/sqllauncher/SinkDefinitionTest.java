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
package com.google.cloud.teleport.v2.sqllauncher;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.sqllauncher.PubSubSinkDefinition.CreateDisposition;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link SinkDefinition} serialization. */
public class SinkDefinitionTest {
  @Test
  public void parse_bigQueryDefinitionHappyPath_yieldsCorrectTable() throws IOException {
    String serialized =
        "{"
            + "  \"type\": \"bigquery\","
            + "  \"table\": {"
            + "    \"projectId\" : \"fake-project\","
            + "    \"datasetId\" : \"fake-dataset\","
            + "    \"tableId\" : \"fake-table\""
            + "  },"
            + "  \"writeDisposition\": \"WRITE_EMPTY\""
            + "}";

    SinkDefinition parsed = parseSinkDefinition(serialized);

    assertThat(parsed, instanceOf(BigQuerySinkDefinition.class));

    BigQuerySinkDefinition definition = (BigQuerySinkDefinition) parsed;

    assertThat(definition.getTable().getProjectId(), equalTo("fake-project"));
    assertThat(definition.getTable().getDatasetId(), equalTo("fake-dataset"));
    assertThat(definition.getTable().getTableId(), equalTo("fake-table"));
    assertThat(definition.getWriteDisposition(), equalTo(WriteDisposition.WRITE_EMPTY));
  }

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void parse_bigQueryDefinitionNoTable_throws() throws IOException {
    String serialized = "{\"type\": \"bigquery\",  \"writeDisposition\": \"WRITE_TRUNCATE\"}";

    exception.expect(JsonProcessingException.class);
    exception.expectMessage(containsString("table"));
    SinkDefinition parsed = parseSinkDefinition(serialized);
  }

  @Test
  public void parse_bigQueryDefinitionNoDisposition_throws() throws IOException {
    String serialized =
        "{"
            + "  \"type\": \"bigquery\","
            + "  \"table\": {"
            + "    \"projectId\" : \"fake-project\","
            + "    \"datasetId\" : \"fake-dataset\","
            + "    \"tableId\" : \"fake-table\""
            + "  }"
            + "}";

    exception.expect(JsonProcessingException.class);
    exception.expectMessage(containsString("writeDisposition"));
    SinkDefinition parsed = parseSinkDefinition(serialized);
  }

  @Test
  public void parse_bigQueryDefinitionBadDisposition_throws() throws IOException {
    String serialized =
        "{"
            + "  \"type\": \"bigquery\","
            + "  \"table\": {"
            + "    \"projectId\" : \"fake-project\","
            + "    \"datasetId\" : \"fake-dataset\","
            + "    \"tableId\" : \"fake-table\""
            + "  },"
            + "  \"writeDisposition\": \"FOO\""
            + "}";

    exception.expect(JsonProcessingException.class);
    exception.expectMessage(containsString("writeDisposition"));
    SinkDefinition parsed = parseSinkDefinition(serialized);
  }

  @Test
  public void parse_definitionWithoutType_throws() throws IOException {
    String serialized =
        "{"
            + "  \"table\": {"
            + "    \"projectId\" : \"fake-project\","
            + "    \"datasetId\" : \"fake-dataset\","
            + "    \"tableId\" : \"fake-table\""
            + "  },"
            + "  \"writeDisposition\": \"WRITE_IF_EMPTY\""
            + "}";

    exception.expect(JsonProcessingException.class);
    exception.expectMessage(allOf(containsString("type"), containsString("missing")));
    SinkDefinition parsed = parseSinkDefinition(serialized);
  }

  @Test
  public void parse_definitionWithBadType_throws() throws IOException {
    String serialized =
        "{"
            + "\"type\": \"foobar\""
            + "  \"table\": {"
            + "    \"projectId\" : \"fake-project\","
            + "    \"datasetId\" : \"fake-dataset\","
            + "    \"tableId\" : \"fake-table\""
            + "  },"
            + "  \"writeDisposition\": \"WRITE_IF_EMPTY\""
            + "}";

    exception.expect(JsonProcessingException.class);
    exception.expectMessage(allOf(containsString("type"), containsString("foobar")));
    SinkDefinition parsed = parseSinkDefinition(serialized);
  }

  @Test
  public void parse_pubsubDefinitionHappyPath_yieldsCorrectTopic() throws IOException {
    String serialized =
        "{"
            + "  \"type\": \"pubsub\","
            + "  \"projectId\" : \"fake-project\","
            + "  \"topic\" : \"fake-topic\","
            + "  \"createDisposition\": \"CREATE_IF_NOT_FOUND\""
            + "}";

    SinkDefinition parsed = parseSinkDefinition(serialized);

    assertThat(parsed, instanceOf(PubSubSinkDefinition.class));

    PubSubSinkDefinition definition = (PubSubSinkDefinition) parsed;

    assertThat(definition.getProjectId(), equalTo("fake-project"));
    assertThat(definition.getTopic(), equalTo("fake-topic"));
    assertThat(definition.getCreateDisposition(), equalTo(CreateDisposition.CREATE_IF_NOT_FOUND));
  }

  @Test
  public void parse_pubsubDefinitionNoProjectId_throws() throws IOException {
    String serialized =
        "{\"type\": \"pubsub\", \"topic\" : \"fake-topic\", \"createDisposition\":"
            + " \"FAIL_IF_NOT_FOUND\"}";

    exception.expect(JsonProcessingException.class);
    exception.expectMessage(containsString("projectId"));
    SinkDefinition parsed = parseSinkDefinition(serialized);
  }

  @Test
  public void parse_pubsubDefinitionNoTopic_throws() throws IOException {
    String serialized =
        "{\"type\": \"pubsub\", \"projectId\" : \"fake-project\", \"createDisposition\":"
            + " \"CREATE_IF_NOT_FOUND\"}";

    exception.expect(JsonProcessingException.class);
    exception.expectMessage(containsString("topic"));
    SinkDefinition parsed = parseSinkDefinition(serialized);
  }

  @Test
  public void parse_pubsubDefinitionNoCreateDisposition_throws() throws IOException {
    String serialized =
        "{\"type\": \"pubsub\", \"projectId\" : \"fake-project\", \"topic\": \"fake-topic\"}";

    exception.expect(JsonProcessingException.class);
    exception.expectMessage(containsString("createDisposition"));
    SinkDefinition parsed = parseSinkDefinition(serialized);
  }

  private SinkDefinition parseSinkDefinition(String serialized) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(serialized, SinkDefinition.class);
  }
}
