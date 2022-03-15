/*
 * Copyright (C) 2020 Google LLC
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

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for FormatDatastreamRecordToJson function. These check appropriate Avro-to-Json conv. */
@RunWith(JUnit4.class)
public class FormatDatastreamJsonToJsonTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final String EXAMPLE_DATASTREAM_JSON =
      "{\"uuid\":\"00c32134-f50e-4460-a6c0-399900010010\",\"read_timestamp\":\"2021-12-25"
          + " 05:42:04.408\","
          + "\"source_timestamp\":\"2021-12-25T05:42:04.408\",\"object\":\"HR_JOBS\",\"read_method\":\"oracle-backfill\",\"stream_name\":\"projects/402074789819/locations/us-central1/streams/destroy\",\"schema_key\":\"ebdb5545a7610cee7b1caae4a45dec7fd3b46fdc\",\"sort_keys\":[1640410924408,1706664,\"\",0],\"source_metadata\":{\"schema\":\"HR\",\"table\":\"JOBS\",\"database\":\"XE\",\"row_id\":\"AAAEARAAEAAAAC9AAS\",\"scn\":1706664,\"is_deleted\":false,\"change_type\":\"INSERT\",\"ssn\":0,\"rs_id\":\"\",\"tx_id\":null,\"log_file\":\"\",\"primary_keys\":[\"JOB_ID\"]},\"payload\":{\"JOB_ID\":\"PR_REP\",\"JOB_TITLE\":\"Public"
          + " Relations Representative\",\"MIN_SALARY\":4500,\"MAX_SALARY\":10500}}";

  private static final String EXAMPLE_DATASTREAM_RECORD =
      "{\"_metadata_stream\":\"my-stream\",\"_metadata_timestamp\":1640410924,\"_metadata_read_timestamp\":1640410924,\"_metadata_read_method\":\"oracle-backfill\",\"_metadata_source_type\":\"oracle\",\"_metadata_deleted\":false,\"_metadata_table\":\"JOBS\",\"_metadata_change_type\":\"INSERT\",\"_metadata_primary_keys\":[\"JOB_ID\"],\"_metadata_schema\":\"HR\",\"_metadata_row_id\":\"AAAEARAAEAAAAC9AAS\",\"_metadata_scn\":1706664,\"_metadata_ssn\":0,\"_metadata_rs_id\":\"\",\"_metadata_tx_id\":null,\"JOB_ID\":\"PR_REP\",\"JOB_TITLE\":\"Public"
          + " Relations"
          + " Representative\",\"MIN_SALARY\":4500,\"MAX_SALARY\":10500,\"rowid\":\"AAAEARAAEAAAAC9AAS\",\"_metadata_source\":{\"schema\":\"HR\",\"table\":\"JOBS\",\"database\":\"XE\",\"row_id\":\"AAAEARAAEAAAAC9AAS\",\"scn\":1706664,\"is_deleted\":false,\"change_type\":\"INSERT\",\"ssn\":0,\"rs_id\":\"\",\"tx_id\":null,\"log_file\":\"\",\"primary_keys\":[\"JOB_ID\"]}}";

  private static final String EXAMPLE_DATASTREAM_RECORD_WITH_HASH_ROWID =
      "{\"_metadata_stream\":\"my-stream\",\"_metadata_timestamp\":1640410924,\"_metadata_read_timestamp\":1640410924,\"_metadata_read_method\":\"oracle-backfill\",\"_metadata_source_type\":\"oracle\",\"_metadata_deleted\":false,\"_metadata_table\":\"JOBS\",\"_metadata_change_type\":\"INSERT\",\"_metadata_primary_keys\":[\"JOB_ID\"],\"_metadata_schema\":\"HR\",\"_metadata_row_id\":1019670290924988842,\"_metadata_scn\":1706664,\"_metadata_ssn\":0,\"_metadata_rs_id\":\"\",\"_metadata_tx_id\":null,\"JOB_ID\":\"PR_REP\",\"JOB_TITLE\":\"Public"
          + " Relations"
          + " Representative\",\"MIN_SALARY\":4500,\"MAX_SALARY\":10500,\"rowid\":1019670290924988842,\"_metadata_source\":{\"schema\":\"HR\",\"table\":\"JOBS\",\"database\":\"XE\",\"row_id\":\"AAAEARAAEAAAAC9AAS\",\"scn\":1706664,\"is_deleted\":false,\"change_type\":\"INSERT\",\"ssn\":0,\"rs_id\":\"\",\"tx_id\":null,\"log_file\":\"\",\"primary_keys\":[\"JOB_ID\"]}}";

  @Test
  public void testProcessElement_validJson() {
    Map<String, String> renameColumns = ImmutableMap.of("_metadata_row_id", "rowid");

    FailsafeElement<String, String> expectedElement =
        FailsafeElement.of(EXAMPLE_DATASTREAM_RECORD, EXAMPLE_DATASTREAM_RECORD);

    PCollection<FailsafeElement<String, String>> pCollection =
        pipeline
            .apply("CreateInput", Create.of(EXAMPLE_DATASTREAM_JSON))
            .apply(
                "FormatDatastreamJsonToJson",
                ParDo.of(
                    (FormatDatastreamJsonToJson)
                        FormatDatastreamJsonToJson.create()
                            .withStreamName("my-stream")
                            .withRenameColumnValues(renameColumns)
                            .withLowercaseSourceColumns(false)))
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(pCollection).containsInAnyOrder(expectedElement);

    pipeline.run();
  }

  @Test
  public void testProcessElement_hashRowId() {
    Map<String, String> renameColumns = ImmutableMap.of("_metadata_row_id", "rowid");

    FailsafeElement<String, String> expectedElement =
        FailsafeElement.of(
            EXAMPLE_DATASTREAM_RECORD_WITH_HASH_ROWID, EXAMPLE_DATASTREAM_RECORD_WITH_HASH_ROWID);

    PCollection<FailsafeElement<String, String>> pCollection =
        pipeline
            .apply("CreateInput", Create.of(EXAMPLE_DATASTREAM_JSON))
            .apply(
                "FormatDatastreamJsonToJson",
                ParDo.of(
                    (FormatDatastreamJsonToJson)
                        FormatDatastreamJsonToJson.create()
                            .withStreamName("my-stream")
                            .withRenameColumnValues(renameColumns)
                            .withHashRowId(true)
                            .withLowercaseSourceColumns(false)))
            .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PAssert.that(pCollection).containsInAnyOrder(expectedElement);

    pipeline.run();
  }
}
