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
package com.google.cloud.teleport.v2.templates.oracle;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDbITBase;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.OracleResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.MultipleFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link SpannerToSourceDb} Flex template for column of size 10MB into Oracle.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToOracleSourceDbWideRow10MbIT extends SpannerToSourceDbITBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerToOracleSourceDbWideRow10MbIT.class);
  private static final String SPANNER_DDL_RESOURCE =
      "oracle/SpannerToOracleSourceDbWideRow10MbIT/oracle-google_standard_sql-spanner-schema.sql";
  private static final String SESSION_FILE_RESOURCE =
      "oracle/SpannerToOracleSourceDbWideRow10MbIT/oracle-col-mb-session.json";
  private static final String TABLE1 = "large_data";
  private static final String ORACLE_SCHEMA_FILE_RESOURCE =
      "oracle/SpannerToOracleSourceDbWideRow10MbIT/oracle-10mb-schema.sql";

  private static HashSet<SpannerToOracleSourceDbWideRow10MbIT> testInstances = new HashSet<>();
  private static PipelineLauncher.LaunchInfo jobInfo;
  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager spannerMetadataResourceManager;
  public static OracleResourceManager jdbcResourceManager;
  public static GcsResourceManager gcsResourceManager;
  private static PubsubResourceManager pubsubResourceManager;
  private SubscriptionName subscriptionName;

  @Before
  public void setUp() throws IOException {
    skipBaseCleanup = true;
    synchronized (SpannerToOracleSourceDbWideRow10MbIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager =
            createSpannerDatabase(SpannerToOracleSourceDbWideRow10MbIT.SPANNER_DDL_RESOURCE);
        spannerMetadataResourceManager = createSpannerMetadataDatabase();

        jdbcResourceManager = SharedOracleReverseITContainer.getInstance();
        testUsername = setupOracleIsolatedUser(jdbcResourceManager);
        createOracleSchema(
            jdbcResourceManager,
            SpannerToOracleSourceDbWideRow10MbIT.ORACLE_SCHEMA_FILE_RESOURCE,
            testUsername);

        gcsResourceManager =
            GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
                .build();
        createAndUploadShardConfigToGcs(gcsResourceManager, jdbcResourceManager);
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(SESSION_FILE_RESOURCE).getPath());
        pubsubResourceManager = setUpPubSubResourceManager();
        subscriptionName =
            createPubsubResources(
                getClass().getSimpleName(),
                pubsubResourceManager,
                getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""),
                gcsResourceManager);
        Map<String, String> jobParameters =
            new HashMap<>() {
              {
                put("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager));
              }
            };
        jobInfo =
            launchDataflowJob(
                gcsResourceManager,
                spannerResourceManager,
                spannerMetadataResourceManager,
                subscriptionName.toString(),
                null,
                null,
                null,
                null,
                null,
                "oracle",
                jobParameters);
      }
    }
  }

  @AfterClass
  public static void cleanUp() throws IOException {
    for (SpannerToOracleSourceDbWideRow10MbIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        spannerMetadataResourceManager,
        gcsResourceManager,
        pubsubResourceManager);
  }

  @Test
  public void spannerToOracleSourceDB10MBTest()
      throws IOException, InterruptedException, MultipleFailureException {
    assertThatPipeline(jobInfo).isRunning();
    // Write row in Spanner
    writeBasicRowInSpanner();
    // Assert events on Oracle
    assertRowInOracle();
  }

  private void writeBasicRowInSpanner() {
    LOG.info("Writing a basic row to Spanner...");

    final int maxBlobSize = 10 * 1024 * 1024; // 10MB
    final int safeBlobSize = maxBlobSize - 1024; // 9.9MB to avoid limit issues

    try {
      byte[] blobData = new byte[safeBlobSize];
      Mutation mutation =
          Mutation.newInsertBuilder("large_data")
              .set("id")
              .to(UUID.randomUUID().toString())
              .set("large_blob")
              .to(ByteArray.copyFrom(blobData)) // Ensures ≤10MB limit
              .build();

      spannerResourceManager.write(mutation);
      LOG.info("✅ Successfully inserted a 9.9MB row into Spanner.");
    } catch (Exception e) {
      LOG.error("❌ Failed to insert BLOB in Spanner: {}", e.getMessage(), e);
    }
  }

  private final List<Throwable> assertionErrors = new ArrayList<>();

  private void assertRowInOracle() throws MultipleFailureException {
    LOG.info("Validating row in Oracle...");
    final int maxBlobSize = 10 * 1024 * 1024; // 10MB
    final int safeBlobSize = maxBlobSize - 1024; // 9.9MB to avoid limit issues
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(jobInfo, Duration.ofMinutes(10)),
                () -> {
                  try {
                    return runIsolatedGetRowCount(jdbcResourceManager, testUsername, TABLE1) == 1;
                  } catch (Exception e) {
                    LOG.error("Error while getting row count from Oracle", e);
                    return false;
                  }
                });

    assertThatResult(result).meetsConditions();

    try {
      List<Map<String, Object>> rows =
          runIsolatedReadTable(jdbcResourceManager, testUsername, TABLE1);
      assertThat(rows).hasSize(1);

      Map<String, Object> row = rows.get(0);
      Object idValue = null;
      Object blobValue = null;
      for (Map.Entry<String, Object> e : row.entrySet()) {
        if ("id".equalsIgnoreCase(e.getKey())) {
          idValue = e.getValue();
        }
        if ("large_blob".equalsIgnoreCase(e.getKey())) {
          blobValue = e.getValue();
        }
      }

      assertThat(idValue).isNotNull();
      assertThat(idValue.toString()).isNotEmpty();

      assertThat(blobValue).isNotNull();
      byte[] bytes;
      if (blobValue instanceof java.sql.Blob) {
        java.sql.Blob blob = (java.sql.Blob) blobValue;
        bytes = blob.getBytes(1, (int) blob.length());
      } else {
        bytes = (byte[]) blobValue;
      }
      assertThat(bytes.length).isEqualTo(safeBlobSize);

    } catch (Exception e) {
      assertionErrors.add(new AssertionError("Oracle validation failed", e));
    }

    if (!assertionErrors.isEmpty()) {
      throw new MultipleFailureException(assertionErrors);
    }
  }
}
