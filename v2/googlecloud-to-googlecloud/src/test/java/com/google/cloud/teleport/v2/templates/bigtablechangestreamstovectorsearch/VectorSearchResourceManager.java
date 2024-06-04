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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

import com.google.api.gax.longrunning.OperationTimedPollAlgorithm;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.aiplatform.v1.CreateIndexRequest;
import com.google.cloud.aiplatform.v1.DeployIndexRequest;
import com.google.cloud.aiplatform.v1.DeployedIndex;
import com.google.cloud.aiplatform.v1.Index;
import com.google.cloud.aiplatform.v1.IndexDatapoint;
import com.google.cloud.aiplatform.v1.IndexEndpoint;
import com.google.cloud.aiplatform.v1.IndexEndpointServiceClient;
import com.google.cloud.aiplatform.v1.IndexEndpointServiceSettings;
import com.google.cloud.aiplatform.v1.IndexServiceClient;
import com.google.cloud.aiplatform.v1.IndexServiceSettings;
import com.google.cloud.aiplatform.v1.MatchServiceClient;
import com.google.cloud.aiplatform.v1.MatchServiceSettings;
import com.google.cloud.aiplatform.v1.ReadIndexDatapointsRequest;
import com.google.cloud.aiplatform.v1.ReadIndexDatapointsResponse;
import com.google.cloud.aiplatform.v1.RemoveDatapointsRequest;
import com.google.cloud.aiplatform.v1.UpsertDatapointsRequest;
import com.google.protobuf.TextFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.common.ResourceManager;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * VectorSearchProvisioner looks up testing infra structure in the given project and region, and
 * creates it if it doesn't already exist. Creation can take up to 30 minutes, so infra is left
 * intact after tests run, so it can be reused in subsequent test runs.
 *
 * <p>Specifically, it creates: - A new index, named defined by TEST_INDEX_NAME, with a
 * dimensionality of 10, accessible via `getTestIndex()` - A new endpoint, name defined by
 * TEST_ENDPOINT_NAME, accessible vai `getTestEndpoint()` - A new index deployment deploying the
 * test index to the test endpoint
 */
public class VectorSearchResourceManager implements ResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(VectorSearchResourceManager.class);

  // Hard-coded; if this class winds up being used by other templates, we'll probably want to
  // add a Builder class, and allow these to be configured.
  // Note - in the cloud-teleport-testing project, a "nokill' suffix prevents the test
  // infrastructure from being automatically cleaned up.
  public static final int TEST_INDEX_DIMENSIONS = 10;
  public static final String TEST_INDEX_NAME = "bt-vs-index1-nokill";
  public static final String TEST_ENDPOINT_NAME = "bt-vs-endpoint1-nokill";
  public static final String TEST_DEPLOYED_INDEX_NAME = "bt_deploy1_nokill";

  /**
   * Factory method for creating a new resource manager, and provisioning the test index/endpoint.
   * The returned resource manager will have a ready-to-use index, endpoint and deployed endpoint.
   *
   * @param projectNumber The _numeric_ project ID (ex "269744978479")
   * @param region The region in which to find-or-create the infra (ex "us-east1")
   * @return A new VectorSearchResourceManagerr instance
   * @throws Exception
   */
  // Factory  method
  public static VectorSearchResourceManager findOrCreateTestInfra(
      String projectNumber, String region) throws Exception {
    var c = new VectorSearchResourceManager(projectNumber, region);
    c.findOrCreateTestInfra();
    return c;
  }

  public Index getTestIndex() {
    return testIndex;
  }

  public IndexEndpoint getTestEndpoint() {
    return testEndpoint;
  }

  private Index testIndex = null;
  private IndexEndpoint testEndpoint = null;
  private DeployedIndex testDeployedIndex = null;

  private String region;
  private String projectNumber;
  private String host; // ie "us-east1-aiplatform.googleapis.com:443"; (port required)
  private String parent; // ie "projects/123/locations/us-east1" (no leading/trailing slashes)

  private IndexServiceClient indexClient;
  private IndexEndpointServiceClient endpointClient;
  private MatchServiceClient matchClient;

  // Each datapoint ID we generate goes into this set; when we tear down, we send a
  // `removeDatapoints` request to the index, to prevent data living beyond the test.
  // This isn't perfect - if a test crashes and cleanup doesn't run, or the cleanup request times
  // the datapoints will be left in the index, but the datapoint IDs are sufficiently long and
  // random that this shouldn't cause collisions between test runs. It could mean that the index
  // will slowly grow over time, so we should figure out how to periodically purge the index, but
  // that could cause running instances of this test to fail, so it's not perfect.
  // Using ConcurrentHashSet instead of HashSet to support parallelized test cases
  private ConcurrentHashSet<String> pendingDatapoints = new ConcurrentHashSet<>();

  private VectorSearchResourceManager(String projectNumber, String region) throws Exception {
    this.projectNumber = projectNumber;
    this.region = region;

    this.host = String.format("%s-aiplatform.googleapis.com:443", region);
    this.parent = String.format("projects/%s/locations/%s", projectNumber, region);
  }

  // Returns a random alphanumeric string, suitable for use as a Datapoint ID or a CBT row key,
  // and records it for later deletion.
  public String makeDatapointId() {
    var id = randomAlphanumeric(20);
    pendingDatapoints.add(id);
    return id;
  }

  /**
   * Load an IndexDatapoint by its ID, returning null if the datapoint does not exist.
   *
   * @param datapointID The datapoint ID to load
   * @return An IndexDatapoint, or null
   */
  public IndexDatapoint findDatapoint(String datapointID) {
    LOG.debug("Finding datapoint {}", datapointID);

    ReadIndexDatapointsRequest request =
        ReadIndexDatapointsRequest.newBuilder()
            .setIndexEndpoint(testEndpoint.getName())
            .setDeployedIndexId(testDeployedIndex.getId())
            .addAllIds(List.of(datapointID))
            .build();

    ReadIndexDatapointsResponse response;

    try {
      response = matchClient.readIndexDatapoints(request);
    } catch (com.google.api.gax.rpc.NotFoundException e) {
      LOG.debug("Datapoint {} does not exist (NotFoundException)", datapointID);
      return null;
    }

    for (var i : response.getDatapointsList()) {
      if (i.getDatapointId().equals(datapointID)) {
        LOG.debug("Datapoint {} Exists", datapointID);
        return i;
      }
    }

    // If we reach this point, we received a response, but it doesn't have the datapoint we asked
    // for, which is probably
    // a bug?
    LOG.error("Datapoint {} not found in response - probably a bug", datapointID);

    return null;
  }

  @Override
  public void cleanupAll() {
    // Cleanup any datapoints that may have been left by failing tests
    deleteDatapoints(this.pendingDatapoints);

    this.matchClient.close();
    this.endpointClient.close();
    this.indexClient.close();
  }

  /**
   * Add a datapoint directly to the index.
   *
   * @param datapointId The datapoint ID
   * @param vector Float embeddings. The length of the array must match the dimension of the index
   */
  public void addDatapoint(String datapointId, Iterable<Float> vector) {
    LOG.debug(
        "Adding datapoint {} directly to index {} with floats",
        datapointId,
        testIndex.getName(),
        vector);
    var dp =
        IndexDatapoint.newBuilder().setDatapointId(datapointId).addAllFeatureVector(vector).build();

    LOG.debug("Doing thing");
    indexClient.upsertDatapoints(
        UpsertDatapointsRequest.newBuilder()
            .setIndex(testIndex.getName())
            .addAllDatapoints(List.of(dp))
            .build());

    LOG.debug("Done thing");
    // LOG.debug("Update Mask", dpr.hasUpdateMask());

  }

  // Cleanup a set of datapoints. Datapoint IDs that don't exist in the index are ignored, so it's
  // safe to run this to remove a set of datapoints larger than the set of those that exist.
  private void deleteDatapoints(Iterable<String> datapointIds) {
    var request =
        RemoveDatapointsRequest.newBuilder()
            .addAllDatapointIds(datapointIds)
            .setIndex(testIndex.getName())
            .build();

    this.indexClient.removeDatapoints(request);
  }

  private void findOrCreateTestInfra() throws Exception {
    // Used to poll long-running operations; it can take up to 30 minutes to create an index and
    // endpoint and then
    // deploy the index to the endpoint. Most of the time is taken on the deploy step.
    var poll =
        OperationTimedPollAlgorithm.create(
            RetrySettings.newBuilder()
                .setInitialRetryDelay(Duration.ofMinutes(1))
                .setRetryDelayMultiplier(1.0)
                .setMaxRetryDelay(Duration.ofMinutes(1))
                .setInitialRpcTimeout(Duration.ZERO)
                .setRpcTimeoutMultiplier(1.0)
                .setMaxRpcTimeout(Duration.ZERO)
                .setTotalTimeout(Duration.ofMinutes(120))
                .build());

    var indexSettings = IndexServiceSettings.newBuilder().setEndpoint(host);
    indexSettings.createIndexOperationSettings().setPollingAlgorithm(poll);
    indexClient = IndexServiceClient.create(indexSettings.build());

    var endpointSettings = IndexEndpointServiceSettings.newBuilder().setEndpoint(host);
    endpointSettings.deployIndexOperationSettings().setPollingAlgorithm(poll);
    endpointClient = IndexEndpointServiceClient.create(endpointSettings.build());

    // All three (index, endpoint, deployment) seem to have to exist before the public domain on the
    // endpoint is
    // necessarily created? Sometimes it is empty if we try to retrieve it too quickly.
    testIndex = findOrCreateIndex();
    testEndpoint = findOrCreateEndpoint();
    testDeployedIndex = findOrCreateDeployedIndex();

    LOG.debug(
        "Creating match client with endpoint {}",
        testEndpoint.getPublicEndpointDomainName() + ":443");

    var matchSettings =
        MatchServiceSettings.newBuilder()
            .setEndpoint(testEndpoint.getPublicEndpointDomainName() + ":443");

    matchClient = MatchServiceClient.create(matchSettings.build());
  }

  private Index findOrCreateIndex() throws Exception {
    LOG.debug("Doing find-or-create for test index {}", TEST_INDEX_NAME);
    for (var i : indexClient.listIndexes(parent).iterateAll()) {
      if (i.getDisplayName().equals(TEST_INDEX_NAME)) {
        LOG.debug("Using existing index: {}:{}", i.getDisplayName(), i.getName());
        return i;
      } else {
        LOG.debug("Ignoring index: {}:{}", i.getDisplayName(), i.getName());
      }
    }

    LOG.debug("Index {} does not exist, creating", TEST_INDEX_NAME);
    return createIndex(TEST_INDEX_NAME);
  }

  private IndexEndpoint findOrCreateEndpoint() throws Exception {
    LOG.debug("Doing find-or-create for test endpoint {}", TEST_ENDPOINT_NAME);

    for (var e : endpointClient.listIndexEndpoints(parent).iterateAll()) {
      if (e.getDisplayName().equals(TEST_ENDPOINT_NAME)) {
        LOG.debug("Using existing endpoint: {}:{}", e.getDisplayName(), e.getName());
        return e;
      } else {
        LOG.debug("Ignoring endpoint {}:{}", e.getDisplayName(), e.getName());
      }
    }

    LOG.debug("Endpoint {} does not exist, creating", TEST_ENDPOINT_NAME);
    return createEndpoint(TEST_ENDPOINT_NAME);
  }

  private DeployedIndex findOrCreateDeployedIndex() throws Exception {
    LOG.debug("Doing find-or-create for test index deployment {}", TEST_DEPLOYED_INDEX_NAME);

    for (var d : testEndpoint.getDeployedIndexesList()) {
      if (d.getId().equals(TEST_DEPLOYED_INDEX_NAME)) {
        LOG.debug("Using existing deployment: {}:{}", d.getDisplayName(), d.getId());
        return d;
      } else {
        LOG.debug("Ignoring deployment {}:{}", d.getDisplayName(), d.getId());
      }
    }

    LOG.debug("DeployedIndex {} does not exist, creating", TEST_DEPLOYED_INDEX_NAME);
    return deployIndexToEndpoint(testIndex.getName(), testEndpoint.getName());
  }

  private Index createIndex(String indexName) throws Exception {
    // This is somewhat of a black box, copied from an index in a good state.
    // The resulting index will have a dimensionality of 10
    final CharSequence indexSchema =
        "struct_value { fields { key: \"config\" value { struct_value { fields { key: \"algorithmConfig\" value { struct_value { fields { key: \"treeAhConfig\" value { struct_value { fields { key: \"fractionLeafNodesToSearch\" value { number_value: 0.05 } } fields { key: \"leafNodeEmbeddingCount\" value { string_value: \"1000\" } } } } } } } } fields { key: \"approximateNeighborsCount\" value { number_value: 1.0 } } fields { key: \"dimensions\" value { number_value: 10.0 } } fields { key: \"distanceMeasureType\" value { string_value: \"DOT_PRODUCT_DISTANCE\" } } fields { key: \"featureNormType\" value { string_value: \"NONE\" } } fields { key: \"shardSize\" value { string_value: \"SHARD_SIZE_SMALL\" } } } } } }";

    var v = com.google.protobuf.Value.newBuilder();
    TextFormat.merge(indexSchema, v);

    var index =
        Index.newBuilder()
            .setIndexUpdateMethod(Index.IndexUpdateMethod.STREAM_UPDATE)
            .setDisplayName(indexName)
            .setDescription(
                "Used in integration tests by the Bigtable Change Streams to Vector Search template")
            .setMetadataSchemaUri(
                "gs://google-cloud-aiplatform/schema/matchingengine/metadata/nearest_neighbor_search_1.0.0.yaml")
            .setMetadata(v)
            .build();

    var request = CreateIndexRequest.newBuilder().setParent(parent).setIndex(index).build();

    return indexClient.createIndexAsync(request).get(30, TimeUnit.MINUTES);
  }

  private IndexEndpoint createEndpoint(String endpointName) throws Exception {
    var endpoint =
        IndexEndpoint.newBuilder()
            .setDisplayName(endpointName)
            .setDescription(
                "Test endpoint for Bigtable Change Streams to Vector Search Dataflow Template")
            .build();

    return endpointClient.createIndexEndpointAsync(parent, endpoint).get(30, TimeUnit.MINUTES);
  }

  private DeployedIndex deployIndexToEndpoint(String indexID, String endpointID) throws Exception {
    DeployIndexRequest request =
        DeployIndexRequest.newBuilder()
            .setIndexEndpoint(endpointID)
            .setDeployedIndex(
                DeployedIndex.newBuilder()
                    .setIndex(indexID)
                    .setDisplayName("Integration tests")
                    .setId(TEST_DEPLOYED_INDEX_NAME)
                    // manually delete them?
                    .build())
            .build();

    return endpointClient.deployIndexAsync(request).get().getDeployedIndex();
  }
}
