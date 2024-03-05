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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch;

import com.google.auto.value.AutoValue;
import com.google.cloud.aiplatform.v1.IndexDatapoint;
import com.google.cloud.aiplatform.v1.IndexName;
import com.google.cloud.aiplatform.v1.IndexServiceClient;
import com.google.cloud.aiplatform.v1.IndexServiceSettings;
import com.google.cloud.aiplatform.v1.UpsertDatapointsRequest;
import com.google.cloud.aiplatform.v1.UpsertDatapointsResponse;
// import com.google.cloud.aiplatform.v1.IndexServiceSettings;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteChangeStreamMutationToVectorSearchFn} class is a {@link PTransform} that takes in
 * {@link PCollection} of Bigtable Change Stream Mutations. The transform converts and writes these
 * records to GCS in avro file format.
 */
@AutoValue
public abstract class WriteChangeStreamMutationToVectorSearchFn
    extends DoFn<ChangeStreamMutation, Void> {
  // extends PTransform<PCollection<ChangeStreamMutation>, PDone> {
  // @VisibleForTesting protected static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";
  /* Logger for class. */
  private static final Logger LOG =
      LoggerFactory.getLogger(WriteChangeStreamMutationToVectorSearchFn.class);
  // private static final long serialVersionUID = 825905520835363852l;

  private transient IndexServiceClient client;
  private String indexName;

  // private static final AtomicLong counter = new AtomicLong(0);

  // private static final String workerId = UUID.randomUUID().toString();

  public static WriteToVectorSearchBuilder newBuilder() {
    return new com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch
        .AutoValue_WriteChangeStreamMutationToVectorSearchFn.Builder();
  }

  // Vector Search destination settings

  public abstract String indexId();

  public abstract String projectId();

  public abstract String region();

  // CBT source settings

  public abstract String embeddingsColumn();

  public abstract int embeddingsByteSize();

  public abstract String crowdingTagColumn();

  public abstract Map<String, String> allowRestrictsMappings();

  public abstract Map<String, String> denyRestrictsMappings();

  public abstract Map<String, String> intNumericRestrictsMappings();

  public abstract Map<String, String> floatNumericRestrictsMappings();

  public abstract Map<String, String> doubleNumericRestrictsMappings();

  @Setup
  public void setup() {
    LOG.info("DOING SETUP");

    String endpoint = region() + "-aiplatform.googleapis.com:443";
    LOG.info("Connecting to endpoint {}", endpoint);
    try {
      client =
          IndexServiceClient.create(
              IndexServiceSettings.newBuilder().setEndpoint(endpoint).build());
    } catch (IOException e) {
      LOG.error("Error: ", e);
    }

    indexName = IndexName.of(projectId(), region(), indexId()).toString();
  }

  // public abstract String gcsOutputDirectory();
  //
  // public abstract String outputFilenamePrefix();
  //
  // public abstract String tempLocation();
  //
  // public abstract Integer numShards();
  //
  // public abstract BigtableSchemaFormat schemaOutputFormat();
  //
  // public abstract BigtableUtils bigtableUtils();
  //
  // @Override
  // public PDone expand(PCollection<ChangeStreamMutation> input) {
  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {

    LOG.info("Processing a mutation");

    var mutation = c.element();

    // For writes
    IndexDatapoint.Builder dp = null;

    for (Entry entry : mutation.getEntries()) {

      // For deletes
      // TODO

      if (entry instanceof SetCell) {
        if (dp == null) {
          dp = IndexDatapoint.newBuilder();
        }

        LOG.info("Mutation");
        SetCell m = (SetCell) entry;
        LOG.info("Have value {}", m.getValue());

        var family = m.getFamilyName();
        var qualifier = m.getQualifier().toStringUtf8();
        var col = family + ":" + qualifier;

        String mappedColumn;

        if (col.equals(embeddingsColumn())) {
          if (embeddingsByteSize() == 4) {
            var floats =
                embeddingsByteSize() == 4
                    ? bytesToFloats(m.getValue())
                    : bytesToFloatsFromDoubles(m.getValue());

            // TODO(meagar):Remove this
            while (floats.size() != 768) {
              floats.add(1.0f);
            }

            dp.addAllFeatureVector(floats);
          }
        } else if (col.equals(crowdingTagColumn())) {
          dp.getCrowdingTagBuilder().setCrowdingAttribute(m.getValue().toStringUtf8()).build();
        } else if ((mappedColumn = allowRestrictsMappings().get(col)) != null) {
          // TODO(meagar): - is addAllowList_Bytes_ the right thing here?
          dp.addRestrictsBuilder()
              .setNamespace(mappedColumn)
              .addAllowListBytes(m.getValue())
              .build();
        } else if ((mappedColumn = denyRestrictsMappings().get(col)) != null) {
          dp.addRestrictsBuilder()
              .setNamespace(mappedColumn)
              .addDenyListBytes(m.getValue())
              .build();
        } else if ((mappedColumn = intNumericRestrictsMappings().get(col)) != null) {
          int i = Bytes.toInt(m.getValue().toByteArray());
          dp.addNumericRestrictsBuilder().setNamespace(mappedColumn).setValueInt(i).build();
        } else if ((mappedColumn = floatNumericRestrictsMappings().get(col)) != null) {
          float f = Bytes.toFloat(m.getValue().toByteArray());
          dp.addNumericRestrictsBuilder().setNamespace(mappedColumn).setValueFloat(f).build();
        } else if ((mappedColumn = doubleNumericRestrictsMappings().get(col)) != null) {
          double d = Bytes.toDouble(m.getValue().toByteArray());
          dp.addNumericRestrictsBuilder().setNamespace(mappedColumn).setValueDouble(d).build();
        }
      } else if (entry instanceof DeleteCells) {

      } else if (entry instanceof DeleteFamily) {

      }
    } // for each entry

    // TODO: Handle these:
    // org.apache.beam.sdk.Pipeline$PipelineExecutionException:
    // com.google.api.gax.rpc.InvalidArgumentException:
    // io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Incorrect dimensionality.
    // Expected 768, got 78. Datapoint ID: 12345.

    if (dp != null) {
      ArrayList<IndexDatapoint> dps = new ArrayList<IndexDatapoint>();
      dps.add(dp.build());

      UpsertDatapointsRequest request =
          UpsertDatapointsRequest.newBuilder().setIndex(indexName).addAllDatapoints(dps).build();

      UpsertDatapointsResponse response = client.upsertDatapoints(request);
    }
  }

  private static ArrayList<Float> bytesToFloats(ByteString value) {
    var embeddings = new ArrayList<Float>();
    byte[] bytes = value.toByteArray();
    int bytes_per_float = 4;
    // TODO(meagar): Assert that bytes.length is a multiple of byte_per_float?
    for (int i = 0; i < bytes.length; i += bytes_per_float) {
      embeddings.add(Bytes.toFloat(bytes, i));
    }
    return embeddings;
  }

  // Convert a ByteString into an array of floats, but assuming the
  // ByteString contains 8 byte doubles, which are narrowed to floats
  private static ArrayList<Float> bytesToFloatsFromDoubles(ByteString value) {
    var embeddings = new ArrayList<Float>();
    byte[] bytes = value.toByteArray();
    int bytes_per_double = 8;
    for (int i = 0; i < bytes.length; i += bytes_per_double) {
      embeddings.add((float)Bytes.toDouble(bytes, i));
    }
    return embeddings;
  }

  /** Builder for {@link WriteChangeStreamMutationToVectorSearchFn}. */
  @AutoValue.Builder
  public abstract static class WriteToVectorSearchBuilder {
    abstract WriteToVectorSearchBuilder setIndexId(String indexId);

    abstract String indexId();

    abstract WriteToVectorSearchBuilder setProjectId(String projectId);

    abstract String projectId();

    abstract WriteToVectorSearchBuilder setRegion(String region);

    abstract String region();

    abstract WriteToVectorSearchBuilder setEmbeddingsColumn(String embeddingsColumn);

    abstract String embeddingsColumn();

    abstract WriteToVectorSearchBuilder setEmbeddingsByteSize(int embeddingsByteSize);

    abstract int embeddingsByteSize();

    abstract WriteToVectorSearchBuilder setCrowdingTagColumn(String crowdingTagColumn);

    abstract String crowdingTagColumn();

    abstract WriteToVectorSearchBuilder setAllowRestrictsMappings(
        Map<String, String> allowRestrictsMappings);

    abstract Map<String, String> allowRestrictsMappings();

    abstract WriteToVectorSearchBuilder setDenyRestrictsMappings(
        Map<String, String> denyRestrictsMappings);

    abstract Map<String, String> denyRestrictsMappings();

    abstract WriteToVectorSearchBuilder setIntNumericRestrictsMappings(
        Map<String, String> intNumericRestrictsMappings);

    abstract Map<String, String> intNumericRestrictsMappings();

    abstract WriteToVectorSearchBuilder setFloatNumericRestrictsMappings(
        Map<String, String> floatNumericRestrictsMappings);

    abstract Map<String, String> floatNumericRestrictsMappings();

    abstract WriteToVectorSearchBuilder setDoubleNumericRestrictsMappings(
        Map<String, String> doubleNumericRestrictsMappings);

    abstract Map<String, String> doubleNumericRestrictsMappings();

    // abstract WriteToGcsBuilder setGcsOutputDirectory(String gcsOutputDirectory);
    //
    // abstract String gcsOutputDirectory();
    //
    // abstract WriteToGcsBuilder setTempLocation(String tempLocation);
    //
    // abstract String tempLocation();
    //
    // abstract WriteToGcsBuilder setOutputFilenamePrefix(String outputFilenamePrefix);
    //
    // abstract WriteToGcsBuilder setNumShards(Integer numShards);
    //
    abstract WriteChangeStreamMutationToVectorSearchFn autoBuild();

    public WriteToVectorSearchBuilder withProjectId(String projectId) {
      return setProjectId(projectId);
    }

    public WriteToVectorSearchBuilder withRegion(String region) {
      return setRegion(region);
    }

    public WriteToVectorSearchBuilder withIndexId(String indexId) {
      return setIndexId(indexId);
    }

    WriteToVectorSearchBuilder withEmbeddingsColumn(String embeddingsColumn) {
      return this.setEmbeddingsColumn(embeddingsColumn);
    }

    WriteToVectorSearchBuilder withEmbeddingsByteSize(int embeddingsByteSize) {
      return this.setEmbeddingsByteSize(embeddingsByteSize);
    }

    WriteToVectorSearchBuilder withCrowdingTagColumn(String crowdingTagColumn) {
      return this.setCrowdingTagColumn(crowdingTagColumn);
    }

    WriteToVectorSearchBuilder withAllowRestrictsMappings(
        Map<String, String> allowRestrictsMappings) {
      return this.setAllowRestrictsMappings(allowRestrictsMappings);
    }

    WriteToVectorSearchBuilder withDenyRestrictsMappings(
        Map<String, String> denyRestrictsMappings) {
      return this.setDenyRestrictsMappings(denyRestrictsMappings);
    }

    WriteToVectorSearchBuilder withIntNumericRestrictsMappings(
        Map<String, String> intNumericRestrictsMappings) {
      return this.setIntNumericRestrictsMappings(intNumericRestrictsMappings);
    }

    WriteToVectorSearchBuilder withFloatNumericRestrictsMappings(
        Map<String, String> floatNumericRestrictsMappings) {
      return this.setFloatNumericRestrictsMappings(floatNumericRestrictsMappings);
    }

    WriteToVectorSearchBuilder withDoubleNumberRestrictsMappings(
        Map<String, String> doubleNumericRestrictsMappings) {
      return this.setDoubleNumericRestrictsMappings(doubleNumericRestrictsMappings);
    }

    public WriteChangeStreamMutationToVectorSearchFn build() {
      // checkNotNull(gcsOutputDirectory(), "Provide output directory to write to. ");
      // checkNotNull(tempLocation(), "Temporary directory needs to be provided. ");
      return autoBuild();
    }

    private static String toBase64String(ByteString bytes) {
      return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }
  }
}
