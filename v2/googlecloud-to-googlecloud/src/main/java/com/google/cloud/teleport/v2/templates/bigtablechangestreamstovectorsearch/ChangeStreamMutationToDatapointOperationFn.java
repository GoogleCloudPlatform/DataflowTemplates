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

import com.google.cloud.aiplatform.v1.IndexDatapoint;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link ChangeStreamMutationToDatapointOperationFn} class is a {@link DoFn} that takes in a
 * Bigtable ChangeStreamMutation and converts it to either an IndexDatapoint (to be added to the
 * index) or a String representing a Datapoint ID to be removed from the index.
 */
public class ChangeStreamMutationToDatapointOperationFn
    extends DoFn<ChangeStreamMutation, IndexDatapoint> {

  public static final TupleTag<IndexDatapoint> UpsertDatapointTag =
      new TupleTag<IndexDatapoint>() {};
  public static final TupleTag<String> RemoveDatapointTag = new TupleTag<String>() {};

  public ChangeStreamMutationToDatapointOperationFn(
      String embeddingsColumn,
      int embeddingsByteSize,
      String crowdingTagColumn,
      Map<String, String> allowRestrictsMappings,
      Map<String, String> denyRestrictsMappings,
      Map<String, String> intNumericRestrictsMappings,
      Map<String, String> floatNumericRestrictsMappings,
      Map<String, String> doubleNumericRestrictsMappings) {
    this.embeddingsColumn = embeddingsColumn;
    this.embeddingsByteSize = embeddingsByteSize;
    this.crowdingTagColumn = crowdingTagColumn;
    this.allowRestrictsMappings = allowRestrictsMappings;
    this.denyRestrictsMappings = denyRestrictsMappings;
    this.intNumericRestrictsMappings = intNumericRestrictsMappings;
    this.floatNumericRestrictsMappings = floatNumericRestrictsMappings;
    this.doubleNumericRestrictsMappings = doubleNumericRestrictsMappings;
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(ChangeStreamMutationToDatapointOperationFn.class);

  private String embeddingsColumn;
  private int embeddingsByteSize;
  private String crowdingTagColumn;
  private Map<String, String> allowRestrictsMappings;
  private Map<String, String> denyRestrictsMappings;
  private Map<String, String> intNumericRestrictsMappings;
  private Map<String, String> floatNumericRestrictsMappings;
  private Map<String, String> doubleNumericRestrictsMappings;

  @ProcessElement
  public void processElement(@Element ChangeStreamMutation mutation, MultiOutputReceiver output) {

    // Mutations should contain one or more setCells, *or* a DeleteCells *or* a DeleteFamily, or
    // other mods that we're not interested in. Depending on what we find, dispatch to the correct
    // handler
    for (Entry entry : mutation.getEntries()) {
      if (entry instanceof SetCell) {
        processInsert(mutation, output);
        return;
      } else if (entry instanceof DeleteCells || entry instanceof DeleteFamily) {
        processDelete(mutation, output);
        return;
      }
    }
  }

  private void processInsert(ChangeStreamMutation mutation, MultiOutputReceiver output) {
    IndexDatapoint.Builder datapointBuilder = IndexDatapoint.newBuilder();

    for (Entry entry : mutation.getEntries()) {
      LOG.debug("Processing {}", entry);

      // We're only interested in SetCell mutations; everything else should be ignored
      if (!(entry instanceof SetCell)) continue;

      SetCell m = (SetCell) entry;
      LOG.debug("Have value {}", m.getValue());

      var family = m.getFamilyName();
      var qualifier = m.getQualifier().toStringUtf8();
      var col = family + ":" + qualifier;

      String mappedColumn;

      if (col.equals(embeddingsColumn)) {
        var floats = Utils.bytesToFloats(m.getValue(), embeddingsByteSize == 8);

        // TODO(meagar):Remove this
        while (floats.size() != 768) {
          floats.add(1.0f);
        }

        datapointBuilder.addAllFeatureVector(floats);
      } else if (col.equals(crowdingTagColumn)) {
        datapointBuilder
            .getCrowdingTagBuilder()
            .setCrowdingAttribute(m.getValue().toStringUtf8())
            .build();
      } else if ((mappedColumn = allowRestrictsMappings.get(col)) != null) {
        // TODO(meagar): - is addAllowList_Bytes_ the right thing here?
        datapointBuilder
            .addRestrictsBuilder()
            .setNamespace(mappedColumn)
            .addAllowListBytes(m.getValue())
            .build();
      } else if ((mappedColumn = denyRestrictsMappings.get(col)) != null) {
        datapointBuilder
            .addRestrictsBuilder()
            .setNamespace(mappedColumn)
            .addDenyListBytes(m.getValue())
            .build();
      } else if ((mappedColumn = intNumericRestrictsMappings.get(col)) != null) {
        int i = Bytes.toInt(m.getValue().toByteArray());
        datapointBuilder
            .addNumericRestrictsBuilder()
            .setNamespace(mappedColumn)
            .setValueInt(i)
            .build();
      } else if ((mappedColumn = floatNumericRestrictsMappings.get(col)) != null) {
        float f = Bytes.toFloat(m.getValue().toByteArray());
        datapointBuilder
            .addNumericRestrictsBuilder()
            .setNamespace(mappedColumn)
            .setValueFloat(f)
            .build();
      } else if ((mappedColumn = doubleNumericRestrictsMappings.get(col)) != null) {
        double d = Bytes.toDouble(m.getValue().toByteArray());
        datapointBuilder
            .addNumericRestrictsBuilder()
            .setNamespace(mappedColumn)
            .setValueDouble(d)
            .build();
      }
    }

    LOG.debug("Emitting an upsert datapoint");
    output.get(UpsertDatapointTag).output(datapointBuilder.build());
  }

  private void processDelete(ChangeStreamMutation mutation, MultiOutputReceiver output) {
    LOG.debug("Handling mutation as a deletion");

    String removeDatapoint = null;

    for (Entry entry : mutation.getEntries()) {
      LOG.debug("Processing {}", entry);

      if (entry instanceof DeleteCells) {
        if (removeDatapoint != null) {
          LOG.error("A single change stream mutation contained DeleteCells AND DeleteFamily");
        }
        // TODO(meagar): Delete cells - verify embeddings column is removed
        LOG.info("Processing a delete cell");
        removeDatapoint = mutation.getRowKey().toStringUtf8();
      } else if (entry instanceof DeleteFamily) {
        if (removeDatapoint != null) {
          LOG.error("A single change stream mutation contained DeleteCells AND DeleteFamily");
        }

        removeDatapoint = mutation.getRowKey().toStringUtf8();
      }
    }

    if (removeDatapoint != null) {
      LOG.info("Emitting a remove datapoint: {}", removeDatapoint);
      output.get(RemoveDatapointTag).output(removeDatapoint);
    }
  }
}
