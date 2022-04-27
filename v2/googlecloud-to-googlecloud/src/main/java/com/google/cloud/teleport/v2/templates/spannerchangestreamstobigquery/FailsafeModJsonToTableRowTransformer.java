/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Key.Builder;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerTable;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.BigQueryUtils;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SpannerToBigQueryUtils;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SpannerUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.json.JSONObject;

/**
 * Class {@link FailsafeModJsonToTableRowTransformer} provides methods that convert a {@link Mod}
 * JSON string wrapped in {@link FailsafeElement} to a {@link TableRow}.
 */
public final class FailsafeModJsonToTableRowTransformer {

  /**
   * Primary class for taking a {@link FailsafeElement} {@link Mod} JSON input and converting to a
   * {@link TableRow}.
   */
  public static class FailsafeModJsonToTableRow
      extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

    /** The tag for the main output of the transformation. */
    public TupleTag<TableRow> transformOut = new TupleTag<TableRow>() {};

    /** The tag for the dead letter output of the transformation. */
    public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut =
        new TupleTag<FailsafeElement<String, String>>() {};

    private final FailsafeModJsonToTableRowOptions failsafeModJsonToTableRowOptions;

    public FailsafeModJsonToTableRow(
        FailsafeModJsonToTableRowOptions failsafeModJsonToTableRowOptions) {
      this.failsafeModJsonToTableRowOptions = failsafeModJsonToTableRowOptions;
    }

    public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {
      PCollectionTuple out =
          input.apply(
              ParDo.of(
                      new FailsafeModJsonToTableRowFn(
                          failsafeModJsonToTableRowOptions.getSpannerConfig(),
                          failsafeModJsonToTableRowOptions.getSpannerChangeStream(),
                          failsafeModJsonToTableRowOptions.getIgnoreFields(),
                          transformOut,
                          transformDeadLetterOut))
                  .withOutputTags(transformOut, TupleTagList.of(transformDeadLetterOut)));
      out.get(transformDeadLetterOut).setCoder(failsafeModJsonToTableRowOptions.getCoder());
      return out;
    }

    /**
     * The {@link FailsafeModJsonToTableRowFn} converts a {@link Mod} JSON string wrapped in {@link
     * FailsafeElement} to a {@link TableRow}.
     */
    public static class FailsafeModJsonToTableRowFn
        extends DoFn<FailsafeElement<String, String>, TableRow> {

      private transient SpannerAccessor spannerAccessor;
      private final SpannerConfig spannerConfig;
      private final String spannerChangeStream;
      private Map<String, TrackedSpannerTable> spannerTableByName;
      private final Set<String> ignoreFields;
      public TupleTag<TableRow> transformOut;
      public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut;

      public FailsafeModJsonToTableRowFn(
          SpannerConfig spannerConfig,
          String spannerChangeStream,
          String ignoreFieldsStr,
          TupleTag<TableRow> transformOut,
          TupleTag<FailsafeElement<String, String>> transformDeadLetterOut) {
        this.spannerConfig = spannerConfig;
        this.spannerChangeStream = spannerChangeStream;
        this.transformOut = transformOut;
        this.transformDeadLetterOut = transformDeadLetterOut;
        this.ignoreFields = new HashSet<>();
        for (String ignoreField : ignoreFieldsStr.split(",")) {
          ignoreFields.add(ignoreField);
        }
      }

      @Setup
      public void setUp() {
        spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
        spannerTableByName =
            new SpannerUtils(spannerAccessor.getDatabaseClient(), spannerChangeStream)
                .getSpannerTableByName();
      }

      @Teardown
      public void tearDown() {
        spannerAccessor.close();
      }

      @ProcessElement
      public void processElement(ProcessContext context) {
        FailsafeElement<String, String> failsafeModJsonString = context.element();

        try {
          TableRow tableRow = modJsonStringToTableRow(failsafeModJsonString.getPayload());
          for (String ignoreField : ignoreFields) {
            if (tableRow.containsKey(ignoreField)) {
              tableRow.remove(ignoreField);
            }
          }
          context.output(tableRow);
        } catch (Exception e) {
          context.output(
              transformDeadLetterOut,
              FailsafeElement.of(failsafeModJsonString)
                  .setErrorMessage(e.getMessage())
                  .setStacktrace(Throwables.getStackTraceAsString(e)));
        }
      }

      private TableRow modJsonStringToTableRow(String modJsonString) throws Exception {
        ObjectNode modObjectNode = (ObjectNode) new ObjectMapper().readTree(modJsonString);
        for (String excludeFieldName : BigQueryUtils.getBigQueryIntermediateMetadataFieldNames()) {
          if (modObjectNode.has(excludeFieldName)) {
            modObjectNode.remove(excludeFieldName);
          }
        }

        Mod mod = Mod.fromJson(modObjectNode.toString());
        String spannerTableName = mod.getTableName();
        TrackedSpannerTable spannerTable = spannerTableByName.get(spannerTableName);
        com.google.cloud.Timestamp spannerCommitTimestamp =
            com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                mod.getCommitTimestampSeconds(), mod.getCommitTimestampNanos());

        // Set metadata fields of the tableRow.
        TableRow tableRow = new TableRow();
        BigQueryUtils.setMetadataFiledsOfTableRow(
            spannerTableName, mod, modJsonString, spannerCommitTimestamp, tableRow);
        JSONObject keysJsonObject = new JSONObject(mod.getKeysJson());
        // Set Spanner key columns of of the tableRow.
        for (TrackedSpannerColumn spannerColumn : spannerTable.getPkColumns()) {
          String spannerColumnName = spannerColumn.getName();
          if (keysJsonObject.has(spannerColumnName)) {
            tableRow.set(spannerColumnName, keysJsonObject.get(spannerColumnName));
          } else {
            throw new IllegalArgumentException(
                "Cannot find value for key column " + spannerColumnName);
          }
        }

        // For "DELETE" mod, we only need to set the key columns.
        if (mod.getModType() == ModType.DELETE) {
          return tableRow;
        }

        // Set non-key columns of the tableRow.
        SpannerToBigQueryUtils.addSpannerNonPkColumnsToTableRow(
            mod.getNewValuesJson(), spannerTable.getNonPkColumns(), tableRow);

        // For "INSERT" mod, we can get all columns from mod.
        if (mod.getModType() == ModType.INSERT) {
          return tableRow;
        }

        // For "UPDATE" mod, the Mod only contains the changed columns, unchanged tracked columns
        // are not included, so we need to do a snapshot read to Spanner to get the full row image
        // tracked by change stream, we want to re-read the updated columns as well to get a
        // consistent view of the whole row after the transaction is committed.
        // Note that the read can fail if the database version retention period (default to be one
        // hour) has passed the snapshot read timestamp, similar to other error cases, the pipeline
        // will put the failed mod into the retry deadletter queue, and retry it for 5 times, and
        // then eventually add the failed mod into the severe deadletter queue which won't be
        // processed by the pipeline again, users should process the severe deadletter queue
        // themselves.
        Builder keyBuilder = com.google.cloud.spanner.Key.newBuilder();
        for (TrackedSpannerColumn spannerColumn : spannerTable.getPkColumns()) {
          String spannerColumnName = spannerColumn.getName();
          if (keysJsonObject.has(spannerColumnName)) {
            SpannerUtils.appendToSpannerKey(spannerColumn, keysJsonObject, keyBuilder);
          } else {
            throw new IllegalArgumentException(
                "Cannot find value for key column " + spannerColumnName);
          }
        }

        List<TrackedSpannerColumn> spannerNonPkColumns = spannerTable.getNonPkColumns();
        List<String> spannerNonPkColumnNames =
            spannerNonPkColumns.stream()
                .map(spannerNonPkColumn -> spannerNonPkColumn.getName())
                .collect(Collectors.toList());

        Options.ReadQueryUpdateTransactionOption options =
            Options.priority(spannerConfig.getRpcPriority().get());
        // We assume the Spanner schema isn't changed while the pipeline is running, so the read is
        // expected to succeed in normal cases. The schema change is currently not supported.
        try (ResultSet resultSet =
            spannerAccessor
                .getDatabaseClient()
                .singleUseReadOnlyTransaction(
                    TimestampBound.ofReadTimestamp(spannerCommitTimestamp))
                .read(
                    spannerTable.getTableName(),
                    KeySet.singleKey(keyBuilder.build()),
                    spannerNonPkColumnNames,
                    options)) {
          SpannerToBigQueryUtils.spannerSnapshotRowToBigQueryTableRow(
              resultSet, spannerNonPkColumns, tableRow);
        }

        return tableRow;
      }
    }
  }

  /**
   * {@link FailsafeModJsonToTableRowOptions} provides options to initialize {@link
   * FailsafeModJsonToTableRowTransformer}.
   */
  @AutoValue
  public abstract static class FailsafeModJsonToTableRowOptions implements Serializable {
    public abstract SpannerConfig getSpannerConfig();

    public abstract String getSpannerChangeStream();

    public abstract String getIgnoreFields();

    public abstract FailsafeElementCoder<String, String> getCoder();

    static Builder builder() {
      return new AutoValue_FailsafeModJsonToTableRowTransformer_FailsafeModJsonToTableRowOptions
          .Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setSpannerConfig(SpannerConfig spannerSpannerConfig);

      abstract Builder setSpannerChangeStream(String spannerChangeStream);

      abstract Builder setIgnoreFields(String ignoreFields);

      abstract Builder setCoder(FailsafeElementCoder<String, String> coder);

      abstract FailsafeModJsonToTableRowOptions build();
    }
  }
}
