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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.TransientColumn;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils.BigQueryUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
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

/**
 * Class {@link FailsafeModJsonToChangelogTableRowTransformer} provides methods that convert a
 * {@link Mod} JSON string wrapped in {@link FailsafeElement} to a {@link TableRow}.
 */
public final class FailsafeModJsonToChangelogTableRowTransformer {

  /**
   * Primary class for taking a {@link FailsafeElement} {@link Mod} JSON input and converting to a
   * {@link TableRow}.
   */
  public static class FailsafeModJsonToTableRow
      extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

    private final BigQueryUtils bigQueryUtils;

    /** The tag for the main output of the transformation. */
    public TupleTag<TableRow> transformOut = new TupleTag<TableRow>() {};

    /** The tag for the dead letter output of the transformation. */
    public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut =
        new TupleTag<FailsafeElement<String, String>>() {};

    private final FailsafeModJsonToTableRowOptions failsafeModJsonToTableRowOptions;

    public FailsafeModJsonToTableRow(
        BigQueryUtils bigQueryUtils,
        FailsafeModJsonToTableRowOptions failsafeModJsonToTableRowOptions) {
      this.bigQueryUtils = bigQueryUtils;
      this.failsafeModJsonToTableRowOptions = failsafeModJsonToTableRowOptions;
    }

    public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {
      PCollectionTuple out =
          input.apply(
              ParDo.of(
                      new FailsafeModJsonToTableRowFn(
                          bigQueryUtils,
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

      private final ImmutableSet<String> ignoreFields;
      private final BigQueryUtils bigQueryUtils;
      public TupleTag<TableRow> transformOut;
      public TupleTag<FailsafeElement<String, String>> transformDeadLetterOut;

      public FailsafeModJsonToTableRowFn(
          BigQueryUtils bigQueryUtils,
          ImmutableSet<String> ignoreFields,
          TupleTag<TableRow> transformOut,
          TupleTag<FailsafeElement<String, String>> transformDeadLetterOut) {
        this.bigQueryUtils = bigQueryUtils;
        this.transformOut = transformOut;
        this.transformDeadLetterOut = transformDeadLetterOut;
        this.ignoreFields = ignoreFields;
      }

      @Setup
      public void setUp() {}

      @Teardown
      public void tearDown() {}

      @ProcessElement
      public void processElement(ProcessContext context) {
        FailsafeElement<String, String> failsafeModJsonString = context.element();
        if (failsafeModJsonString == null) {
          return;
        }

        try {
          TableRow tableRow = modJsonStringToTableRow(failsafeModJsonString.getPayload());
          if (tableRow == null) {
            // TableRow was not generated because pipeline configuration requires ignoring some
            // column / column families
            return;
          }

          for (String ignoreField : ignoreFields) {
            tableRow.remove(ignoreField);
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
        for (TransientColumn transientColumn : TransientColumn.values()) {
          if (modObjectNode.has(transientColumn.getColumnName())) {
            modObjectNode.remove(transientColumn.getColumnName());
          }
        }

        TableRow tableRow = new TableRow();
        if (bigQueryUtils.setTableRowFields(
            Mod.fromJson(modObjectNode.toString()), modJsonString, tableRow)) {
          return tableRow;
        } else {
          return null;
        }
      }
    }
  }

  /**
   * {@link FailsafeModJsonToTableRowOptions} provides options to initialize {@link
   * FailsafeModJsonToChangelogTableRowTransformer}.
   */
  @AutoValue
  public abstract static class FailsafeModJsonToTableRowOptions implements Serializable {
    public abstract ImmutableSet<String> getIgnoreFields();

    public abstract FailsafeElementCoder<String, String> getCoder();

    static Builder builder() {
      return new AutoValue_FailsafeModJsonToChangelogTableRowTransformer_FailsafeModJsonToTableRowOptions
          .Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setIgnoreFields(ImmutableSet<String> ignoreFields);

      abstract Builder setCoder(FailsafeElementCoder<String, String> coder);

      abstract FailsafeModJsonToTableRowOptions build();
    }
  }
}
