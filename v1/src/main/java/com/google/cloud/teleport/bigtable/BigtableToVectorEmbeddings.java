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
package com.google.cloud.teleport.bigtable;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.teleport.bigtable.BigtableToVectorEmbeddings.Options;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow pipeline that exports data from a Cloud Bigtable table to JSON files in GCS,
 * specifically for Vector Embedding purposes. Currently, filtering on Cloud Bigtable table is not
 * supported.
 *
 * <p>Check out <a href=
 * "https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v1/README_Cloud_Bigtable_to_Vector_Embeddings.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Cloud_Bigtable_to_Vector_Embeddings",
    category = TemplateCategory.BATCH,
    displayName = "Cloud Bigtable to Vector Embeddings",
    description =
        "The Bigtable to Vector Embedding template is a pipeline that reads data from a Bigtable table and writes it to a Cloud Storage bucket in JSON format, for vector embeddings",
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bigtable-to-vector-embeddings",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The Bigtable table must exist.",
      "The output Cloud Storage bucket must exist before running the pipeline."
    })
public class BigtableToVectorEmbeddings {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableToVectorEmbeddings.class);

  /** Options for the export pipeline. */
  public interface Options extends PipelineOptions {
    @TemplateParameter.ProjectId(
        order = 1,
        description = "Project ID",
        helpText =
            "The ID of the Google Cloud project of the Cloud Bigtable instance that you want to"
                + " read data from")
    ValueProvider<String> getBigtableProjectId();

    @SuppressWarnings("unused")
    void setBigtableProjectId(ValueProvider<String> projectId);

    @TemplateParameter.Text(
        order = 2,
        regexes = {"[a-z][a-z0-9\\-]+[a-z0-9]"},
        description = "Instance ID",
        helpText = "The ID of the Cloud Bigtable instance that contains the table")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @TemplateParameter.Text(
        order = 3,
        regexes = {"[_a-zA-Z0-9][-_.a-zA-Z0-9]*"},
        description = "Table ID",
        helpText = "The ID of the Cloud Bigtable table to read")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);

    @TemplateParameter.GcsWriteFolder(
        order = 4,
        optional = true,
        description = "Cloud Storage directory for storing JSON files",
        helpText = "The Cloud Storage path where the output JSON files can be stored.",
        example = "gs://your-bucket/your-path/")
    ValueProvider<String> getOutputDirectory();

    @SuppressWarnings("unused")
    void setOutputDirectory(ValueProvider<String> outputDirectory);

    @TemplateParameter.Text(
        order = 5,
        description = "JSON file prefix",
        helpText = "The prefix of the JSON file name. For example, \"table1-\"")
    @Default.String("part")
    ValueProvider<String> getFilenamePrefix();

    @SuppressWarnings("unused")
    void setFilenamePrefix(ValueProvider<String> filenamePrefix);

    @TemplateParameter.Text(
        order = 6,
        description = "ID column",
        helpText =
            "The fully qualified column name where the ID is stored. In the format cf:col or row_key.")
    ValueProvider<String> getIdColumn();

    @SuppressWarnings("unused")
    void setIdColumn(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 7,
        description = "Embedding column",
        helpText =
            "The fully qualified column name where the embeddings are stored. In the format cf:col or row_key.")
    ValueProvider<String> getEmbeddingColumn();

    @SuppressWarnings("unused")
    void setEmbeddingColumn(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "Crowding tag column",
        helpText =
            "The fully qualified column name where the crowding tag is stored. In the format cf:col or row_key.")
    ValueProvider<String> getCrowdingTagColumn();

    @SuppressWarnings("unused")
    void setCrowdingTagColumn(ValueProvider<String> value);

    @TemplateParameter.Integer(
        order = 9,
        optional = true,
        description = "The byte size of the embeddings array. Can be 4 or 8.",
        helpText = "The byte size of each entry in the embeddings array. Use 4 for Float, and 8 for Double.")
    @Default.Integer(4)
    ValueProvider<Integer> getEmbeddingByteSize();

    @SuppressWarnings("unused")
    void setEmbeddingByteSize(ValueProvider<Integer> value);

    @TemplateParameter.Text(
        order = 10,
        optional = true,
        description = "Allow restricts mappings",
        helpText =
            "The comma separated fully qualified column names of the columns that should be used as the `allow` restricts, with their alias. In the format cf:col;alias.")
    ValueProvider<String> getAllowRestrictsMappings();

    @SuppressWarnings("unused")
    void setAllowRestrictsMappings(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 11,
        optional = true,
        description = "Deny restricts mappings",
        helpText =
            "The comma separated fully qualified column names of the columns that should be used as the `deny` restricts, with their alias. In the format cf:col;alias.")
    ValueProvider<String> getDenyRestrictsMappings();

    @SuppressWarnings("unused")
    void setDenyRestrictsMappings(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 12,
        optional = true,
        description = "Integer numeric restricts mappings",
        helpText =
            "The comma separated fully qualified column names of the columns that should be used as integer `numeric_restricts`, with their alias. In the format cf:col;alias.")
    ValueProvider<String> getIntNumericRestrictsMappings();

    @SuppressWarnings("unused")
    void setIntNumericRestrictsMappings(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 13,
        optional = true,
        description = "Float numeric restricts mappings",
        helpText =
            "The comma separated fully qualified column names of the columns that should be used as float (4 bytes) `numeric_restricts`, with their alias. In the format cf:col;alias.")
    ValueProvider<String> getFloatNumericRestrictsMappings();

    @SuppressWarnings("unused")
    void setFloatNumericRestrictsMappings(ValueProvider<String> value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Double numeric restricts mappings",
        helpText =
            "The comma separated fully qualified column names of the columns that should be used as double (8 bytes) `numeric_restricts`, with their alias. In the format cf:col;alias.")
    ValueProvider<String> getDoubleNumericRestrictsMappings();

    @SuppressWarnings("unused")
    void setDoubleNumericRestrictsMappings(ValueProvider<String> value);
  }

  /**
   * Runs a pipeline to export data from a Cloud Bigtable table to JSON files in GCS in JSON format,
   * for use of Vertex Vector Search.
   *
   * @param args arguments to the pipeline
   */
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    PipelineResult result = run(options);

    // Wait for pipeline to finish only if it is not constructing a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() == null) {
      result.waitUntilFinish();
    }
    LOG.info("Completed pipeline setup");
  }

  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(PipelineUtils.tweakPipelineOptions(options));

    BigtableIO.Read read =
        BigtableIO.read()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .withRowFilter(RowFilter.newBuilder().setCellsPerColumnLimitFilter(1).build());

    // Do not validate input fields if it is running as a template.
    if (options.as(DataflowPipelineOptions.class).getTemplateLocation() != null) {
      read = read.withoutValidation();
    }

    // Concatenating cloud storage folder with file prefix to get complete path
    ValueProvider<String> outputFilePrefix = options.getFilenamePrefix();

    ValueProvider<String> outputFilePathWithPrefix =
        ValueProvider.NestedValueProvider.of(
            options.getOutputDirectory(),
            (SerializableFunction<String, String>)
                folder -> {
                  if (!folder.endsWith("/")) {
                    // Appending the slash if not provided by user
                    folder = folder + "/";
                  }
                  return folder + outputFilePrefix.get();
                });
    pipeline
        .apply("Read from Bigtable", read)
        .apply(
            "Transform to JSON",
            MapElements.via(
                new BigtableToVectorEmbeddingsFn(
                    options.getIdColumn(),
                    options.getEmbeddingColumn(),
                    options.getEmbeddingByteSize(),
                    options.getCrowdingTagColumn(),
                    options.getAllowRestrictsMappings(),
                    options.getDenyRestrictsMappings(),
                    options.getIntNumericRestrictsMappings(),
                    options.getFloatNumericRestrictsMappings(),
                    options.getDoubleNumericRestrictsMappings())))
        .apply("Write to storage", TextIO.write().to(outputFilePathWithPrefix).withSuffix(".json"));

    return pipeline.run();
  }

  /** Translates Bigtable {@link Row} to Vector Embeddings JSON. */
  static class BigtableToVectorEmbeddingsFn extends SimpleFunction<Row, String> {
    private static final String ID_KEY = "id";
    private static final String EMBEDDING_KEY = "embedding";
    private static final String RESTRICTS_KEY = "restricts";
    private static final String NUMERIC_RESTRICTS_KEY = "numeric_restricts";
    private static final String CROWDING_TAG_KEY = "crowding_tag";

    private static final String NAMESPACE_KEY = "namespace";

    private static final String ALLOW_KEY = "allow";
    private static final String DENY_KEY = "deny";

    private static final String VALUE_INT_KEY = "value_int";
    private static final String VALUE_FLOAT_KEY = "value_float";
    private static final String VALUE_DOUBLE_KEY = "value_double";

    private String idColumn;
    private String embeddingsColumn;
    private Integer embeddingByteSize;
    private String crowdingTagColumn;
    private Map<String, String> allowRestricts;
    private Map<String, String> denyRestricts;
    private Map<String, String> intNumericRestricts;
    private Map<String, String> floatNumericRestricts;
    private Map<String, String> doubleNumericRestricts;

    private ValueProvider<Integer> embeddingByteSizeProvider;
    private ValueProvider<String> idColumnProvider;
    private ValueProvider<String> embeddingsColumnProvider;
    private ValueProvider<String> crowdingTagColumnProvider;
    private ValueProvider<String> allowRestrictsMappingsProvider;
    private ValueProvider<String> denyRestrictsMappingsProvider;
    private ValueProvider<String> intNumericRestrictsMappingsProvider;
    private ValueProvider<String> floatNumericRestrictsMappingsProvider;
    private ValueProvider<String> doubleNumericRestrictsMappingsProvider;

    public BigtableToVectorEmbeddingsFn(
        ValueProvider<String> idColumnProvider,
        ValueProvider<String> embeddingsColumnProvider,
        ValueProvider<Integer> embeddingByteSizeProvider,
        ValueProvider<String> crowdingTagColumnProvider,
        ValueProvider<String> allowRestrictsMappingsProvider,
        ValueProvider<String> denyRestrictsMappingsProvider,
        ValueProvider<String> intNumericRestrictsMappingsProvider,
        ValueProvider<String> floatNumericRestrictsMappingsProvider,
        ValueProvider<String> doubleNumericRestrictsMappingsProvider) {
      this.idColumnProvider = idColumnProvider;
      this.embeddingsColumnProvider = embeddingsColumnProvider;
      this.embeddingByteSizeProvider = embeddingByteSizeProvider;
      this.crowdingTagColumnProvider = crowdingTagColumnProvider;
      this.allowRestrictsMappingsProvider = allowRestrictsMappingsProvider;
      this.denyRestrictsMappingsProvider = denyRestrictsMappingsProvider;
      this.intNumericRestrictsMappingsProvider = intNumericRestrictsMappingsProvider;
      this.floatNumericRestrictsMappingsProvider = floatNumericRestrictsMappingsProvider;
      this.doubleNumericRestrictsMappingsProvider = doubleNumericRestrictsMappingsProvider;
    }

    @Override
    public String apply(Row row) {
      this.embeddingByteSize = this.embeddingByteSizeProvider.get();
      if (this.embeddingByteSize != 4 && this.embeddingByteSize != 8) {
        throw new RuntimeException("embeddingByteSize can be either 4 or 8");
      }
      this.idColumn = this.idColumnProvider.get();
      this.embeddingsColumn = this.embeddingsColumnProvider.get();
      this.crowdingTagColumn = this.crowdingTagColumnProvider.get();
      this.allowRestricts =
          Optional.ofNullable(this.allowRestricts)
              .orElse(extractColumnsAliases(this.allowRestrictsMappingsProvider));
      this.denyRestricts =
          Optional.ofNullable(this.denyRestricts)
              .orElse(extractColumnsAliases(this.denyRestrictsMappingsProvider));
      this.intNumericRestricts =
          Optional.ofNullable(this.intNumericRestricts)
              .orElse(extractColumnsAliases(this.intNumericRestrictsMappingsProvider));
      this.floatNumericRestricts =
          Optional.ofNullable(this.floatNumericRestricts)
              .orElse(extractColumnsAliases(this.floatNumericRestrictsMappingsProvider));
      this.doubleNumericRestricts =
          Optional.ofNullable(this.doubleNumericRestricts)
              .orElse(extractColumnsAliases(this.doubleNumericRestrictsMappingsProvider));

      StringWriter stringWriter = new StringWriter();
      JsonWriter jsonWriter = new JsonWriter(stringWriter);
      VectorEmbeddings vectorEmbeddings = buildObject(row);
      try {
        serialize(jsonWriter, vectorEmbeddings);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return stringWriter.toString();
    }

    private void serialize(JsonWriter jsonWriter, VectorEmbeddings vectorEmbeddings)
        throws IOException {
      jsonWriter.beginObject();

      // Required fields.
      jsonWriter.name(ID_KEY).value(vectorEmbeddings.id);
      jsonWriter.name(EMBEDDING_KEY);
      jsonWriter.beginArray();
      if (this.embeddingByteSize == 4) {
        for (Float f : vectorEmbeddings.floatEmbeddings) {
          jsonWriter.value(f);
        }
      } else if (this.embeddingByteSize == 8) {
        for (Double d : vectorEmbeddings.doubleEmbeddings) {
          jsonWriter.value(d);
        }
      }
      jsonWriter.endArray();

      // Optional fields.
      if (vectorEmbeddings.crowdingTag != "") {
        jsonWriter.name(CROWDING_TAG_KEY).value(vectorEmbeddings.crowdingTag);
      }
      if (vectorEmbeddings.restricts != null && !vectorEmbeddings.restricts.isEmpty()) {
        jsonWriter.name(RESTRICTS_KEY);
        jsonWriter.beginArray();
        for (Restrict r : vectorEmbeddings.restricts) {
          jsonWriter.beginObject();
          jsonWriter.name(NAMESPACE_KEY).value(r.namespace);
          if (r.allow != null && !r.allow.isEmpty()) {
            jsonWriter.name(ALLOW_KEY);
            jsonWriter.beginArray();
            for (String a : r.allow) {
              jsonWriter.value(a);
            }
            jsonWriter.endArray();
          } else if (r.deny != null && !r.deny.isEmpty()) {
            jsonWriter.name(DENY_KEY);
            jsonWriter.beginArray();
            for (String d : r.deny) {
              jsonWriter.value(d);
            }
            jsonWriter.endArray();
          }
          jsonWriter.endObject();
        }
        jsonWriter.endArray();
      }
      if (vectorEmbeddings.numericRestricts != null
          && !vectorEmbeddings.numericRestricts.isEmpty()) {
        jsonWriter.name(NUMERIC_RESTRICTS_KEY);
        jsonWriter.beginArray();
        for (NumericRestrict numericRestrict : vectorEmbeddings.numericRestricts) {
          jsonWriter.beginObject();
          jsonWriter.name(NAMESPACE_KEY).value(numericRestrict.namespace);
          switch (numericRestrict.type) {
            case INT:
              jsonWriter.name(VALUE_INT_KEY).value(numericRestrict.valueInt);
              break;
            case FLOAT:
              jsonWriter.name(VALUE_FLOAT_KEY).value(numericRestrict.valueFloat);
              break;
            case DOUBLE:
              jsonWriter.name(VALUE_DOUBLE_KEY).value(numericRestrict.valueDouble);
              break;
          }
          jsonWriter.endObject();
        }
        jsonWriter.endArray();
      }
      jsonWriter.endObject();
    }

    private VectorEmbeddings buildObject(Row row) {
      VectorEmbeddings vectorEmbeddings = new VectorEmbeddings();

      maybeAddToObject(vectorEmbeddings, "row_key", row.getKey());
      for (Family family : row.getFamiliesList()) {
        String familyName = family.getName();
        for (Column column : family.getColumnsList()) {
          for (Cell cell : column.getCellsList()) {
            maybeAddToObject(
                vectorEmbeddings,
                familyName + ":" + column.getQualifier().toStringUtf8(),
                cell.getValue());
          }
        }
      }

      // Assert fields
      if (StringUtils.isEmpty(vectorEmbeddings.id)) {
        throw new RuntimeException(
            String.format(
                "'%s' value is missing for row '%s'", ID_KEY, row.getKey().toStringUtf8()));
      }
      if (this.embeddingByteSize == 4
          && (vectorEmbeddings.floatEmbeddings == null
              || vectorEmbeddings.floatEmbeddings.isEmpty())) {
        throw new RuntimeException(
            String.format(
                "'%s' value is missing for row '%s'", EMBEDDING_KEY, row.getKey().toStringUtf8()));
      }
      if (this.embeddingByteSize == 8
          && (vectorEmbeddings.doubleEmbeddings == null
              || vectorEmbeddings.doubleEmbeddings.isEmpty())) {
        throw new RuntimeException(
            String.format(
                "'%s' value is missing for row '%s'", EMBEDDING_KEY, row.getKey().toStringUtf8()));
      }
      return vectorEmbeddings;
    }

    private void maybeAddToObject(
        VectorEmbeddings vectorEmbeddings, String columnQualifier, ByteString value) {
      if (columnQualifier.equals(this.idColumn)) {
        vectorEmbeddings.id = value.toStringUtf8();
      } else if (columnQualifier.equals(this.crowdingTagColumn)) {
        vectorEmbeddings.crowdingTag = value.toStringUtf8();
      } else if (columnQualifier.equals(this.embeddingsColumn)) {
        vectorEmbeddings.floatEmbeddings = new ArrayList<Float>();
        vectorEmbeddings.doubleEmbeddings = new ArrayList<Double>();

        byte[] bytes = value.toByteArray();
        for (int i = 0; i < bytes.length; i += embeddingByteSize) {
          if (embeddingByteSize == 4) {
            vectorEmbeddings.floatEmbeddings.add(Bytes.toFloat(bytes, i));
          } else if (embeddingByteSize == 8) {
            vectorEmbeddings.doubleEmbeddings.add(Bytes.toDouble(bytes, i));
          }
        }
      } else if (this.allowRestricts.containsKey(columnQualifier)) {
        vectorEmbeddings.addRestrict(
            Restrict.allowRestrict(allowRestricts.get(columnQualifier), value));
      } else if (this.denyRestricts.containsKey(columnQualifier)) {
        vectorEmbeddings.addRestrict(
            Restrict.denyRestrict(denyRestricts.get(columnQualifier), value));
      } else if (this.intNumericRestricts.containsKey(columnQualifier)) {
        vectorEmbeddings.addNumericRestrict(
            NumericRestrict.intValue(intNumericRestricts.get(columnQualifier), value));
      } else if (this.floatNumericRestricts.containsKey(columnQualifier)) {
        vectorEmbeddings.addNumericRestrict(
            NumericRestrict.floatValue(floatNumericRestricts.get(columnQualifier), value));
      } else if (this.doubleNumericRestricts.containsKey(columnQualifier)) {
        vectorEmbeddings.addNumericRestrict(
            NumericRestrict.doubleValue(doubleNumericRestricts.get(columnQualifier), value));
      }
    }

    private Map<String, String> extractColumnsAliases(ValueProvider<String> restricts) {
      Map<String, String> columnsWithAliases = new HashMap<>();
      if (StringUtils.isBlank(restricts.get())) {
        return columnsWithAliases;
      }
      String[] columnsList = restricts.get().split(",");

      for (String columnsWithAlias : columnsList) {
        String[] columnWithAlias = columnsWithAlias.split(";");
        if (columnWithAlias.length == 2) {
          columnsWithAliases.put(columnWithAlias[0], columnWithAlias[1]);
        }
      }
      return columnsWithAliases;
    }
  }
}

// Data model classes.
class Restrict {
  String namespace;
  List<String> allow;
  List<String> deny;

  static Restrict allowRestrict(String namespace, ByteString value) {
    Restrict restrict = new Restrict();
    restrict.namespace = namespace;
    restrict.allow = new ArrayList<String>();
    restrict.allow.add(value.toStringUtf8());
    return restrict;
  }

  static Restrict denyRestrict(String namespace, ByteString value) {
    Restrict restrict = new Restrict();
    restrict.namespace = namespace;
    restrict.deny = new ArrayList<String>();
    restrict.deny.add(value.toStringUtf8());
    return restrict;
  }
}

class NumericRestrict {
  enum Type {
    INT,
    FLOAT,
    DOUBLE
  };

  String namespace;
  Type type;
  Integer valueInt;
  Float valueFloat;
  Double valueDouble;

  static NumericRestrict intValue(String namespace, ByteString value) {
    NumericRestrict restrict = new NumericRestrict();
    restrict.namespace = namespace;
    restrict.valueInt = Bytes.toInt(value.toByteArray());
    restrict.type = Type.INT;
    return restrict;
  }

  static NumericRestrict floatValue(String namespace, ByteString value) {
    NumericRestrict restrict = new NumericRestrict();
    restrict.namespace = namespace;
    restrict.valueFloat = Bytes.toFloat(value.toByteArray());
    restrict.type = Type.FLOAT;
    return restrict;
  }

  static NumericRestrict doubleValue(String namespace, ByteString value) {
    NumericRestrict restrict = new NumericRestrict();
    restrict.namespace = namespace;
    restrict.valueDouble = Bytes.toDouble(value.toByteArray());
    restrict.type = Type.DOUBLE;
    return restrict;
  }
}

class VectorEmbeddings {
  String id;
  String crowdingTag;
  List<Float> floatEmbeddings;
  List<Double> doubleEmbeddings;
  List<Restrict> restricts;
  List<NumericRestrict> numericRestricts;

  void addRestrict(Restrict restrict) {
    if (this.restricts == null) {
      this.restricts = new ArrayList<Restrict>();
    }
    restricts.add(restrict);
  }

  void addNumericRestrict(NumericRestrict numericRestrict) {
    if (this.numericRestricts == null) {
      this.numericRestricts = new ArrayList<NumericRestrict>();
    }
    numericRestricts.add(numericRestrict);
  }
}
