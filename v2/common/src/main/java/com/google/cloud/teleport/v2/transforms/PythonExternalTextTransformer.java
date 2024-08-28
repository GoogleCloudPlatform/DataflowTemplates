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
package com.google.cloud.teleport.v2.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.JavascriptTextTransformerOptions;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.PythonCallableSource;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PythonExternalTextTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(PythonExternalTextTransformer.class);

  public interface PythonExternalTextTransformerOptions extends JavascriptTextTransformerOptions {
    @TemplateParameter.GcsReadFile(
        order = 1,
        optional = true,
        description = "Cloud Storage path to python UDF source",
        helpText =
            "The Cloud Storage path pattern for the Python code containing your user-defined "
                + "functions.",
        example = "gs://your-bucket/your-function.py")
    String getPythonExternalTextTransformGcsPath();

    void setPythonExternalTextTransformGcsPath(String pythonExternalTextTransformerGcsPath);

    @TemplateParameter.Text(
        order = 2,
        optional = true,
        regexes = {"[a-zA-Z0-9_]+"},
        description = "UDF Python Function Name",
        helpText =
            "The name of the function to call from your Python file. Use only letters, digits, and underscores.",
        example = "'transform' or 'transform_udf1'")
    String getPythonExternalTextTransformFunctionName();

    void setPythonExternalTextTransformFunctionName(String pythonExternalTextTransformFunctionName);
  }

  @AutoValue
  public abstract static class FailsafePythonExternalUdf
      extends PTransform<PCollection<Row>, PCollection<Row>> {
    public abstract @Nullable String fileSystemPath();

    public abstract @Nullable String functionName();

    public abstract @Nullable Boolean loggingEnabled();

    public abstract @Nullable Integer reloadIntervalMinutes();

    public static Builder newBuilder() {
      return new AutoValue_PythonExternalTextTransformer_FailsafePythonExternalUdf.Builder();
    }

    /** Builder for {@link FailsafePythonExternalUdf}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFileSystemPath(@Nullable String fileSystemPath);

      public abstract Builder setFunctionName(@Nullable String functionName);

      public abstract Builder setReloadIntervalMinutes(Integer value);

      public abstract Builder setLoggingEnabled(@Nullable Boolean loggingEnabled);

      public abstract FailsafePythonExternalUdf build();
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> elements) {
      return elements
          .apply(
              PythonExternalTransform.<PCollection<Row>, PCollection<Row>>from("__constructor__")
                  .withArgs(
                      PythonCallableSource.of(
                          String.format(
                              "import traceback\n"
                                  + "from typing import Mapping\n"
                                  + "from typing import NamedTuple\n"
                                  + "from typing import Optional\n"
                                  + "\n"
                                  + "import apache_beam as beam\n"
                                  + "from apache_beam import coders\n"
                                  + "from apache_beam.io.filesystems import FileSystems\n"
                                  + "from apache_beam.utils import python_callable\n"
                                  + "\n"
                                  + "class ElementRow(NamedTuple):\n"
                                  + "  messageId:  Optional[str]\n"
                                  + "  message:    Optional[str]\n"
                                  + "  attributes: Optional[Mapping[str, str]]\n"
                                  + "\n"
                                  + "class FailsafeRow(NamedTuple):\n"
                                  + "  original:      ElementRow\n"
                                  + "  transformed:   ElementRow\n"
                                  + "  error_message: Optional[str]\n"
                                  + "  stack_trace:   Optional[str]\n"
                                  + "coders.registry.register_coder(FailsafeRow, coders.RowCoder)\n"
                                  + "\n"
                                  + "class UdfTransform(beam.PTransform):\n"
                                  + "  def expand(self, pcoll):\n"
                                  + "    return pcoll | \"applyUDF\" >> beam.ParDo(self.UdfDoFn())\n"
                                  + "\n"
                                  + "  @beam.typehints.with_output_types(FailsafeRow)\n"
                                  + "  class UdfDoFn(beam.DoFn):\n"
                                  + "    def __init__(self):\n"
                                  + "      self.udf_file = FileSystems.open(\"%s\").read().decode()\n"
                                  + "\n"
                                  + "    def process(self, elem):\n"
                                  + "      try:\n"
                                  + "        transformed_message = python_callable.PythonCallableWithSource.load_from_script(\n"
                                  + "          self.udf_file, \"%s\")(elem.message)\n"
                                  + "        transformed_row = ElementRow(messageId=str(elem.messageId),\n"
                                  + "                                   message=str(transformed_message),\n"
                                  + "                                   attributes=elem.attributes)\n"
                                  + "        error_message = \"\"\n"
                                  + "        stack_trace = \"\"\n"
                                  + "\n"
                                  + "      except Exception as e:\n"
                                  + "        transformed_row = elem\n"
                                  + "        error_message = str(e)\n"
                                  + "        stack_trace = traceback.format_exc()\n"
                                  + "      yield FailsafeRow(original=elem, transformed=transformed_row,\n"
                                  + "                     error_message=error_message, stack_trace=stack_trace)",
                              fileSystemPath(), functionName()))))
          .setRowSchema(PythonExternalTextTransformer.FailsafeRowPythonExternalUdf.FAILSAFE_SCHEMA);
    }
  }

  public abstract static class FailsafeRowPythonExternalUdf
      extends PTransform<PCollection<PubsubMessage>, PCollection<Row>> {
    public static final Schema ROW_SCHEMA =
        Schema.builder()
            .addNullableStringField("messageId")
            .addNullableStringField("message")
            .addNullableMapField("attributes", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .build();

    public static final Schema FAILSAFE_SCHEMA =
        Schema.builder()
            .addNullableRowField("original", ROW_SCHEMA)
            .addNullableRowField("transformed", ROW_SCHEMA)
            .addNullableStringField("error_message")
            .addNullableStringField("stack_trace")
            .build();

    public static <T> MapElements<T, Row> pubSubMappingFunction() {
      return MapElements.into(TypeDescriptor.of(Row.class))
          .via(
              (message) -> {
                assert message != null;
                return pubSubMessageToRow((PubsubMessage) message);
              });
    }

    public static <T> MapElements<T, Row> stringMappingFunction() {
      return MapElements.into(TypeDescriptor.of(Row.class))
          .via(
              (message) -> {
                assert message != null;
                return stringMessageToRow((String) message);
              });
    }

    public static Row pubSubMessageToRow(PubsubMessage message) {
      String messageId = message.getMessageId();
      String payload = new String(message.getPayload());
      Map<String, String> attributeMap = new HashMap<>();
      if (message.getAttributeMap() != null) {
        attributeMap.putAll(message.getAttributeMap());
      }

      Map<String, Object> rowValuesMap = new HashMap<>();
      rowValuesMap.put("messageId", messageId);
      rowValuesMap.put("message", payload);
      rowValuesMap.put("attributes", attributeMap);
      return Row.withSchema(ROW_SCHEMA).withFieldValues(rowValuesMap).build();
    }

    public static Row stringMessageToRow(String message) {
      Map<String, Object> rowValuesMap = new HashMap<>();
      rowValuesMap.put("message", message);
      return Row.withSchema(ROW_SCHEMA).withFieldValues(rowValuesMap).build();
    }
  }

  public static class RowToStringFailsafeElementFn
      extends DoFn<Row, FailsafeElement<String, String>> {
    TupleTag udfSuccessTag;
    TupleTag udfFailureTag;

    public RowToStringFailsafeElementFn(
        TupleTag<FailsafeElement<String, String>> udfSuccessTag,
        TupleTag<FailsafeElement<String, String>> udfFailureTag) {
      this.udfSuccessTag = udfSuccessTag;
      this.udfFailureTag = udfFailureTag;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Row element = context.element();
      assert element != null;
      Row originalMessageRow = element.getValue("original");
      String originalMsg = originalMessageRow.getValue("message");
      if (!Strings.isNullOrEmpty(element.getValue("error_message"))
          || !Strings.isNullOrEmpty(element.getValue("stack_trace"))) {
        FailsafeElement failsafeObj =
            FailsafeElement.of(
                    originalMsg, new String(originalMsg.getBytes(), StandardCharsets.UTF_8))
                .setStacktrace(element.getValue("stack_trace"))
                .setErrorMessage(element.getValue("error_message"));
        context.output(udfFailureTag, failsafeObj);
      } else {
        Row transformedMessageRow = element.getValue("transformed");
        String transformedMsg = transformedMessageRow.getValue("message");
        FailsafeElement failsafeObj =
            FailsafeElement.of(
                originalMsg, new String(transformedMsg.getBytes(), StandardCharsets.UTF_8));
        context.output(udfSuccessTag, failsafeObj);
      }
    }
  }

  public static class RowToStringElementFn extends DoFn<Row, String> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      Row row = context.element();
      String errorMessage = row.getValue("error_message");
      String stackTrace = row.getValue("stack_trace");
      if (!Strings.isNullOrEmpty(errorMessage) || !Strings.isNullOrEmpty(stackTrace)) {
        Object originalElement = row.getValue("original");
        throw new RuntimeException(
            String.format(
                "Error applying UDF to the source record [%s], error message: [%s], stack trace: [%s]",
                originalElement, errorMessage, stackTrace));
      }

      Row transformedRow = row.getValue("transformed");
      context.output(transformedRow.getValue("message"));
    }
  }

  public static class RowToTableRowElementFn extends DoFn<Row, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      Row row = context.element();
      String errorMessage = row.getValue("error_message");
      String stackTrace = row.getValue("stack_trace");
      if (!Strings.isNullOrEmpty(errorMessage) || !Strings.isNullOrEmpty(stackTrace)) {
        Object originalElement = row.getValue("original");
        throw new RuntimeException(
            String.format(
                "Error applying UDF to the source record [%s], error message: [%s], stack trace: [%s]",
                originalElement, errorMessage, stackTrace));
      }

      Row transformedRow = row.getValue("transformed");
      context.output(BigQueryConverters.convertJsonToTableRow(transformedRow.getValue("message")));
    }
  }

  public static class RowToPubSubFailsafeElementFn
      extends DoFn<Row, FailsafeElement<PubsubMessage, String>> {
    TupleTag udfSuccessTag;
    TupleTag udfFailureTag;

    public RowToPubSubFailsafeElementFn(
        TupleTag<FailsafeElement<PubsubMessage, String>> udfSuccessTag,
        TupleTag<FailsafeElement<PubsubMessage, String>> udfFailureTag) {
      this.udfSuccessTag = udfSuccessTag;
      this.udfFailureTag = udfFailureTag;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      Row element = context.element();
      assert element != null;
      PubsubMessage originalMessage = rowToPubSubMessage(element.getValue("original"));

      if (!Strings.isNullOrEmpty(element.getValue("error_message"))
          || !Strings.isNullOrEmpty(element.getValue("stack_trace"))) {
        FailsafeElement failsafeObj =
            FailsafeElement.of(
                    originalMessage,
                    new String(originalMessage.getPayload(), StandardCharsets.UTF_8))
                .setStacktrace(element.getValue("stack_trace"))
                .setErrorMessage(element.getValue("error_message"));
        context.output(udfFailureTag, failsafeObj);
      } else {
        PubsubMessage transformedMessage = rowToPubSubMessage(element.getValue("transformed"));
        FailsafeElement failsafeObj =
            FailsafeElement.of(
                originalMessage,
                new String(transformedMessage.getPayload(), StandardCharsets.UTF_8));
        context.output(udfSuccessTag, failsafeObj);
      }
    }

    public PubsubMessage rowToPubSubMessage(Row row) {
      assert row != null;
      String messageId = row.getValue("messageId");
      String payload = row.getValue("message");
      Map<String, String> attributeMap = row.getValue("attributes");

      assert payload != null;
      return new PubsubMessage(payload.getBytes(), attributeMap, messageId);
    }
  }

  public static void overwritepyVersion(PipelineOptions options) {
    // no-op
  }
}
