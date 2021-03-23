package com.google.cloud.teleport.v2.transforms;

import static com.google.cloud.teleport.v2.templates.ProtegrityDataTokenization.FAILSAFE_ELEMENT_CODER;

import com.google.cloud.teleport.v2.utils.SchemasUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * The {@link JsonToBeamRow} converts jsons string to beam rows.
 */
public class JsonToBeamRow extends PTransform<PCollection<String>, PCollection<Row>> {

  final String failedToParseDeadLetterPath;
  final SchemasUtils schema;

  public JsonToBeamRow(String failedToParseDeadLetterPath, SchemasUtils schema) {
    this.failedToParseDeadLetterPath = failedToParseDeadLetterPath;
    this.schema = schema;
  }

  @Override
  public PCollection<Row> expand(PCollection<String> jsons) {
    JsonToRow.ParseResult rows = jsons
        .apply("JsonToRow",
            JsonToRow.withExceptionReporting(schema.getBeamSchema()).withExtendedErrorInfo());

    if (failedToParseDeadLetterPath != null) {
      /*
       * Write Row conversion errors to filesystem specified path
       */
      rows.getFailedToParseLines()
          .apply("ToFailsafeElement",
              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                  .via((Row errRow) -> FailsafeElement
                      .of(errRow.getString("line"), errRow.getString("line"))
                      .setErrorMessage(errRow.getString("err"))
                  ))
          .apply("WriteCsvConversionErrorsToGcs",
              ErrorConverters.WriteErrorsToTextIO.<String, String>newBuilder()
                  .setErrorWritePath(failedToParseDeadLetterPath)
                  .setTranslateFunction(SerializableFunctions.getCsvErrorConverter())
                  .build());
    }
    return rows.getResults().setRowSchema(schema.getBeamSchema());
  }
}
