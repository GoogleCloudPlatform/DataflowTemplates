/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common transforms for Beam Row.
 */
public class BeamRowConverters {

  /**
   * The tag for the main output for the transform.
   */
  public static final TupleTag<Row> TRANSFORM_OUT = new TupleTag<Row>() {
  };
  /**
   * The tag for the dead-letter output of the transform.
   */
  public static final TupleTag<FailsafeElement<String, String>> TRANSFORM_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {
      };
  /**
   * Default String/String Coder for FailsafeElement.
   */
  public static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER =
      FailsafeElementCoder.of(
          NullableCoder.of(StringUtf8Coder.of()), NullableCoder.of(StringUtf8Coder.of()));
  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(BeamRowConverters.class);

  /**
   * The {@link FailsafeJsonToBeamRow} converts jsons string as {@link FailsafeElement} to beam rows
   * as {@link PCollectionTuple}.
   */

  @AutoValue
  public abstract static class FailsafeJsonToBeamRow<T> extends
      PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_BeamRowConverters_FailsafeJsonToBeamRow.Builder<T>()
          .setFailsafeElementCoder(FAILSAFE_ELEMENT_CODER);
    }

    public abstract Schema beamSchema();

    public abstract TupleTag<Row> successTag();

    public abstract TupleTag<FailsafeElement<String, String>> failureTag();

    public abstract FailsafeElementCoder<String, String> failsafeElementCoder();

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> jsons) {
      JsonToRow.ParseResult rows = jsons
          .apply("FailsafeToString",
              MapElements.into(TypeDescriptors.strings()).via(FailsafeElement::getPayload))
          .apply("JsonToRow",
              JsonToRow.withExceptionReporting(beamSchema()).withExtendedErrorInfo());
      /*
       * Write Row conversion errors to filesystem specified path
       */
      PCollection<FailsafeElement<String, String>> failures = rows.getFailedToParseLines()
          .apply("ToFailsafeElement",
              MapElements.into(failsafeElementCoder().getEncodedTypeDescriptor())
                  .via((Row errRow) -> FailsafeElement
                      .of(errRow.getString("line"), errRow.getString("line"))
                      .setErrorMessage(errRow.getString("err"))
                  ));
      return PCollectionTuple.of(successTag(), rows.getResults()).and(failureTag(), failures);
    }

    /**
     * Builder for {@link FailsafeJsonToBeamRow}.
     */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      public abstract Builder<T> setSuccessTag(TupleTag<Row> successTag);

      public abstract Builder<T> setFailureTag(
          TupleTag<FailsafeElement<String, String>> failsafeTag);

      public abstract Builder<T> setBeamSchema(Schema beamSchema);

      public abstract Builder<T> setFailsafeElementCoder(
          FailsafeElementCoder<String, String> failsafeElementCoder);

      public abstract FailsafeJsonToBeamRow<T> build();
    }
  }

  /**
   * {@link RowToCsvRecord} is a {@link PTransform} that converts {@link Row} to {@link String} in
   * csv format. By default This transform uses default translation function {@link
   * RowToCsvRecord.RowToCsvDefaultSerializableFunction} with ";" as delimiter for csv records and
   * "null" as nullSubstitutor. To specify custom translation function, need to call
   * setTranslationFunction
   */
  @AutoValue
  public abstract static class RowToCsvRecord extends
      PTransform<PCollection<Row>, PCollection<String>> {

    public static BeamRowConverters.RowToCsvRecord.Builder newBuilder() {
      return new AutoValue_BeamRowConverters_RowToCsvRecord.Builder()
          .setTranslateFunction(new RowToCsvDefaultSerializableFunction());
    }

    abstract RowToCsvRecord.Builder toBuilder();


    public abstract @Nullable
    SerializableFunction<Row, String> getTranslateFunction();


    @Override
    public PCollection<String> expand(PCollection<Row> pCollection) {

      return pCollection
          .apply("RowToCsv",
              MapElements.into(TypeDescriptors.strings())
                  .via(getTranslateFunction()));

    }

    /**
     * Builder for {@link BeamRowConverters.RowToCsvRecord}.
     */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract BeamRowConverters.RowToCsvRecord.Builder setTranslateFunction(
          @Nullable SerializableFunction<Row, String> translateFunction);

      public abstract BeamRowConverters.RowToCsvRecord build();
    }

    private static class RowToCsvDefaultSerializableFunction implements
        SerializableFunction<Row, String> {

      public RowToCsvDefaultSerializableFunction() {
      }

      @Override
      public String apply(Row input) {
        String nullSubstitutor = "null";
        String delimmiter = ";";
        return getCsvFromRow(input, delimmiter, nullSubstitutor);
      }
    }
  }

  /**
   * {@link FailsafeRowToFailsafeCsv} is a {@link PTransform} that converts {@link
   * FailsafeElement<Row>} to {@link FailsafeElement<String>} in csv format. By default This
   * transform uses default translation function {@link FailsafeRowToFailsafeCsv.FailsafeRowToFailsafeCsvDefaultSerializableFunction}
   * with ";" as delimiter for csv records and "null" as nullSubstitutor. To specify custom
   * translation function, need to call setTranslationFunction
   */
  @AutoValue
  public abstract static class FailsafeRowToFailsafeCsv extends
      PTransform<PCollection<FailsafeElement<Row, Row>>, PCollection<FailsafeElement<String, String>>> {

    public static BeamRowConverters.FailsafeRowToFailsafeCsv.Builder newBuilder() {
      return new AutoValue_BeamRowConverters_FailsafeRowToFailsafeCsv.Builder()
          .setTranslateFunction(new FailsafeRowToFailsafeCsvDefaultSerializableFunction());
    }

    abstract BeamRowConverters.FailsafeRowToFailsafeCsv.Builder toBuilder();


    public abstract @Nullable
    SerializableFunction<FailsafeElement<Row, Row>, FailsafeElement<String, String>> getTranslateFunction();


    @Override
    public PCollection<FailsafeElement<String, String>> expand(
        PCollection<FailsafeElement<Row, Row>> pCollection) {

      return pCollection
          .apply("FailsafeRowToFailsafeCsv",
              MapElements.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor())
                  .via(getTranslateFunction()));

    }

    /**
     * Builder for {@link BeamRowConverters.FailsafeRowToFailsafeCsv}.
     */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract BeamRowConverters.FailsafeRowToFailsafeCsv.Builder setTranslateFunction(
          @Nullable SerializableFunction<FailsafeElement<Row, Row>, FailsafeElement<String, String>> translateFunction);

      public abstract BeamRowConverters.FailsafeRowToFailsafeCsv build();
    }

    private static class FailsafeRowToFailsafeCsvDefaultSerializableFunction implements
        SerializableFunction<FailsafeElement<Row, Row>, FailsafeElement<String, String>> {

      public FailsafeRowToFailsafeCsvDefaultSerializableFunction() {
      }

      @Override
      public FailsafeElement<String, String> apply(FailsafeElement<Row, Row> input) {
        String nullSubstitutor = "null";
        String delimmiter = ";";

        return FailsafeElement
            .of(getCsvFromRow(input.getOriginalPayload(), delimmiter, nullSubstitutor),
                getCsvFromRow(input.getPayload(), delimmiter, nullSubstitutor));
      }
    }

  }


  /**
   * Simple method to convert row to the CSV record using specific delimiter and nullSubstitutor
   * from parameters.
   */
  public static String getCsvFromRow(Row row, String delimiter, String nullSubstitutor) {
    return row.getValues()
        .stream()
        .map(item -> item == null ? nullSubstitutor : item)
        .map(Object::toString)
        .collect(Collectors.joining(delimiter));
  }
}
