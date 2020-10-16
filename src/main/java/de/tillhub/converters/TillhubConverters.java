package de.tillhub.converters;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.templates.common.BigQueryConverters;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import de.tillhub.coders.TableRowsArrayJsonCoder;
import de.tillhub.converters.AutoValue_TillhubConverters_FailsafeJsonToTableRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class TillhubConverters {

    /** The log to output status messages to. */
    private static final Logger LOG = LoggerFactory.getLogger(TillhubConverters.class);

    /**
     * The {@link TillhubConverters.FailsafeJsonToTableRow} transform converts JSON strings to {@link TableRow} objects.
     * The transform accepts a {@link FailsafeElement} object so the original payload of the incoming
     * record can be maintained across multiple series of transforms.
     */
    @AutoValue
    public abstract static class FailsafeJsonToTableRow<T>
            extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

        public abstract TupleTag<TableRow> successTag();

        public abstract TupleTag<FailsafeElement<T, String>> failureTag();

        public static <T> TillhubConverters.FailsafeJsonToTableRow.Builder<T> newBuilder() {
            return new AutoValue_TillhubConverters_FailsafeJsonToTableRow.Builder<>();
        }

        /** Builder for {@link TillhubConverters.FailsafeJsonToTableRow}. */
        @AutoValue.Builder
        public abstract static class Builder<T> {
            public abstract TillhubConverters.FailsafeJsonToTableRow.Builder<T> setSuccessTag(TupleTag<TableRow> successTag);

            public abstract TillhubConverters.FailsafeJsonToTableRow.Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

            public abstract TillhubConverters.FailsafeJsonToTableRow<T> build();
        }

        @Override
        public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> failsafeElements) {
            return failsafeElements.apply(
                    "JsonToTableRow",
                    ParDo.of(
                            new DoFn<FailsafeElement<T, String>, TableRow>() {
                                @ProcessElement
                                public void processElement(ProcessContext context) {
                                    FailsafeElement<T, String> element = context.element();
                                    String payload = element.getPayload();
                                    LOG.info("Before splitting child elements " + payload);
                                    String[] split = payload.split("~#~#~");
                                    String json = split[0];
                                    String clientAccount = split[1];
                                    String transaction = split[2];

                                    try {
                                        LOG.info("About to convert to Rows json " + json);
                                        TableRow[] rows = convertJsonToTableRow(json);
                                        for (int i = 0 ; i < rows.length ; i++) {
                                            rows[i].set("client_account", clientAccount);
                                            rows[i].set("oltp_entity_id", transaction);
                                            context.output(rows[i]);
                                        }
                                    } catch (Exception e) {
                                        context.output(
                                                failureTag(),
                                                FailsafeElement.of(element)
                                                        .setErrorMessage(e.getMessage())
                                                        .setStacktrace(Throwables.getStackTraceAsString(e)));
                                    }
                                }
                            })
                            .withOutputTags(successTag(), TupleTagList.of(failureTag())));
        }

        /**
         * Converts a JSON string to a {@link TableRow} object. If the data fails to convert, a {@link
         * RuntimeException} will be thrown.
         *
         * @param json The JSON string to parse.
         * @return The parsed {@link TableRow} object.
         */
        public static TableRow[] convertJsonToTableRow(String json) {
            TableRow[] rows;
            // Parse the JSON into a {@link TableRow} object.
            try (InputStream inputStream =
                         new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
                rows = TableRowsArrayJsonCoder.of().decode(inputStream, Coder.Context.OUTER);

            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize json to table row: " + json, e);
            }

            return rows;
        }
    }

}
