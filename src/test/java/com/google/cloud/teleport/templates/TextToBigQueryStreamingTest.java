package com.google.cloud.teleport.templates;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.values.FailsafeElement;
import java.io.IOException;
import java.util.Collections;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.junit.Test;

/** Test cases for the {@link TextToBigQueryStreaming} class. */
public class TextToBigQueryStreamingTest {
    /**
     * Tests the TextToBigQueryStreaming.wrapBigQueryInsertError() returns the FailsafeElement
     * with jsonized table row and error message.
     */
    @Test
    public void wrapBigQueryInsertErrorTest() {
        TableRow row = new TableRow();
        row.set("ticker", "GOOGL");
        row.set("price", 1006.94);

        BigQueryInsertError error = createBigQueryInsertError(row);
        FailsafeElement failsafeElement = TextToBigQueryStreaming.wrapBigQueryInsertError(error);

        try {
            JsonFactory factory = Transport.getJsonFactory();
            assertThat(failsafeElement.getOriginalPayload(), is(equalTo(factory.toPrettyString(error.getRow()))));
            assertThat(failsafeElement.getErrorMessage(), is(equalTo(factory.toPrettyString(error.getError()))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private BigQueryInsertError createBigQueryInsertError(TableRow row) {
        return new BigQueryInsertError(
            row,
            new TableDataInsertAllResponse.InsertErrors()
                .setIndex(0L)
                .setErrors(Collections.singletonList(
                    new ErrorProto()
                        .setReason("a Reason")
                        .setLocation("A location")
                        .setMessage("A Message")
                        .setDebugInfo("The debug info")
                )),
            new TableReference()
                .setProjectId("dummy-project-id")
                .setDatasetId("dummy-dataset-id")
                .setTableId("dummy-table-id"));
    }
}
