/*
 * Copyright (C) 2020 Google Inc.
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
package com.google.cloud.teleport.v2.transforms.io;


import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.v2.templates.ProtegrityDataTokenization;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The {@link BigQueryIO} class for writing data from template to BigTable.
 */
public class BigQueryIO {
    /**
     * Logger for class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryIO.class);

    public static WriteResult write(PCollection<Row> input, String bigQueryTableName, TableSchema schema) {
        return input
                .apply("RowToTableRow", ParDo.of(new BigQueryConverters.RowToTableRowFn()))
                .apply(
                        "WriteSuccessfulRecords",
                        org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.writeTableRows()
                                .withCreateDisposition(org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withExtendedErrorInfo()
                                .withMethod(org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.STREAMING_INSERTS)
                                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                .withSchema(schema)
                                .to(bigQueryTableName));
    }

    /**
     * Method to wrap a {@link BigQueryInsertError} into a {@link FailsafeElement}.
     *
     * @param insertError BigQueryInsert error.
     * @return FailsafeElement object.
     */
    public static FailsafeElement<String, String> wrapBigQueryInsertError(
            BigQueryInsertError insertError) {

        FailsafeElement<String, String> failsafeElement;
        try {

            failsafeElement =
                    FailsafeElement.of(
                            insertError.getRow().toPrettyString(), insertError.getRow().toPrettyString());
            failsafeElement.setErrorMessage(insertError.getError().toPrettyString());

        } catch (IOException e) {
            BigQueryIO.LOG.error("Failed to wrap BigQuery insert error.");
            throw new RuntimeException(e);
        }
        return failsafeElement;
    }
}
