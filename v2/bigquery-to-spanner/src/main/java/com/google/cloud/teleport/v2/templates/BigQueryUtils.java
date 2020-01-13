package com.google.cloud.teleport.v2.templates;

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class to contain helper factories for interacting with BigQuery directly.
 */
public class BigQueryUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryUtils.class);

  /**
   * Factory to create ReadSessions.
   */
  static class ReadSessionFactory {

    /**
     * Creates ReadSession for schema extraction.
     *
     * @param client BigQueryStorage client used to create ReadSession.
     * @param tableString String that represents table to export from.
     * @param tableReadOptions TableReadOptions that specify any fields in the table to filter on.
     * @return session ReadSession object that contains the schema for the export.
     */
    static ReadSession create(
        BigQueryStorageClient client, String tableString, TableReadOptions tableReadOptions) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(tableString);
      String parentProjectId = "projects/" + tableReference.getProjectId();

      TableReferenceProto.TableReference storageTableRef =
          TableReferenceProto.TableReference.newBuilder()
              .setProjectId(tableReference.getProjectId())
              .setDatasetId(tableReference.getDatasetId())
              .setTableId(tableReference.getTableId())
              .build();

      CreateReadSessionRequest.Builder builder =
          CreateReadSessionRequest.newBuilder()
              .setParent(parentProjectId)
              .setReadOptions(tableReadOptions)
              .setTableReference(storageTableRef);
      try {
        return client.createReadSession(builder.build());
      } catch (InvalidArgumentException iae) {
        LOG.error("Error creating ReadSession: " + iae.getMessage());
        throw new RuntimeException(iae);
      }
    }
  }

  static class BigQueryStorageClientFactory {

    /**
     * Creates BigQueryStorage client for use in extracting table schema.
     *
     * @return BigQueryStorageClient
     */
    static BigQueryStorageClient create() {
      try {
        return BigQueryStorageClient.create();
      } catch (IOException e) {
        LOG.error("Error connecting to BigQueryStorage API: " + e.getMessage());
        throw new RuntimeException(e);
      }
    }

  }
}
