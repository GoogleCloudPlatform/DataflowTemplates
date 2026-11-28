/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.mongodb.validator.options;

import com.google.cloud.teleport.metadata.TemplateParameter;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Pipeline options for the MongoDB to Cloud Firestore Data Validator Flex Template. */
public interface MongoDbDataValidatorOptions extends PipelineOptions {

  @TemplateParameter.Password(
      order = 1,
      groupName = "Source",
      description = "Source MongoDB Connection URI",
      helpText =
          "MongoDB connection URI for the source cluster (e.g.,"
              + " mongodb://user:pass@host:27017/?authSource=admin).")
  @Required
  String getSourceUri();

  void setSourceUri(String value);

  @TemplateParameter.Password(
      order = 2,
      groupName = "Target",
      description = "Target Firestore (MongoDB Compatibility) Connection URI",
      helpText =
          "MongoDB-compatible connection URI for the destination Cloud Firestore Enterprise"
              + " database.")
  @Required
  String getTargetUri();

  void setTargetUri(String value);

  @TemplateParameter.Text(
      order = 3,
      groupName = "Source",
      description = "Source MongoDB Database",
      helpText = "Name of the source MongoDB database to validate.")
  @Required
  String getSourceDatabase();

  void setSourceDatabase(String value);

  @TemplateParameter.Text(
      order = 4,
      groupName = "Target",
      optional = true,
      description = "Target Firestore Database",
      helpText = "Name of the target database to validate. Defaults to sourceDatabase if omitted.")
  @Default.String("")
  String getTargetDatabase();

  void setTargetDatabase(String value);

  @TemplateParameter.GcsWriteFolder(
      order = 5,
      description = "GCS Output Directory for Validation Reports",
      helpText =
          "Cloud Storage base path where JSONL mismatch records, per-collection stats, and the"
              + " validation summary JSON will be written.",
      example = "gs://your-bucket/mongodb-validation")
  @Required
  String getGcsOutputPath();

  void setGcsOutputPath(String value);

  @TemplateParameter.Text(
      order = 6,
      optional = true,
      regexes = {"^[^ ;]*$"},
      description = "Optional BigQuery Dataset for Validation Tables",
      helpText =
          "Optional BigQuery dataset (e.g., project.dataset or dataset) where MismatchedRecords,"
              + " CollectionValidationStats, and ValidationSummary tables will be appended.",
      example = "my_project.validation_reports")
  @Default.String("")
  String getBigQueryDataset();

  void setBigQueryDataset(String value);

  @TemplateParameter.Text(
      order = 7,
      optional = true,
      regexes = {"^[^ ;]*$"},
      description = "Validation Run ID",
      helpText =
          "Unique identifier for this validation run. Defaults to <jobName>_<timestamp> if"
              + " omitted.",
      example = "cutover_run_01")
  @Default.String("")
  String getRunId();

  void setRunId(String value);

  @TemplateParameter.Text(
      order = 8,
      groupName = "Source",
      optional = true,
      description = "Comma-Separated Source Collections",
      helpText =
          "Comma-separated list of source collections to validate. If empty, validates the union of"
              + " all non-system collections across source and target databases.")
  @Default.String("")
  String getCollections();

  void setCollections(String value);

  @TemplateParameter.Text(
      order = 9,
      optional = true,
      description = "Collection Name Mappings (sourceCol:targetCol)",
      helpText =
          "Optional comma-separated source:target collection mappings when collections were renamed"
              + " during migration (e.g., users_v1:users,orders_legacy:orders).")
  @Default.String("")
  String getCollectionMappings();

  void setCollectionMappings(String value);

  @TemplateParameter.Text(
      order = 10,
      optional = true,
      description = "Ignored Document Field Paths",
      helpText =
          "Optional comma-separated dot-paths of fields to exclude from comparison (e.g.,"
              + " _migrated_at,audit.syncTimestamp).")
  @Default.String("")
  String getIgnoredFields();

  void setIgnoredFields(String value);

  @TemplateParameter.Integer(
      order = 11,
      optional = true,
      description = "Target Document Count per Read Split",
      helpText =
          "Target number of documents per parallel _id range split. Smaller values create finer"
              + " worker parallelism and shorter cursor lifetimes.")
  @Default.Integer(200000)
  Integer getTargetChunkSize();

  void setTargetChunkSize(Integer value);

  @TemplateParameter.Integer(
      order = 12,
      optional = true,
      description = "Maximum Splits per Collection",
      helpText = "Upper bound on parallel _id splits generated per collection.")
  @Default.Integer(256)
  Integer getMaxSplitsPerCollection();

  void setMaxSplitsPerCollection(Integer value);

  @TemplateParameter.Integer(
      order = 13,
      optional = true,
      description = "Cursor & Lookup Batch Size",
      helpText = "Maximum number of documents per cursor batch or $in lookup batch.")
  @Default.Integer(1000)
  Integer getBatchSize();

  void setBatchSize(Integer value);

  @TemplateParameter.Integer(
      order = 14,
      optional = true,
      description = "Maximum Batch Memory Budget (Bytes)",
      helpText =
          "Maximum serialized BSON byte budget per in-memory lookup/recheck batch to prevent OOM on"
              + " large (up to 16 MiB) documents.")
  @Default.Integer(8388608)
  Integer getMaxBatchBytes();

  void setMaxBatchBytes(Integer value);

  @TemplateParameter.Boolean(
      order = 15,
      optional = true,
      description = "Strict BSON Type Checking",
      helpText =
          "When true (default), requires exact BSON numeric types (e.g., int32 != int64 != double)."
              + " When false, permits numeric equivalence within the exact 2^53 integer range.")
  @Default.Boolean(true)
  Boolean getStrictTypeChecking();

  void setStrictTypeChecking(Boolean value);

  @TemplateParameter.Boolean(
      order = 16,
      optional = true,
      description = "Ignore Subdocument Field Ordering",
      helpText =
          "When false (default), enforces exact BSON key ordering inside embedded subdocuments."
              + " When true, compares subdocument keys order-independently.")
  @Default.Boolean(false)
  Boolean getIgnoreSubdocumentFieldOrder();

  void setIgnoreSubdocumentFieldOrder(Boolean value);

  @TemplateParameter.Boolean(
      order = 17,
      optional = true,
      description = "Validate Secondary Indexes and TTL Specs",
      helpText =
          "When true (default), compares secondary index key definitions, uniqueness, sparse flags,"
              + " and TTL expireAfterSeconds between source and target collections.")
  @Default.Boolean(true)
  Boolean getValidateIndexes();

  void setValidateIndexes(Boolean value);

  @TemplateParameter.Integer(
      order = 18,
      optional = true,
      description = "Delayed Recheck Retries for Candidate Mismatches",
      helpText =
          "Number of delayed primary/majority recheck attempts for candidate document mismatches to"
              + " filter out transient live-CDC replication lag. Set to 0 for static offline"
              + " cutovers.")
  @Default.Integer(2)
  Integer getMismatchRecheckRetries();

  void setMismatchRecheckRetries(Integer value);

  @TemplateParameter.Integer(
      order = 19,
      optional = true,
      description = "Initial Delay Between Mismatch Rechecks (ms)",
      helpText =
          "Base backoff in milliseconds before re-fetching candidate mismatched _ids from primary.")
  @Default.Integer(5000)
  Integer getMismatchRecheckDelayMs();

  void setMismatchRecheckDelayMs(Integer value);

  @TemplateParameter.Enum(
      order = 20,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("primary"),
        @TemplateParameter.TemplateEnumOption("primaryPreferred"),
        @TemplateParameter.TemplateEnumOption("secondary"),
        @TemplateParameter.TemplateEnumOption("secondaryPreferred")
      },
      optional = true,
      description = "Source MongoDB Read Preference During Bulk Scan",
      helpText =
          "MongoDB read preference used on the source cluster during initial slice scans. Candidate"
              + " mismatch rechecks always read from primary with majority read concern.")
  @Default.String("secondaryPreferred")
  String getSourceReadPreference();

  void setSourceReadPreference(String value);
}
