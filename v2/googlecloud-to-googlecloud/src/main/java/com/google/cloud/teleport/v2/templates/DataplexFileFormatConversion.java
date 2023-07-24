/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormatCsvOptions;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.clients.DefaultDataplexClient;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.io.AvroSinkWithJodaDatesConversion;
import com.google.cloud.teleport.v2.options.DataplexUpdateMetadataOptions;
import com.google.cloud.teleport.v2.templates.DataplexFileFormatConversion.FileFormatConversionOptions;
import com.google.cloud.teleport.v2.transforms.AvroConverters;
import com.google.cloud.teleport.v2.transforms.CsvConverters;
import com.google.cloud.teleport.v2.transforms.JsonConverters;
import com.google.cloud.teleport.v2.transforms.NoopTransform;
import com.google.cloud.teleport.v2.transforms.ParquetConverters;
import com.google.cloud.teleport.v2.utils.DataplexUtils;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.utils.Schemas;
import com.google.cloud.teleport.v2.utils.WriteDisposition.WriteDispositionException;
import com.google.cloud.teleport.v2.utils.WriteDisposition.WriteDispositionOptions;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.cloud.teleport.v2.values.DataplexEnums.DataplexAssetResourceSpec;
import com.google.cloud.teleport.v2.values.DataplexEnums.EntityType;
import com.google.cloud.teleport.v2.values.DataplexEnums.StorageSystem;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DataplexFileFormatConversion} pipeline converts file format of the files from the
 * given asset or the list of entities to, the new converted files are stored in the bucket
 * referenced by the provided output asset.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/googlecloud-to-googlecloud/README_Dataplex_File_Format_Conversion.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "Dataplex_File_Format_Conversion",
    category = TemplateCategory.BATCH,
    displayName = "Dataplex: Convert Cloud Storage File Format",
    description =
        "A pipeline that converts file format of Cloud Storage files, registering metadata for the"
            + " newly created files in Dataplex.",
    optionsClass = FileFormatConversionOptions.class,
    flexContainerName = "dataplex-file-format-conversion",
    contactInformation = "https://cloud.google.com/support")
public class DataplexFileFormatConversion {

  private static final Logger LOG = LoggerFactory.getLogger(DataplexBigQueryToGcs.class);
  private static final int MAX_CREATE_ENTITY_ATTEMPTS = 10;

  /**
   * The {@link FileFormatConversionOptions} provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface FileFormatConversionOptions
      extends GcpOptions, PipelineOptions, ExperimentalOptions, DataplexUpdateMetadataOptions {

    @TemplateParameter.Text(
        order = 1,
        regexes = {
          "^(projects\\/[^\\n\\r\\/]+\\/locations\\/[^\\n\\r\\/]+\\/lakes\\/[^\\n\\r\\/]+\\/zones\\/[^\\n\\r\\/]+\\/assets\\/[^\\n\\r\\/]+|projects\\/[^\\n\\r\\/]+\\/locations\\/[^\\n\\r\\/]+\\/lakes\\/[^\\n\\r\\/]+\\/zones\\/[^\\n\\r\\/]+\\/entities\\/[^\\n\\r\\/,]+(,projects\\/[^\\n\\r\\/]+\\/locations\\/[^\\n\\r\\/]+\\/lakes\\/[^\\n\\r\\/]+\\/zones\\/[^\\n\\r\\/]+\\/entities\\/[^\\n\\r\\/,]+)*)$"
        },
        description = "Dataplex asset name or Dataplex entity names for the files to be converted.",
        helpText =
            "Dataplex asset or Dataplex entities that contain the input files. Format:"
                + " projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset"
                + " name> OR"
                + " projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/entities/<entity"
                + " 1 name>,projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/entities/<entity"
                + " 2 name>... .")
    String getInputAssetOrEntitiesList();

    void setInputAssetOrEntitiesList(String inputAssetOrEntitiesList);

    @TemplateParameter.Enum(
        order = 2,
        enumOptions = {@TemplateEnumOption("AVRO"), @TemplateEnumOption("PARQUET")},
        description = "Output file format in Cloud Storage.",
        helpText = "Output file format in Cloud Storage. Format: PARQUET or AVRO.")
    @Required
    FileFormatOptions getOutputFileFormat();

    void setOutputFileFormat(FileFormatOptions outputFileFormat);

    @TemplateParameter.Enum(
        order = 3,
        enumOptions = {
          @TemplateEnumOption("UNCOMPRESSED"),
          @TemplateEnumOption("SNAPPY"),
          @TemplateEnumOption("GZIP"),
          @TemplateEnumOption("BZIP2")
        },
        optional = true,
        description = "Output file compression in Cloud Storage.",
        helpText =
            "Output file compression. Format: UNCOMPRESSED, SNAPPY, GZIP, or BZIP2. BZIP2 not"
                + " supported for PARQUET files.")
    @Default.Enum("SNAPPY")
    DataplexCompression getOutputFileCompression();

    void setOutputFileCompression(DataplexCompression outputFileCompression);

    @TemplateParameter.Text(
        order = 4,
        regexes = {
          "^projects\\/[^\\n\\r\\/]+\\/locations\\/[^\\n\\r\\/]+\\/lakes\\/[^\\n\\r\\/]+\\/zones\\/[^\\n\\r\\/]+\\/assets\\/[^\\n\\r\\/]+$"
        },
        description = "Dataplex asset name for the destination Cloud Storage bucket.",
        helpText =
            "Name of the Dataplex asset that contains Cloud Storage bucket where output files will"
                + " be put into. Format:"
                + " projects/<name>/locations/<loc>/lakes/<lake-name>/zones/<zone-name>/assets/<asset"
                + " name>.")
    @Required
    String getOutputAsset();

    void setOutputAsset(String outputAsset);

    @TemplateParameter.Enum(
        order = 5,
        enumOptions = {
          @TemplateEnumOption("OVERWRITE"),
          @TemplateEnumOption("FAIL"),
          @TemplateEnumOption("SKIP")
        },
        optional = true,
        description = "Action that occurs if a destination file already exists.",
        helpText =
            "Specifies the action that occurs if a destination file already exists. Format:"
                + " OVERWRITE, FAIL, SKIP. If SKIP, only files that don't exist in the destination"
                + " directory will be processed. If FAIL and at least one file already exists, no"
                + " data will be processed and an error will be produced.")
    @Default.Enum("SKIP")
    WriteDispositionOptions getWriteDisposition();

    void setWriteDisposition(WriteDispositionOptions value);
  }

  /** Supported input file formats. */
  public enum InputFileFormat {
    CSV,
    JSON,
    PARQUET,
    AVRO
  }

  private static final ImmutableSet<String> EXPECTED_INPUT_FILES_EXTENSIONS =
      ImmutableSet.of(".csv", ".json", ".parquet", ".avro");

  private static final Pattern ASSET_PATTERN =
      Pattern.compile(
          "^projects/[^\\n\\r/]+/locations/[^\\n\\r/]+/lakes/[^\\n\\r/]+/zones/[^\\n\\r/]+"
              + "/assets/[^\\n\\r/]+$");
  private static final Pattern ENTITIES_PATTERN =
      Pattern.compile(
          "^projects/[^\\n\\r/]+/locations/[^\\n\\r/]+/lakes/[^\\n\\r/]+/zones/[^\\n\\r/]+"
              + "/entities/[^\\n\\r/]+"
              + "(,projects/[^\\n\\r/]+/locations/[^\\n\\r/]+/lakes/[^\\n\\r/]+/zones/[^\\n\\r/]+"
              + "/entities/[^\\n\\r/]+)*$");

  private static int conversionsCounter = 0;

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) throws IOException {
    UncaughtExceptionLogger.register();

    FileFormatConversionOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(FileFormatConversionOptions.class);
    List<String> experiments = new ArrayList<>();
    if (options.getExperiments() != null) {
      experiments.addAll(options.getExperiments());
    }
    if (!experiments.contains("upload_graph")) {
      experiments.add("upload_graph");
    }
    options.setExperiments(experiments);

    run(
        Pipeline.create(options),
        options,
        DefaultDataplexClient.withDefaultClient(options.getGcpCredential()),
        DataplexFileFormatConversion::gcsOutputPathFrom);
  }

  /**
   * Runs the pipeline to completion with the specified options.
   *
   * @return The pipeline result.
   */
  public static PipelineResult run(
      Pipeline pipeline,
      FileFormatConversionOptions options,
      DataplexClient dataplex,
      OutputPathProvider outputPathProvider)
      throws IOException {
    boolean isInputAsset = ASSET_PATTERN.matcher(options.getInputAssetOrEntitiesList()).matches();
    if (!isInputAsset
        && !ENTITIES_PATTERN.matcher(options.getInputAssetOrEntitiesList()).matches()) {
      throw new IllegalArgumentException(
          "Either input asset or input entities list must be provided");
    }

    GoogleCloudDataplexV1Asset outputAsset = dataplex.getAsset(options.getOutputAsset());
    if (outputAsset == null
        || outputAsset.getResourceSpec() == null
        || !DataplexAssetResourceSpec.STORAGE_BUCKET
            .name()
            .equals(outputAsset.getResourceSpec().getType())
        || outputAsset.getResourceSpec().getName() == null) {
      throw new IllegalArgumentException(
          "Output asset must be an existing asset with resource spec name being a GCS bucket and"
              + " resource spec type of "
              + DataplexAssetResourceSpec.STORAGE_BUCKET.name());
    }
    String outputBucket = outputAsset.getResourceSpec().getName();

    Predicate<String> inputFilesFilter;
    switch (options.getWriteDisposition()) {
      case OVERWRITE:
        inputFilesFilter = inputFilePath -> true;
        break;
      case FAIL:
        Set<String> outputFilePaths = getAllOutputFilePaths(outputBucket);
        inputFilesFilter =
            inputFilePath -> {
              if (outputFilePaths.contains(
                  inputFilePathToOutputFilePath(
                      outputPathProvider,
                      inputFilePath,
                      outputBucket,
                      options.getOutputFileFormat()))) {
                throw new WriteDispositionException(
                    String.format(
                        "The file %s already exists in the output asset bucket: %s",
                        inputFilePath, outputBucket));
              }
              return true;
            };
        break;
      case SKIP:
        outputFilePaths = getAllOutputFilePaths(outputBucket);
        inputFilesFilter =
            inputFilePath ->
                !outputFilePaths.contains(
                    inputFilePathToOutputFilePath(
                        outputPathProvider,
                        inputFilePath,
                        outputBucket,
                        options.getOutputFileFormat()));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported existing file behaviour: " + options.getWriteDisposition());
    }

    ImmutableList<GoogleCloudDataplexV1Entity> entities =
        isInputAsset
            ? dataplex.getCloudStorageEntities(options.getInputAssetOrEntitiesList())
            : dataplex.getEntities(
                Splitter.on(',').trimResults().splitToList(options.getInputAssetOrEntitiesList()));

    List<EntityWithPartitions> processedEntities = new ArrayList<>();
    for (GoogleCloudDataplexV1Entity entity : entities) {
      ImmutableList<GoogleCloudDataplexV1Partition> partitions =
          dataplex.getPartitions(entity.getName());
      if (partitions.isEmpty()) {
        String outputPath = outputPathProvider.outputPathFrom(entity.getDataPath(), outputBucket);
        Iterator<String> inputFilePaths =
            getFilesFromFilePattern(entityToFileSpec(entity)).filter(inputFilesFilter).iterator();
        if (inputFilePaths.hasNext()) {
          processedEntities.add(new EntityWithPartitions(entity));
        }
        inputFilePaths.forEachRemaining(
            inputFilePath ->
                pipeline.apply(
                    "Convert " + shortenDataplexName(entity.getName()),
                    new ConvertFiles(entity, inputFilePath, options, outputPath)));
      } else {
        List<GoogleCloudDataplexV1Partition> processedPartitions = new ArrayList<>();
        for (GoogleCloudDataplexV1Partition partition : partitions) {
          String outputPath =
              outputPathProvider.outputPathFrom(partition.getLocation(), outputBucket);
          Iterator<String> inputFilePaths =
              getFilesFromFilePattern(partitionToFileSpec(partition))
                  .filter(inputFilesFilter)
                  .iterator();
          if (inputFilePaths.hasNext()) {
            processedPartitions.add(partition);
          }
          inputFilePaths.forEachRemaining(
              inputFilePath ->
                  pipeline.apply(
                      "Convert " + shortenDataplexName(partition.getName()),
                      new ConvertFiles(entity, inputFilePath, options, outputPath)));
        }
        if (!processedPartitions.isEmpty()) {
          processedEntities.add(new EntityWithPartitions(entity, processedPartitions));
        }
      }
    }

    if (options.getUpdateDataplexMetadata() && !processedEntities.isEmpty()) {
      updateDataplexMetadata(
          dataplex, options, processedEntities, outputPathProvider, outputBucket);
    }

    if (processedEntities.isEmpty()) {
      pipeline.apply("Nothing to convert", new NoopTransform());
    }

    return pipeline.run();
  }

  @VisibleForTesting
  static void updateDataplexMetadata(
      DataplexClient dataplex,
      FileFormatConversionOptions options,
      List<EntityWithPartitions> processedSourceEntities,
      OutputPathProvider outputPathProvider,
      String outputBucket)
      throws IOException {

    LOG.info("Updating Dataplex metadata...");

    String assetName = options.getOutputAsset();
    Map<String, GoogleCloudDataplexV1Entity> existingOutputDataPathToEntity =
        DataplexUtils.getDataPathToEntityMappingForAsset(
            dataplex, assetName, DataplexUtils.GCS_PATH_ONLY_FILTER);

    // We can end up with 2+ source entities mapped to a single output entity, because the source
    // comes from the getInputAssetOrEntitiesList param, which can be a list of entities,
    // potentially from different assets. For example:
    //   Source Entity 1: gs://src_bucket1/tableA
    //   Source Entity 2: gs://src_bucket2/tableA
    //   Target Entity:   gs://dst_bucket/tableA
    // So first we'll collect all the unique output paths and create a single entity for each one.

    Map<String, GoogleCloudDataplexV1Entity> allOutputDataPathsToEntity = new HashMap<>();
    for (EntityWithPartitions ep : processedSourceEntities) {
      GoogleCloudDataplexV1Entity sourceEntity = ep.entity;
      String outputPath =
          outputPathProvider.outputPathFrom(sourceEntity.getDataPath(), outputBucket);
      if (allOutputDataPathsToEntity.containsKey(outputPath)) {
        continue; // Already created or verified an entity for this outputPath.
      }
      GoogleCloudDataplexV1Entity outputEntity = existingOutputDataPathToEntity.get(outputPath);
      if (outputEntity == null) {
        outputEntity = createNewOutputEntity(dataplex, options, sourceEntity, outputPath);
      } else {
        // Reload each entity 1 by 1 as the listEntities API never returns entity schema.
        GoogleCloudDataplexV1Entity richOutputEntity = loadEntity(dataplex, outputEntity.getName());
        DataplexUtils.verifyEntityIsUserManaged(richOutputEntity);
        updateEntitySchema(dataplex, options, sourceEntity, richOutputEntity);
      }
      allOutputDataPathsToEntity.put(outputPath, outputEntity);
    }

    // Create/update all partitions separately once the output entities have been created/verified.

    for (EntityWithPartitions ep : processedSourceEntities) {
      GoogleCloudDataplexV1Entity sourceEntity = ep.entity;
      String outputEntityPath =
          outputPathProvider.outputPathFrom(sourceEntity.getDataPath(), outputBucket);
      GoogleCloudDataplexV1Entity outputEntity = allOutputDataPathsToEntity.get(outputEntityPath);
      checkNotNull(
          outputEntity,
          String.format(
              "Entity for data path %s not found.", outputEntityPath)); // Shouldn't happen.
      for (GoogleCloudDataplexV1Partition sourcePartition : ep.getPartitions()) {
        String outputPartitionPath =
            outputPathProvider.outputPathFrom(sourcePartition.getLocation(), outputBucket);
        createOrUpdatePartition(dataplex, outputEntity, sourcePartition, outputPartitionPath);
      }
    }
  }

  private static GoogleCloudDataplexV1Entity loadEntity(DataplexClient dataplex, String entityName)
      throws IOException {
    GoogleCloudDataplexV1Entity richEntity = dataplex.getEntity(entityName);
    checkNotNull(richEntity, String.format("Could not load entity %s", entityName));
    return richEntity;
  }

  private static GoogleCloudDataplexV1Entity createNewOutputEntity(
      DataplexClient dataplex,
      FileFormatConversionOptions options,
      GoogleCloudDataplexV1Entity sourceEntity,
      String outputPath)
      throws IOException {

    String assetName = options.getOutputAsset();
    String zoneName = DataplexUtils.getZoneFromAsset(assetName);

    // The output entity will have the same schema as the source entity, but different asset,
    // dataPath, and file/compression format. We're also reusing the same entity ID because
    // createEntityWithUniqueId() will auto-generate a new one if the original ID already
    // exists (e.g. tableA => tableA_2).

    GoogleCloudDataplexV1Entity entity =
        DataplexUtils.createEntityWithUniqueId(
            dataplex,
            zoneName,
            new GoogleCloudDataplexV1Entity()
                .setId(sourceEntity.getId())
                .setAsset(DataplexUtils.getShortAssetNameFromAsset(assetName))
                .setDataPath(outputPath)
                .setType(EntityType.TABLE.name())
                .setSystem(StorageSystem.CLOUD_STORAGE.name())
                .setSchema(sourceEntity.getSchema().clone().setUserManaged(true))
                .setFormat(
                    DataplexUtils.storageFormat(
                        options.getOutputFileFormat(), options.getOutputFileCompression())),
            MAX_CREATE_ENTITY_ATTEMPTS);
    if (entity.getName() == null || entity.getName().isEmpty()) {
      throw new IOException("Dataplex returned an entity with no name: " + entity);
    }
    LOG.info(
        "Created a new entity for data path {} in zone {}: {}",
        outputPath,
        zoneName,
        entity.getName());
    return entity;
  }

  private static void updateEntitySchema(
      DataplexClient dataplex,
      FileFormatConversionOptions options,
      GoogleCloudDataplexV1Entity sourceEntity,
      GoogleCloudDataplexV1Entity richOutputEntity)
      throws IOException {

    GoogleCloudDataplexV1Entity entity =
        richOutputEntity
            .clone()
            .setType(EntityType.TABLE.name())
            .setSystem(StorageSystem.CLOUD_STORAGE.name())
            .setSchema(sourceEntity.getSchema().clone().setUserManaged(true))
            .setFormat(
                DataplexUtils.storageFormat(
                    options.getOutputFileFormat(), options.getOutputFileCompression()));

    try {
      dataplex.updateEntity(entity);
    } catch (IOException e) {
      throw new IOException(String.format("Error updating entity: %s", entity.getName()), e);
    }

    LOG.info("Updated metadata for entity: {}", entity.getName());
  }

  private static GoogleCloudDataplexV1Partition createOrUpdatePartition(
      DataplexClient dataplex,
      GoogleCloudDataplexV1Entity outputEntity,
      GoogleCloudDataplexV1Partition sourcePartition,
      String outputPartitionPath)
      throws IOException {

    GoogleCloudDataplexV1Partition outputPartition =
        new GoogleCloudDataplexV1Partition()
            .setLocation(outputPartitionPath)
            .setValues(sourcePartition.getValues());

    GoogleCloudDataplexV1Partition createdPartition;
    try {
      createdPartition = dataplex.createOrUpdatePartition(outputEntity.getName(), outputPartition);
      checkNotNull(createdPartition, "Got null in response to create partition.");
    } catch (Exception e) {
      throw new IOException(
          String.format(
              "Error creating partition for entity %s with values: %s.",
              outputEntity.getName(), outputPartition.getValues()),
          e);
    }
    LOG.info(
        "Created partition {} for entity {}.", createdPartition.getName(), outputEntity.getName());
    return createdPartition;
  }

  private static String shortenDataplexName(String name) {
    // adding a unique number just in case the entities or partition names will repeat
    return name.substring(name.lastIndexOf('/') + 1) + ' ' + conversionsCounter++;
  }

  private static String entityToFileSpec(GoogleCloudDataplexV1Entity entity) {
    if (!Strings.isNullOrEmpty(entity.getDataPathPattern())) {
      return entity.getDataPathPattern();
    }
    return addWildCard(entity.getDataPath());
  }

  private static String partitionToFileSpec(GoogleCloudDataplexV1Partition partition) {
    return addWildCard(partition.getLocation());
  }

  /** Return the output path that is similar to the input, but with a new bucket. */
  private static String gcsOutputPathFrom(String inputPath, String outputBucket) {
    return String.format("gs://%s/%s", outputBucket, GcsPath.fromUri(inputPath).getObject());
  }

  private static String addWildCard(String path) {
    return path.endsWith("/") ? path + "**" : path + "/**";
  }

  private static String ensurePathEndsWithSlash(String path) {
    return path.endsWith("/") ? path : path + '/';
  }

  private static String ensurePathStartsWithFSPrefix(String path) {
    return path.startsWith("gs://") || path.startsWith("/") ? path : "gs://" + path;
  }

  /** Example conversion: 1.json => 1.parquet; 1.abc => 1.abc.parquet. */
  private static String replaceInputExtensionWithOutputExtension(
      String path, FileFormatOptions outputFileFormat) {
    String inputFileExtension = path.substring(path.lastIndexOf('.'));
    if (EXPECTED_INPUT_FILES_EXTENSIONS.contains(inputFileExtension)) {
      return path.substring(0, path.length() - inputFileExtension.length())
          + outputFileFormat.getFileSuffix();
    } else {
      return path + outputFileFormat.getFileSuffix();
    }
  }

  private static String inputFilePathToOutputFilePath(
      OutputPathProvider outputPathProvider,
      String inputFilePath,
      String outputBucket,
      FileFormatOptions outputFileFormat) {
    return replaceInputExtensionWithOutputExtension(
        outputPathProvider.outputPathFrom(inputFilePath, outputBucket), outputFileFormat);
  }

  private static Stream<String> getFilesFromFilePattern(String pattern) throws IOException {
    return FileSystems.match(pattern, EmptyMatchTreatment.ALLOW).metadata().stream()
        .map(MatchResult.Metadata::resourceId)
        .map(ResourceId::toString);
  }

  private static ImmutableSet<String> getAllOutputFilePaths(String outputBucket)
      throws IOException {
    return getFilesFromFilePattern(addWildCard(ensurePathStartsWithFSPrefix(outputBucket)))
        .collect(ImmutableSet.toImmutableSet());
  }

  /** Convert the input file path to a new output file path. */
  @FunctionalInterface
  interface OutputPathProvider {
    String outputPathFrom(String inputPath, String outputBucket);
  }

  private static class ConvertFiles extends PTransform<PBegin, PDone> {
    /** The tag for the headers of the CSV if required. */
    private static final TupleTag<String> CSV_HEADERS = new TupleTag<String>() {};

    /** The tag for the lines of the CSV. */
    private static final TupleTag<String> CSV_LINES = new TupleTag<String>() {};

    private final GoogleCloudDataplexV1Entity entity;
    private final String inputFilePath;
    private final FileFormatOptions outputFileFormat;
    private final DataplexCompression outputFileCompression;
    private final String outputPath;

    protected ConvertFiles(
        GoogleCloudDataplexV1Entity entity,
        String inputFilePath,
        FileFormatConversionOptions options,
        String outputPath) {
      super();
      this.entity = entity;
      this.outputFileFormat = options.getOutputFileFormat();
      this.inputFilePath = inputFilePath;
      this.outputFileCompression = options.getOutputFileCompression();
      this.outputPath = outputPath;
    }

    @Override
    public PDone expand(PBegin input) {
      Schema schema = Schemas.dataplexSchemaToAvro(entity.getSchema());
      String serializedSchema = Schemas.serialize(schema);
      PCollection<GenericRecord> records;
      switch (InputFileFormat.valueOf(entity.getFormat().getFormat())) {
        case CSV:
          records =
              input
                  .apply("CSV", readCsvTransform(entity, inputFilePath))
                  .get(CSV_LINES)
                  .apply("ToGenRec", ParDo.of(csvToGenericRecordFn(entity, serializedSchema)))
                  .setCoder(AvroCoder.of(GenericRecord.class, schema));
          break;
        case JSON:
          records =
              input
                  .apply("Json", readJsonTransform(inputFilePath))
                  .apply("ToGenRec", ParDo.of(jsonToGenericRecordFn(serializedSchema)))
                  .setCoder(AvroCoder.of(GenericRecord.class, schema));
          break;
        case PARQUET:
          records =
              input.apply(
                  "Parquet",
                  ParquetConverters.ReadParquetFile.newBuilder()
                      .withInputFileSpec(inputFilePath)
                      .withSerializedSchema(serializedSchema)
                      .build());
          break;
        case AVRO:
          records =
              input.apply(
                  "Avro",
                  AvroConverters.ReadAvroFile.newBuilder()
                      .withInputFileSpec(inputFilePath)
                      .withSerializedSchema(serializedSchema)
                      .build());
          break;
        default:
          throw new IllegalArgumentException(
              "Unexpected input file format: " + entity.getFormat().getFormat());
      }

      Sink<GenericRecord> sink;
      switch (outputFileFormat) {
        case PARQUET:
          sink =
              ParquetIO.sink(schema).withCompressionCodec(outputFileCompression.getParquetCodec());
          break;
        case AVRO:
          sink =
              new AvroSinkWithJodaDatesConversion<GenericRecord>(schema)
                  .withCodec(outputFileCompression.getAvroCodec());
          break;
        default:
          throw new UnsupportedOperationException(
              "Output format is not implemented: " + outputFileFormat);
      }

      String outputFileName =
          replaceInputExtensionWithOutputExtension(
              inputFilePath.substring(inputFilePath.lastIndexOf('/') + 1), outputFileFormat);

      records.apply(
          "Write",
          FileIO.<GenericRecord>write()
              .via(sink)
              .to(ensurePathEndsWithSlash(outputPath))
              .withNaming((window, pane, numShards, shardIndex, compression) -> outputFileName)
              .withNumShards(1)); // Must be 1 as we can only have 1 file per Dataplex partition.

      return PDone.in(input.getPipeline());
    }

    private static CsvConverters.ReadCsv readCsvTransform(
        GoogleCloudDataplexV1Entity entity, String inputFileSpec) {
      CsvConverters.ReadCsv.Builder builder =
          CsvConverters.ReadCsv.newBuilder()
              .setCsvFormat("Default")
              .setHeaderTag(CSV_HEADERS)
              .setLineTag(CSV_LINES);
      GoogleCloudDataplexV1StorageFormatCsvOptions csvOptions = entity.getFormat().getCsv();
      if (csvOptions == null) {
        return builder
            .setInputFileSpec(inputFileSpec)
            .setFileEncoding("UTF-8")
            .setHasHeaders(false)
            .setDelimiter(",")
            .build();
      }
      if (csvOptions.getHeaderRows() != null && csvOptions.getHeaderRows() > 1) {
        // TODO(olegsa): consider updating CsvConverters.ReadCsv to support multiple headers rows
        //  (probably rare case)
        throw new UnsupportedOperationException(
            "CSV conversion currently doesn't support files with multiple headers.");
      }
      // TODO(olegsa): consider updating CsvConverters.ReadCsv to support
      //  GoogleCloudDataplexV1StorageFormatCsvOptions.getQuote (probably rare case)
      return builder
          .setInputFileSpec(inputFileSpec)
          .setFileEncoding(csvOptions.getEncoding() != null ? csvOptions.getEncoding() : "UTF-8")
          .setHasHeaders(csvOptions.getHeaderRows() != null && csvOptions.getHeaderRows() > 0)
          .setDelimiter(csvOptions.getDelimiter() != null ? csvOptions.getDelimiter() : ",")
          .build();
    }

    private static CsvConverters.StringToGenericRecordFn csvToGenericRecordFn(
        GoogleCloudDataplexV1Entity entity, String serializedSchema) {
      GoogleCloudDataplexV1StorageFormatCsvOptions csvOptions = entity.getFormat().getCsv();
      return new CsvConverters.StringToGenericRecordFn(
              csvOptions != null && csvOptions.getDelimiter() != null
                  ? csvOptions.getDelimiter()
                  : ",")
          .withSerializedSchema(serializedSchema);
    }

    private static JsonConverters.ReadJson readJsonTransform(String inputFileSpec) {
      return JsonConverters.ReadJson.newBuilder().setInputFileSpec(inputFileSpec).build();
    }

    private static JsonConverters.StringToGenericRecordFn jsonToGenericRecordFn(
        String serializedSchema) {
      return new JsonConverters.StringToGenericRecordFn(serializedSchema);
    }
  }

  @VisibleForTesting
  static class EntityWithPartitions {
    private final GoogleCloudDataplexV1Entity entity;
    private final List<GoogleCloudDataplexV1Partition> partitions;

    public EntityWithPartitions(GoogleCloudDataplexV1Entity entity) {
      this(entity, Collections.emptyList());
    }

    public EntityWithPartitions(
        GoogleCloudDataplexV1Entity entity, List<GoogleCloudDataplexV1Partition> partitions) {
      this.entity = entity;
      this.partitions = partitions;
    }

    public GoogleCloudDataplexV1Entity getEntity() {
      return entity;
    }

    public List<GoogleCloudDataplexV1Partition> getPartitions() {
      return partitions;
    }
  }
}
