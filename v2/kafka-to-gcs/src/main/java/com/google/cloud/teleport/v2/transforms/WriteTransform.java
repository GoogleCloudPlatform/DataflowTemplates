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

package com.google.cloud.teleport.v2.transforms;


import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.options.KafkaToGCSOptions;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

@AutoValue
public abstract class WriteTransform
        extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, POutput> {

    public static WriteTransformBuilder newBuilder() {
        return new AutoValue_WriteTransform.Builder();
    }
    public abstract KafkaToGCSOptions options();

    @Override
    public POutput expand(PCollection<KafkaRecord<byte[], byte[]>> record) {
        POutput pOutput = null;
        WriteToGCSUtility.FileFormat outputFileFormat =
                WriteToGCSUtility.FileFormat.valueOf(options().getOutputFileFormat().toUpperCase());

        switch (outputFileFormat) {
            case TEXT:
                pOutput = record.apply(JsonWriteTransform
                        .newBuilder()
                        .setOutputFilenamePrefix(options().getOutputFilenamePrefix())
                        .setNumShards(options().getNumShards())
                        .setOutputDirectory(options().getOutputDirectory())
                        .setWindowDuration(options().getWindowDuration())
                        .setTempDirectory(options().getTempLocation())
                        .build());
                break;
            case AVRO:
                 pOutput = record.apply(AvroWriteTransform
                         .newBuilder()
                         .setOutputDirectory(options().getOutputDirectory())
                         .setNumShards(options().getNumShards())
                         .setMessageFormat(options().getMessageFormat())
                         .setSchemaRegistryURL(options().getSchemaRegistryURL())
                         .setSchemaPath(options().getSchemaPath())
                         .setWindowDuration(options().getWindowDuration())
                         .build()
                 );
                break;
            case PARQUET:
                // TODO: Add more info to the error string.
                throw new UnsupportedOperationException("Unsupported output format");
        }
        return pOutput;
    }
    @AutoValue.Builder
    public abstract static class WriteTransformBuilder {
        public abstract WriteTransformBuilder setOptions(KafkaToGCSOptions options);

        abstract KafkaToGCSOptions options();

        abstract WriteTransform autoBuild();

        public WriteTransform build() {
            return autoBuild();
        }
    }
}
