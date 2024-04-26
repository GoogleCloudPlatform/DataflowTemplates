package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class JsonWriteTransform extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, POutput> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroWriteTransform.class);
    public abstract String outputDirectory();
    public abstract Integer numShards();

    public abstract String outputFilenamePrefix();
    public abstract String windowDuration();
    public abstract String tempDirectory();

    public static JsonWriteTransformBuilder newBuilder() {

        return new AutoValue_JsonWriteTransform.Builder();
    }

    @AutoValue.Builder
    public abstract static class JsonWriteTransformBuilder {
        abstract JsonWriteTransform autoBuild();

        public abstract JsonWriteTransformBuilder setNumShards(Integer numShards);
        public abstract JsonWriteTransformBuilder setOutputDirectory(String outputDirectory);

        public abstract JsonWriteTransformBuilder setOutputFilenamePrefix(String outputFilenamePrefix);

        public abstract JsonWriteTransformBuilder setWindowDuration(String windowDuration);
        public abstract JsonWriteTransformBuilder setTempDirectory(String tempDirectory);

        public JsonWriteTransform build() {
            return autoBuild();
        }
    }

    public static class ConvertBytesToString extends DoFn<KafkaRecord<byte[], byte[]>, String> {
        @ProcessElement
        public void processElement(@Element KafkaRecord<byte[], byte[]>kafkaRecord, OutputReceiver<String> o) {
            // TODO: Add DLQ support.
            StringDeserializer deserializer = new StringDeserializer();
            o.output(
                    deserializer.deserialize(kafkaRecord.getTopic(), kafkaRecord.getKV().getValue())
            );
        }
    }
    public POutput expand(PCollection<KafkaRecord<byte[], byte[]>> records) {
        return records.apply(ParDo.of(new ConvertBytesToString()))
                // TextIO needs windowing for unbounded PCollections.
                .apply(Window.into(FixedWindows.of(DurationUtils.parseDuration(windowDuration()))))
                .apply("Write using TextIO", TextIO.write()
                        .withWindowedWrites()
                        .withNumShards(numShards())
                        .to(WindowedFilenamePolicy.writeWindowedFiles()
                                .withOutputDirectory(outputDirectory())
                                .withOutputFilenamePrefix(outputFilenamePrefix())
                                .withSuffix(".json")
                                .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                        )
                        .withTempDirectory(FileBasedSink.convertToFileResourceIfPossible(tempDirectory()))
                );
    }
}
