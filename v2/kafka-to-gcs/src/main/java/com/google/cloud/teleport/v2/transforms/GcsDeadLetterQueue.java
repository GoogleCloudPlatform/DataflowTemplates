package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.joda.time.Duration;

@AutoValue
public abstract class GcsDeadLetterQueue extends PTransform<PCollection<BadRecord>, POutput> {
    private final int numShards = 3;
    private final String outputFileNamePrefix = "DLQ";

    public abstract String dlqOutputDirectory();

    @AutoValue.Builder
    public abstract static class GcsDeadLetterQueueBuilder {
        public abstract GcsDeadLetterQueueBuilder setDlqOutputDirectory(String value);
        abstract GcsDeadLetterQueue autoBuild();
        public GcsDeadLetterQueue build() {
            return autoBuild();
        }
    }

    @Override
    public POutput expand(PCollection<BadRecord> input) {
        return input
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply(
                ParDo.of(new DlqUtils.GetPayLoadStringFromBadRecord()))
                .apply(ParDo.of(new DoFn<KV<String, String>, String>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, String> kv, OutputReceiver<String> o) {
                        o.output(kv.getValue());
                    }
                })
        ).apply(TextIO.write()
                .withWindowedWrites()
                .withNumShards(numShards)
                .to(
                        WindowedFilenamePolicy.writeWindowedFiles()
                                .withOutputFilenamePrefix(outputFileNamePrefix)
                                .withSuffix(".json")
                                .withShardTemplate(WriteToGCSUtility.SHARD_TEMPLATE)
                                .withOutputDirectory(dlqOutputDirectory())
                )
                .withTempDirectory(
                        FileBasedSink.convertToFileResourceIfPossible(dlqOutputDirectory() + "/temp")
                )
        );
    }

    public static GcsDeadLetterQueueBuilder newBuilder() {
        return new AutoValue_GcsDeadLetterQueue.Builder();
    }
}
