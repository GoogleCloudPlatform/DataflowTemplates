package com.google.cloud.teleport.v2.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;

public abstract class JSONTransformer<T>
        extends PTransform<PCollection<FailsafeElement<T, String>>, PCollectionTuple> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public abstract TupleTag<FailsafeElement<T, String>> successTag();

    public abstract TupleTag<FailsafeElement<T, String>> failureTag();

    public static <T> Builder<T> newBuilder() {
        return new AutoValueJSONTransformer.Builder<>();
    }

    public abstract static class Builder<T> {
        public abstract Builder<T> setSuccessTag(TupleTag<FailsafeElement<T, String>> successTag);

        public abstract Builder<T> setFailureTag(TupleTag<FailsafeElement<T, String>> failureTag);

        public abstract JSONTransformer<T> build();
    }
    ;

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<T, String>> elements) {
        return elements.apply(
                "ProcessEvents",
                ParDo.of(
                                new DoFn<FailsafeElement<T, String>, FailsafeElement<T, String>>() {

                                    private List<String> keysToSkip;

                                    @Setup
                                    public void setup() {
                                        keysToSkip = List.of("event_id", "event_timestamp", "event_name");
                                    }

                                    @ProcessElement
                                    public void processElement(@Element FailsafeElement<T, String> event, MultiOutputReceiver out) {
                                        String payloadStr = event.getPayload();
                                        try {
                                            String transformedJson =
                                                    JSONTransformer.transformJson(payloadStr, keysToSkip);
                                            if (!Strings.isNullOrEmpty(transformedJson)) {
                                                out.get(successTag()).output(
                                                        FailsafeElement.of(event.getOriginalPayload(), transformedJson));
                                            }
                                        } catch (Throwable e) {
                                            out.get(failureTag()).output(
                                                    FailsafeElement.of(event)
                                                            .setErrorMessage(e.getMessage())
                                                            .setStacktrace(Throwables.getStackTraceAsString(e)));
                                        }
                                    }
                                })
                        .withOutputTags(successTag(), TupleTagList.of(failureTag())));
    }

    public static JsonNode transformJson(JsonNode jsonNode, List<String> keysToSkip) {
        ObjectNode transformedJson = objectMapper.createObjectNode();

        jsonNode
                .fieldNames()
                .forEachRemaining(
                        key -> {
                            JsonNode value = jsonNode.get(key);

                            if (!keysToSkip.contains(key)) {
                                if (value.isTextual()) {
                                    // Directly use the text value without converting it to a JSON string
                                    value = objectMapper.valueToTree(value.asText());
                                } else {
                                    value = objectMapper.valueToTree(value.toString());
                                }
                            }

                            if (key.contains("timestamp")) {
                                long timestampValue = value.asLong();
                                int length = String.valueOf(timestampValue).length();
                                switch (length) {
                                    case 13:
                                        value =
                                                objectMapper.valueToTree(Instant.ofEpochMilli(timestampValue).toString());
                                        break;
                                    case 10:
                                        value =
                                                objectMapper.valueToTree(Instant.ofEpochSecond(timestampValue).toString());
                                        break;
                                    case 16:
                                        value =
                                                objectMapper.valueToTree(
                                                        Instant.ofEpochSecond(
                                                                        timestampValue / 1_000_000, (timestampValue % 1_000_000) * 1000)
                                                                .toString());
                                        break;
                                }
                            }

                            if ("null".equals(value.asText())) {
                                value = objectMapper.nullNode();
                            }

                            transformedJson.set(key, value);
                        });

        String processingTimestampAsString = Instant.ofEpochMilli(System.currentTimeMillis()).toString();
        transformedJson.put("processing_timestamp", processingTimestampAsString);

        return transformedJson;
    }

    public static String transformJson(String jsonString, List<String> keysToSkip)
            throws IOException {
        JsonNode jsonNode = objectMapper.readTree(jsonString);
        JsonNode transformedJson = transformJson(jsonNode, keysToSkip);
        return objectMapper.writeValueAsString(transformedJson);
    }
}
