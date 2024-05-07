package com.google.cloud.teleport.v2.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.KV;

public class DlqUtils {
    public static class GetPayLoadStringFromBadRecord extends DoFn<BadRecord, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element BadRecord badRecord,
                                   OutputReceiver<KV<String, String>> receiver) {
            String record = badRecord.getRecord().getHumanReadableJsonRecord();
            receiver.output(KV.of(badRecord.getFailure().getException(), record));
        }
    }
}
