
package com.google.cloud.teleport.it.datastream;

import com.google.cloud.datastream.v1.*;

import java.util.concurrent.ExecutionException;

public interface DatastreamResourceManager {

    ConnectionProfile createConnectionProfile(String connectionProfileId, String displayName, String hostname, String password) throws ExecutionException, InterruptedException;

//    Stream createStream(String stringId, String requestID);

//    void cleanUpAll();

}
