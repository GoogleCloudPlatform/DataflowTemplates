package com.google.cloud.teleport.it.datastream;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.DatastreamSettings;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class DatastreamResourceManagerTest {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        Credentials credentials = ServiceAccountCredentials.fromStream(new FileInputStream("/Users/titodo/credentials/credentials.json")) ;
        DatastreamSettings datastreamSettings = DatastreamSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
        DatastreamClient datastreamClient = DatastreamClient.create(datastreamSettings);


        DefaultDatastreamResourceManager rm = new DefaultDatastreamResourceManager(datastreamClient, "us-central1", "datastream-rm");
        rm.createConnectionProfile("jc-connection-profile", "JC Connection Profile", "34.172.27.69", "password");

    }
}
