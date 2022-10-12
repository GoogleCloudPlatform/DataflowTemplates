package com.google.cloud.teleport.it.datastream;

import com.google.cloud.datastream.v1.*;

import java.util.concurrent.ExecutionException;

public class DefaultDatastreamResourceManager implements DatastreamResourceManager {

    private final DatastreamClient datastreamClient;
    private final String location;
    private final String projectId;

    public DefaultDatastreamResourceManager(DatastreamClient datastreamClient, String location, String projectId) {
        this.datastreamClient = datastreamClient;
        this.location = location;
        this.projectId = projectId;
    }

    @Override
    public ConnectionProfile createConnectionProfile(String connectionProfileId, String displayName, String hostname, String password) throws ExecutionException, InterruptedException {

        MysqlProfile.Builder setMysqlProfileBuilder = MysqlProfile.newBuilder()
                .setHostname(hostname)
                .setPassword(password)
                .setPort(3306);

        ConnectionProfile connectionProfile = ConnectionProfile.newBuilder()
                .setDisplayName(displayName)
                .setMysqlProfile(setMysqlProfileBuilder)
                .build();

        CreateConnectionProfileRequest request = CreateConnectionProfileRequest.newBuilder()
                .setParent(LocationName.of(projectId, location).toString())
                .setConnectionProfile(connectionProfile)
                .setConnectionProfileId(connectionProfileId)
                .setValidateOnly(true)
                .setForce(true)
                .build();

        return datastreamClient
                .createConnectionProfileAsync(request)
                .get();
    }
}
