package com.google.cloud.syndeo.service;

import com.google.cloud.syndeo.service.v1.SyndeoServiceV1;
import com.google.cloud.syndeo.service.v1.TransformServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.expansion.service.ExpansionService;

public class SyndeoTransformService extends TransformServiceGrpc.TransformServiceImplBase {
    public static void main(String[] args) {
        System.out.println("OIOI DUDE!");
//        ExpansionService beamService = new ExpansionService(args);
    }

    @Override
    public void listTransforms(
            SyndeoServiceV1.ListTransformRequest request,
            StreamObserver<SyndeoServiceV1.ListTransformResponse> responseObserver) {
    }

    @Override
    public void validateTransform(
            SyndeoServiceV1.ValidateTransformRequest request,
            StreamObserver<SyndeoServiceV1.ValidateTransformResponse> responseObserver) {
    }
}
