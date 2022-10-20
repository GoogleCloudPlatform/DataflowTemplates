/*
 * Copyright (C) 2018 Google LLC
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

package com.google.cloud.syndeo.service;

import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

import com.google.cloud.syndeo.service.v1.SyndeoServiceV1;
import io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class SyndeoTransformServiceTest {

    @Test
    public void testTransformsAreLoaded() {
        SyndeoTransformService.loadSchemaTransforms();
        assertEquals(9, SyndeoTransformService.TRANSFORM_PROVIDERS.size());
        assertThat(SyndeoTransformService.TRANSFORM_PROVIDERS.keySet(), hasItems(
                "kafka:read", "schemaIO:bigquery:v1:write"));
    }

    class InMemoryStreamObserver<T> implements StreamObserver<T> {
        public final List<T> elements = new ArrayList<>();

        @Override
        public void onNext(T t) {
            elements.add(t);
        }

        @Override
        public void onError(Throwable throwable) {}

        @Override
        public void onCompleted() {}
    }

    @Test
    public void testTransformsAreQueried() {
        SyndeoTransformService ts = new SyndeoTransformService();
        SyndeoTransformService.loadSchemaTransforms();
        InMemoryStreamObserver<SyndeoServiceV1.ListTransformResponse> observer = new InMemoryStreamObserver<>();

        ts.listTransforms(SyndeoServiceV1.ListTransformRequest.newBuilder().build(), observer);
    }
}