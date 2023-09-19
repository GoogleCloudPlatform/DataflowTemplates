/*
 * Copyright (C) 2023 Google LLC
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

import com.google.ads.googleads.v14.enums.CampaignStatusEnum.CampaignStatus;
import com.google.ads.googleads.v14.resources.Campaign;
import com.google.ads.googleads.v14.services.GoogleAdsRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleAdsRowToReportRowJsonFnTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testRowToJsonWithValidInput() {
    String query =
        "SELECT campaign.id, campaign.name, campaign.start_date, campaign.status FROM campaign";
    GoogleAdsRow row =
        GoogleAdsRow.newBuilder()
            .setCampaign(
                Campaign.newBuilder()
                    .setId(1234567890L)
                    .setName("foo")
                    .setStartDate("1970-01-01")
                    .setStatus(CampaignStatus.ENABLED))
            .build();

    PCollection<String> actual =
        pipeline
            .apply("Create", Create.of(row))
            .apply(ParDo.of(new GoogleAdsRowToReportRowJsonFn(query)));

    String expected =
        "{\"campaign_id\":1234567890,\"campaign_name\":\"foo\",\"campaign_start_date\":\"1970-01-01\",\"campaign_status\":\"ENABLED\"}";
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run();
  }
}
