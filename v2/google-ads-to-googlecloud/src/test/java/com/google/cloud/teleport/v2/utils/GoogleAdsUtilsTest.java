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
package com.google.cloud.teleport.v2.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GoogleAdsUtilsTest {
  // Example queries from
  // https://developers.google.com/google-ads/api/docs/query/cookbook.
  private static final String[] VALID_QUERIES = {
    "SELECT campaign.name,\n"
        + "  campaign_budget.amount_micros,\n"
        + "  campaign.status,\n"
        + "  campaign.optimization_score,\n"
        + "  campaign.advertising_channel_type,\n"
        + "  metrics.clicks,\n"
        + "  metrics.impressions,\n"
        + "  metrics.ctr,\n"
        + "  metrics.average_cpc,\n"
        + "  metrics.cost_micros,\n"
        + "  campaign.bidding_strategy_type\n"
        + "FROM campaign\n"
        + "WHERE segments.date DURING LAST_7_DAYS\n"
        + "  AND campaign.status != 'REMOVED'",
    "SELECT ad_group.name,\n"
        + "  campaign.name,\n"
        + "  ad_group.status,\n"
        + "  ad_group.type,\n"
        + "  metrics.clicks,\n"
        + "  metrics.impressions,\n"
        + "  metrics.ctr,\n"
        + "  metrics.average_cpc,\n"
        + "  metrics.cost_micros\n"
        + "FROM ad_group\n"
        + "WHERE segments.date DURING LAST_7_DAYS\n"
        + "  AND ad_group.status != 'REMOVED'",
    "SELECT ad_group_ad.ad.expanded_text_ad.headline_part1,\n"
        + "  ad_group_ad.ad.expanded_text_ad.headline_part2,\n"
        + "  ad_group_ad.ad.expanded_text_ad.headline_part3,\n"
        + "  ad_group_ad.ad.final_urls,\n"
        + "  ad_group_ad.ad.expanded_text_ad.description,\n"
        + "  ad_group_ad.ad.expanded_text_ad.description2,\n"
        + "  campaign.name,\n"
        + "  ad_group.name,\n"
        + "  ad_group_ad.policy_summary.approval_status,\n"
        + "  ad_group_ad.ad.type,\n"
        + "  metrics.clicks,\n"
        + "  metrics.impressions,\n"
        + "  metrics.ctr,\n"
        + "  metrics.average_cpc,\n"
        + "  metrics.cost_micros\n"
        + "FROM ad_group_ad\n"
        + "WHERE segments.date DURING LAST_7_DAYS\n"
        + "  AND ad_group_ad.status != 'REMOVED'",
    "SELECT ad_group_criterion.keyword.text,\n"
        + "  campaign.name,\n"
        + "  ad_group.name,\n"
        + "  ad_group_criterion.system_serving_status,\n"
        + "  ad_group_criterion.keyword.match_type,\n"
        + "  ad_group_criterion.approval_status,\n"
        + "  ad_group_criterion.final_urls,\n"
        + "  metrics.clicks,\n"
        + "  metrics.impressions,\n"
        + "  metrics.ctr,\n"
        + "  metrics.average_cpc,\n"
        + "  metrics.cost_micros\n"
        + "FROM keyword_view\n"
        + "WHERE segments.date DURING LAST_7_DAYS\n"
        + "  AND ad_group_criterion.status != 'REMOVED'",
    "SELECT search_term_view.search_term,\n"
        + "  segments.keyword.info.match_type,\n"
        + "  search_term_view.status,\n"
        + "  campaign.name,\n"
        + "  ad_group.name,\n"
        + "  metrics.clicks,\n"
        + "  metrics.impressions,\n"
        + "  metrics.ctr,\n"
        + "  metrics.average_cpc,\n"
        + "  metrics.cost_micros,\n"
        + "  campaign.advertising_channel_type\n"
        + "FROM search_term_view\n"
        + "WHERE segments.date DURING LAST_7_DAYS",
    "SELECT ad_group_criterion.resource_name,\n"
        + "  ad_group_criterion.type,\n"
        + "  campaign.name,\n"
        + "  ad_group.name,\n"
        + "  ad_group_criterion.system_serving_status,\n"
        + "  ad_group_criterion.bid_modifier,\n"
        + "  metrics.clicks,\n"
        + "  metrics.impressions,\n"
        + "  metrics.ctr,\n"
        + "  metrics.average_cpc,\n"
        + "  metrics.cost_micros,\n"
        + "  campaign.advertising_channel_type\n"
        + "FROM ad_group_audience_view\n"
        + "WHERE segments.date DURING LAST_7_DAYS",
    "SELECT ad_group_criterion.age_range.type,\n"
        + "  campaign.name,\n"
        + "  ad_group.name,\n"
        + "  ad_group_criterion.system_serving_status,\n"
        + "  ad_group_criterion.bid_modifier,\n"
        + "  metrics.clicks,\n"
        + "  metrics.impressions,\n"
        + "  metrics.ctr,\n"
        + "  metrics.average_cpc,\n"
        + "  metrics.cost_micros,\n"
        + "  campaign.advertising_channel_type\n"
        + "FROM age_range_view\n"
        + "WHERE segments.date DURING LAST_7_DAYS",
    "SELECT ad_group_criterion.gender.type,\n"
        + "  campaign.name,\n"
        + "  ad_group.name,\n"
        + "  ad_group_criterion.system_serving_status,\n"
        + "  ad_group_criterion.bid_modifier,\n"
        + "  metrics.clicks,\n"
        + "  metrics.impressions,\n"
        + "  metrics.ctr,\n"
        + "  metrics.average_cpc,\n"
        + "  metrics.cost_micros,\n"
        + "  campaign.advertising_channel_type\n"
        + "FROM gender_view\n"
        + "WHERE segments.date DURING LAST_7_DAYS",
    "SELECT campaign_criterion.location.geo_target_constant,\n"
        + "  campaign.name,\n"
        + "  campaign_criterion.bid_modifier,\n"
        + "  metrics.clicks,\n"
        + "  metrics.impressions,\n"
        + "  metrics.ctr,\n"
        + "  metrics.average_cpc,\n"
        + "  metrics.cost_micros\n"
        + "FROM location_view\n"
        + "WHERE segments.date DURING LAST_7_DAYS\n"
        + "  AND campaign_criterion.status != 'REMOVED'",
    "SELECT geo_target_constant.canonical_name,\n"
        + "  geo_target_constant.country_code,\n"
        + "  geo_target_constant.id,\n"
        + "  geo_target_constant.name,\n"
        + "  geo_target_constant.status,\n"
        + "  geo_target_constant.target_type\n"
        + "FROM geo_target_constant\n"
        + "WHERE geo_target_constant.resource_name = 'geoTargetConstants/1014044'",
    "SELECT geo_target_constant.canonical_name,\n"
        + "  geo_target_constant.country_code,\n"
        + "  geo_target_constant.id,\n"
        + "  geo_target_constant.name,\n"
        + "  geo_target_constant.status,\n"
        + "  geo_target_constant.target_type\n"
        + "FROM geo_target_constant\n"
        + "WHERE geo_target_constant.country_code = 'US'\n"
        + "  AND geo_target_constant.target_type = 'City'\n"
        + "  AND geo_target_constant.name = 'Mountain View'\n"
        + "  AND geo_target_constant.status = 'ENABLED'"
  };

  private static final TableSchema[] VALID_QUERIES_SCHEMAS = {
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_budget_amount_micros")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_optimization_score")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_advertising_channel_type")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_clicks")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_impressions")
                    .setType("INTEGER"),
                new TableFieldSchema().setMode("NULLABLE").setName("metrics_ctr").setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_average_cpc")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_cost_micros")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_bidding_strategy_type")
                    .setType("STRING"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_type")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_clicks")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_impressions")
                    .setType("INTEGER"),
                new TableFieldSchema().setMode("NULLABLE").setName("metrics_ctr").setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_average_cpc")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_cost_micros")
                    .setType("INTEGER"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_ad_ad_expanded_text_ad_headline_part1")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_ad_ad_expanded_text_ad_headline_part2")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_ad_ad_expanded_text_ad_headline_part3")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("REPEATED")
                    .setName("ad_group_ad_ad_final_urls")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_ad_ad_expanded_text_ad_description")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_ad_ad_expanded_text_ad_description2")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_ad_policy_summary_approval_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_ad_ad_type")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_clicks")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_impressions")
                    .setType("INTEGER"),
                new TableFieldSchema().setMode("NULLABLE").setName("metrics_ctr").setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_average_cpc")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_cost_micros")
                    .setType("INTEGER"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_keyword_text")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_system_serving_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_keyword_match_type")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_approval_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("REPEATED")
                    .setName("ad_group_criterion_final_urls")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_clicks")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_impressions")
                    .setType("INTEGER"),
                new TableFieldSchema().setMode("NULLABLE").setName("metrics_ctr").setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_average_cpc")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_cost_micros")
                    .setType("INTEGER"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("search_term_view_search_term")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("segments_keyword_info_match_type")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("search_term_view_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_clicks")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_impressions")
                    .setType("INTEGER"),
                new TableFieldSchema().setMode("NULLABLE").setName("metrics_ctr").setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_average_cpc")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_cost_micros")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_advertising_channel_type")
                    .setType("STRING"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_resource_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_type")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_system_serving_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_bid_modifier")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_clicks")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_impressions")
                    .setType("INTEGER"),
                new TableFieldSchema().setMode("NULLABLE").setName("metrics_ctr").setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_average_cpc")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_cost_micros")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_advertising_channel_type")
                    .setType("STRING"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_age_range_type")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_system_serving_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_bid_modifier")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_clicks")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_impressions")
                    .setType("INTEGER"),
                new TableFieldSchema().setMode("NULLABLE").setName("metrics_ctr").setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_average_cpc")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_cost_micros")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_advertising_channel_type")
                    .setType("STRING"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_gender_type")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_system_serving_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("ad_group_criterion_bid_modifier")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_clicks")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_impressions")
                    .setType("INTEGER"),
                new TableFieldSchema().setMode("NULLABLE").setName("metrics_ctr").setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_average_cpc")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_cost_micros")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_advertising_channel_type")
                    .setType("STRING"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_criterion_location_geo_target_constant")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("campaign_criterion_bid_modifier")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_clicks")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_impressions")
                    .setType("INTEGER"),
                new TableFieldSchema().setMode("NULLABLE").setName("metrics_ctr").setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_average_cpc")
                    .setType("FLOAT"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("metrics_cost_micros")
                    .setType("INTEGER"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_canonical_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_country_code")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_id")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_target_type")
                    .setType("STRING"))),
    new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_canonical_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_country_code")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_id")
                    .setType("INTEGER"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_status")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setMode("NULLABLE")
                    .setName("geo_target_constant_target_type")
                    .setType("STRING")))
  };

  // Anything that doesn't start with SELECT FieldName (, FieldName)* FROM ResourceName is invalid.
  // See https://developers.google.com/google-ads/api/docs/query/grammar.
  private static final String[] INVALID_QUERIES = {
    "SELECT * FROM campaign",
    "SELECT campaign_name + 1 FROM campaign",
    "SELECT 1 AS campaign_name FROM campaign",
    "SELECT campaign_name",
    "SELECT campaign_name, COUNT(campaign_name) FROM campaign"
  };

  @Test
  public void testCreateBigQuerySchemaWithValidQueries() {
    for (int i = 0; i < VALID_QUERIES.length; ++i) {
      assertEquals(GoogleAdsUtils.createBigQuerySchema(VALID_QUERIES[i]), VALID_QUERIES_SCHEMAS[i]);
    }
  }

  @Test
  public void testCreateBigQuerySchemaWithInvalidQueries() {
    for (String invalidQuery : INVALID_QUERIES) {
      assertThrows(
          IllegalArgumentException.class, () -> GoogleAdsUtils.createBigQuerySchema(invalidQuery));
    }
  }
}
