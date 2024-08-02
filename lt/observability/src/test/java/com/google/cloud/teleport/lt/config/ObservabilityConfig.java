package com.google.cloud.teleport.lt.config;

import com.google.cloud.teleport.lt.rules.RegressionRule;
import com.google.cloud.teleport.lt.rules.TotalRunTimeAnomaly;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;

public class ObservabilityConfig {

  String templateName;
  String testName;
  int numPreviousRunsToObserve;
  List<RegressionRule> regressionRules;

  public ObservabilityConfig(String templateName, String testName, int numPreviousRunsToObserve,
      List<RegressionRule> regressionRules) {
    this.templateName = templateName;
    this.testName = testName;
    this.numPreviousRunsToObserve = numPreviousRunsToObserve;
    this.regressionRules = regressionRules;
  }

  public String getTemplateName() {
    return templateName;
  }

  public String getTestName() {
    return testName;
  }

  public int getNumPreviousRunsToObserve() {
    return numPreviousRunsToObserve;
  }

  public List<RegressionRule> getRegressionRules() {
    return regressionRules;
  }

  private static final String configFileName = "lt-observability-config.json";

  public static List<ObservabilityConfig> parseConfig() throws IOException {
    List<ObservabilityConfig> configs = new ArrayList<>();
    try (InputStream stream = ObservabilityConfig.class.getClassLoader().getResourceAsStream(configFileName)) {
      String result = IOUtils.toString(stream, StandardCharsets.UTF_8);
      JsonParser parser = new JsonParser();
      JsonObject configJson = parser.parseString(result).getAsJsonObject();

      for (Map.Entry<String, JsonElement> entry : configJson.entrySet()) {
        String templateName = entry.getKey();
        JsonObject value = entry.getValue().getAsJsonObject();
        for (Map.Entry<String, JsonElement> entry1 : configJson.entrySet()) {
          String testName = entry1.getKey();
          int numPreviousRunsToObserve = entry1.getValue().getAsJsonObject()
              .get("numPreviousRunsToObserve").getAsInt();
          List<RegressionRule> rules = new ArrayList<>();
          for (JsonElement rule : entry1.getValue().getAsJsonObject().get("regressionRules")
              .getAsJsonArray()) {
            rules.add(RegressionRule.ruleMap.get(rule.getAsString()));
          }
          configs.add(
              new ObservabilityConfig(templateName, testName, numPreviousRunsToObserve, rules));
        }
      }
    }

    return configs;
  }
}
