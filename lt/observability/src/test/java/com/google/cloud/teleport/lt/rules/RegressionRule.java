package com.google.cloud.teleport.lt.rules;

import com.google.cloud.teleport.lt.PerfResultRow;
import com.google.cloud.teleport.lt.Violation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface RegressionRule {

  public static final Map<String, RegressionRule> ruleMap = new HashMap<>() {{
    put("TotalRunTimeAnomaly", TotalRunTimeAnomaly.getInstance());
  }};

  List<Violation> getViolations(PerfResultRow row, List<PerfResultRow> previousRows);
}
