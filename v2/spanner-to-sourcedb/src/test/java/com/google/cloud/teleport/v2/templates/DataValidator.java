package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

public class DataValidator {

  public static void main(String[] args) throws Exception {
    System.out.println("Starting data validation...");
    
    // Connect to Spanner
    SpannerOptions options = SpannerOptions.newBuilder().setProjectId("span-cloud-migrations-testing").build();
    Spanner spanner = options.getService();
    DatabaseId db = DatabaseId.of("span-cloud-migrations-testing", "pratick-load-test", "poc-source");
    DatabaseClient spannerClient = spanner.getDatabaseClient(db);

    // Connect to MySQL
    String mysqlUrl = "jdbc:mysql://34.61.176.118:3306/loadtest_db";
    Connection mysqlConn = DriverManager.getConnection(mysqlUrl, "root", "password123");

    long mismatchCount = 0;
    long totalCount = 0;
    long missingInMysqlCount = 0;

    System.out.println("Querying Spanner for first 100 keys...");
    long lastId = -1;
    for (long offset = 0; offset < 100; offset += 100) {
      Statement statement = Statement.newBuilder(
          "SELECT Id, FirstName, LastName, Email, Status FROM Users WHERE Id > @lastId ORDER BY Id LIMIT 100")
          .bind("lastId").to(lastId)
          .build();
      
      try (ResultSet resultSet = spannerClient.singleUse().executeQuery(statement)) {
        Map<Long, SpannerRow> spannerBatch = new HashMap<>();
        while (resultSet.next()) {
          long id = resultSet.getLong("Id");
          String firstName = resultSet.isNull("FirstName") ? null : resultSet.getString("FirstName");
          String lastName = resultSet.isNull("LastName") ? null : resultSet.getString("LastName");
          String email = resultSet.isNull("Email") ? null : resultSet.getString("Email");
          String status = resultSet.isNull("Status") ? null : resultSet.getString("Status");
          spannerBatch.put(id, new SpannerRow(id, firstName, lastName, email, status));
        }
        
        totalCount += spannerBatch.size();
        
        // Fetch corresponding rows from MySQL
        if (spannerBatch.isEmpty()) {
            break;
        }
        
        long minId = Long.MAX_VALUE;
        long maxId = Long.MIN_VALUE;
        for (Long id : spannerBatch.keySet()) {
            minId = Math.min(minId, id);
            maxId = Math.max(maxId, id);
        }
        lastId = maxId;
        
        String query = "SELECT Id, FirstName, LastName, Email, Status FROM Users WHERE Id >= ? AND Id <= ?";
        try (PreparedStatement ps = mysqlConn.prepareStatement(query)) {
            ps.setLong(1, minId);
            ps.setLong(2, maxId);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong("Id");
                    SpannerRow spannerRow = spannerBatch.get(id);
                    if (spannerRow != null) {
                        String firstName = rs.getString("FirstName");
                        String lastName = rs.getString("LastName");
                        String email = rs.getString("Email");
                        String status = rs.getString("Status");
                        
                        boolean match = true;
                        if (!equals(spannerRow.firstName, firstName)) match = false;
                        if (!equals(spannerRow.lastName, lastName)) match = false;
                        if (!equals(spannerRow.email, email)) match = false;
                        if (!equals(spannerRow.status, status)) match = false;
                        
                        if (!match) {
                            mismatchCount++;
                            System.out.println("Mismatch for ID " + id + ":");
                            System.out.println("  Spanner: " + spannerRow.firstName + ", " + spannerRow.lastName + ", " + spannerRow.email + ", " + spannerRow.status);
                            System.out.println("  MySQL  : " + firstName + ", " + lastName + ", " + email + ", " + status);
                        }
                        spannerBatch.remove(id);
                    }
                }
            }
        }
        
        missingInMysqlCount += spannerBatch.size();
        for (Long missingId : spannerBatch.keySet()) {
            System.out.println("Missing in MySQL: ID " + missingId);
        }
        System.out.println("Processed " + totalCount + " rows...");
      }
    }
    
    System.out.println("Validation complete!");
    System.out.println("Total Rows Checked: " + totalCount);
    System.out.println("Mismatches: " + mismatchCount);
    System.out.println("Missing in MySQL: " + missingInMysqlCount);
    
    mysqlConn.close();
  }

  private static boolean equals(String s1, String s2) {
      if (s1 == null && s2 == null) return true;
      if (s1 == null || s2 == null) return false;
      return s1.equals(s2);
  }

  static class SpannerRow {
      long id;
      String firstName;
      String lastName;
      String email;
      String status;
      SpannerRow(long id, String f, String l, String e, String s) {
          this.id = id; this.firstName = f; this.lastName = l; this.email = e; this.status = s;
      }
  }
}
