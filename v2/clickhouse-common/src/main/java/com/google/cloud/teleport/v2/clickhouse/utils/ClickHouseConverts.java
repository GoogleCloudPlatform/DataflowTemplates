package com.google.cloud.teleport.v2.clickhouse.utils;

// utils class for ClickHouse
public class ClickHouseConverts {

    public static String setJDBCCredentials(String jdbcURL, String username, String password) {
        String credentials = password != null
                ? String.format("user=%s&password=%s", username, password)
                : String.format("user=%s", username);
        return jdbcURL + (jdbcURL.contains("?") ? "&" : "?") + credentials;
    }


}
