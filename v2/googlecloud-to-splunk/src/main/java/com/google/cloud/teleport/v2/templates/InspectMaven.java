package com.google.cloud.teleport.v2.templates;

import java.net.URL;

public class InspectMaven {
  public static void main(String[] argv) {
    URL url = InspectMaven.class.getClassLoader().getResource("META-INF/maven/com.fasterxml.jackson.core/jackson-core/pom.properties");
    System.out.println(url);
  }
}
