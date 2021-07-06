/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.snowflake.util;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;

/**
 * 
 * The {@link SimpleMapper} provides mapper methods that map source data from batch extract/load
 * and stream load to user specified data format.
 * 
 */
public class SimpleMapper {

	/**
	 * Maps user data from specified format to String[].
	 * 
	 * @param sourceFormat either json or csv
	 * @return data as String[]
	 */
	public static SnowflakeIO.UserDataMapper<String> getCsvUserDataMapper(String sourceFormat) {
		if("json".equalsIgnoreCase(sourceFormat)) {
			return (SnowflakeIO.UserDataMapper<String>) recordLine -> {
				
				JsonObject jo = (JsonObject) JsonParser.parseString(recordLine);

				return jo.entrySet().stream()
						.map(entry -> entry.getValue().getAsString())
						.toArray();
			};
		}
		else
		{
			return (SnowflakeIO.UserDataMapper<String>) recordLine -> recordLine.split(",", -1);
		}
	}
	
	
	/**
	 * Maps String[] to user specified data format.
	 * 
	 * @return
	 */
	public static SnowflakeIO.CsvMapper<String> getCsvMapper() {
		return (SnowflakeIO.CsvMapper<String>) record -> String.join(",", record);
	}

}
