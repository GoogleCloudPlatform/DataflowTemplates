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
package com.google.cloud.teleport.v2.snowflake.utils;

import com.google.gson.JsonParser;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;

/**
 * 
 * The {@link SimpleMapper} provides mapper methods that map source data from batch extract/load
 * and stream load to user specified data format.
 * 
 */
public class SimpleMapper {

	private static final String COMMA_SEPARATOR_REGEX = ",";

	/**
	 * Maps user data from specified format to String[].
	 * 
	 * @param sourceFormat either json or csv
	 * @return data as String[]
	 */
	public static SnowflakeIO.UserDataMapper<String> getUserDataMapper(String sourceFormat) {
		if ("json".equalsIgnoreCase(sourceFormat)) {
			return (SnowflakeIO.UserDataMapper<String>) recordLine -> JsonParser.parseString(recordLine).getAsJsonObject().entrySet()
					.stream().map(entry -> entry.getValue().getAsString()).toArray();
		}
		else if("csv".equalsIgnoreCase(sourceFormat)){
			return (SnowflakeIO.UserDataMapper<String>) recordLine -> recordLine.split(COMMA_SEPARATOR_REGEX, -1);
		}
		else{
			throw new IllegalArgumentException(String.format("Provided sourceFormat '%s' is invalid. sourceFormat should be either json or csv", sourceFormat));
		}
	}

}
