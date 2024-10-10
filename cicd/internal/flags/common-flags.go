/*
 * Copyright (C) 2022 Google LLC
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

package flags

import (
	"flag"
	"strings"
)

const (
	ALL     = "ALL"     // All modules
	DEFAULT = "DEFAULT" // Modules other than those excluded
	KAFKA   = "KAFKA"
	SPANNER = "SPANNER"
)

// Avoid making these vars public.
var (
	modulesToBuild string
	moduleMap      = map[string][]string{
		ALL:     {},
		DEFAULT: {},
		KAFKA: {"v2/kafka-common/",
			"v2/kafka-to-bigquery/",
			"v2/kafka-to-gcs/",
			"v2/kafka-to-kafka/",
			"v2/kafka-to-pubsub/",
			"plugins/templates-maven-plugin",
		},
		SPANNER: {"v2/datastream-to-spanner/",
			"v2/spanner-change-streams-to-sharded-file-sink/",
			"v2/gcs-to-sourcedb/",
			"v2/sourcedb-to-spanner/",
			"v2/spanner-to-sourcedb/",
			"v2/spanner-custom-shard",
			"plugins/templates-maven-plugin"},
	}
)

// Registers all common flags. Must be called before flag.Parse().
func RegisterCommonFlags() {
	flag.StringVar(&modulesToBuild, "modules-to-build", ALL, "List of modules to build/run commands against")
}

// Returns all modules to build.
func ModulesToBuild() []string {
	m := modulesToBuild
	if m == "DEFAULT" {
		// "DEFAULT" is "ALL" minus other modules defined in moduleMap
		var s []string
		for k, v := range moduleMap {
			if k != "ALL" && k != "DEFAULT" {
				for _, n := range v {
					if !(strings.HasPrefix(n, "plugins/") || strings.Contains(n, "common/")) {
						s = append(s, "!"+n)
					}
				}
			}
		}
		return s
	} else if val, ok := moduleMap[modulesToBuild]; ok {
		return val
	}
	if len(m) == 0 {
		return make([]string, 0)
	}
	return strings.Split(m, ",")
}
