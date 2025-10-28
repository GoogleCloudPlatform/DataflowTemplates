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
 * WARRANTIES OR CONDITIONS OF ANY, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package flags

import (
	"flag"
	"fmt" // ADDED: Import fmt package
	"strings"
)

const (
	ALL        = "ALL"
	DEFAULT    = "DEFAULT"
	KAFKA      = "KAFKA"
	SPANNER    = "SPANNER"
	BIGTABLE   = "BIGTABLE"
	DATASTREAM = "DATASTREAM"
)

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
			"v2/pubsub-to-kafka/",
			"plugins/templates-maven-plugin",
		},
		SPANNER: {"v2/datastream-to-spanner/",
			"v2/spanner-change-streams-to-sharded-file-sink/",
			"v2/gcs-to-sourcedb/",
			"v2/sourcedb-to-spanner/",
			"v2/spanner-to-sourcedb/",
			"v2/spanner-custom-shard/",
			"plugins/templates-maven-plugin"},
		BIGTABLE: {"v2/bigtable-common/",
			"v2/bigquery-to-bigtable/",
			"v2/bigtable-changestreams-to-hbase/",
			"plugins/templates-maven-plugin",
		},
		DATASTREAM: {
			"plugins/templates-maven-plugin",
			"v2/datastream-common/",
			"v2/datastream-mongodb-to-firestore/",
			"v2/datastream-to-bigquery/",
			"v2/datastream-to-mongodb/",
			"v2/datastream-to-postgres/",
			"v2/datastream-to-sql/",
		},
	}
)

func RegisterCommonFlags() {
	flag.StringVar(&modulesToBuild, "modules-to-build", ALL, "List of modules to build/run commands against")
}

// CHANGED: This function now contains all logic for deciding which modules to build.
// It returns a slice of strings that will be passed to Maven.
func ProjectsToBuild() []string {
	// If a specific module is given for testing, it takes highest priority.
	if ModuleToTest != "" {
		// -pl tells Maven to build only this project
		// -am tells Maven to also build any dependencies it needs
		return []string{"-pl", ModuleToTest, "-am"}
	}

	// Otherwise, use the default behavior with --modules-to-build
	m := modulesToBuild
	var modules []string
	if m == "DEFAULT" {
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
		modules = s
	} else if val, ok := moduleMap[modulesToBuild]; ok {
		modules = val
	} else if len(m) > 0 {
		modules = strings.Split(m, ",")
	}

	if len(modules) > 0 {
		return []string{"-pl", strings.Join(modules, ","), "-am"}
	}

	// Return empty if ALL modules should be built (Maven's default)
	return []string{}
}
