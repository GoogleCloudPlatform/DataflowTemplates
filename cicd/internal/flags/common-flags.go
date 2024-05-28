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
	ALL     = "ALL"
	SPANNER = "SPANNER"
)

// Avoid making these vars public.
var (
	modulesToBuild string
	moduleMap      = map[string]string{ALL: "", SPANNER: "v2/datastream-to-spanner/,v2/spanner-change-streams-to-sharded-file-sink/,v2/gcs-to-sourcedb/,v2/sourcedb-to-spanner/,plugins/templates-maven-plugin"}
)

// Registers all common flags. Must be called before flag.Parse().
func RegisterCommonFlags() {
	flag.StringVar(&modulesToBuild, "modules-to-build", ALL, "List of modules to build/run commands against")
}

// Returns all modules to build.
func ModulesToBuild() []string {
	m := modulesToBuild
	if val, ok := moduleMap[modulesToBuild]; ok {
		m = val
	}
	if len(m) == 0 {
		return make([]string, 0)
	}
	return strings.Split(m, ",")
}
