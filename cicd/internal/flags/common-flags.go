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
	"log"
	"strings"
)

// Avoid making these vars public.
var (
	changedFiles string
)

func RegisterCommonFlags() {
	flag.StringVar(&changedFiles, "changed-files", "", "List of changed files as a comma-separated string")
}

func ChangedFiles() []string {
	if len(changedFiles) == 0 {
		log.Println("WARNING: No changed files were passed. This could indicate an error.")
		return make([]string, 0)
	}

	return strings.Split(changedFiles, ",")
}
