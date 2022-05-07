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
	"fmt"
	"log"
	"regexp"
	"strings"
)

// Avoid making these vars public.
var (
	changedFiles string
)

// Registers all common flags. Must be called before flag.Parse().
func RegisterCommonFlags() {
	flag.StringVar(&changedFiles, "changed-files", "", "List of changed files as a comma-separated string")
}

// Returns all changed files with regexes. If no regexes are passed, all files are returned. If even one
// is passed, then only file paths with a match anywhere in the file will be returned. If multiple are
// passed, it is equivalent to (regex1|regex2|...|regexN)
func ChangedFiles(regexes ...string) []string {
	if len(changedFiles) == 0 {
		log.Println("WARNING: No changed files were passed. This could indicate an error.")
		return make([]string, 0)
	}

	files := strings.Split(changedFiles, ",")
	if len(regexes) == 0 {
		return files
	}

	var fullRegex string
	if len(regexes) == 1 {
		fullRegex = regexes[0]
	} else {
		fullRegex = fmt.Sprintf("(%s)", strings.Join(regexes, "|"))
	}
	re := regexp.MustCompile(fullRegex)

	results := make([]string, 0)
	for _, f := range files {
		if re.MatchString(f) {
			results = append(results, f)
		}
	}

	if len(results) == 0 {
		log.Println("INFO: All changed files got filtered out.")
	}
	return results
}
