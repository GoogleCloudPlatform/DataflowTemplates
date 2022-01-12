/*
 * Copyright (C) 2021 Google LLC
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

package repo

import (
	"fmt"
	"os"
	"strings"
)

const (
	// Roots in relation to the root directory of the repository.
	ClassicRoot = "."
	FlexRoot    = "v2"
)

// Returns a map of roots to their modules. Properties are:
// 		Key: The root module, equivalent to one of the const values (e.g. ClassicRoot)
//		Value: All the submodules, sometimes nested under another parent that is also in the slice
// This could be used in the equivalent command:
//		mvn x:y -f {key}/pom.xml -pl {value}
// An empty value indicates no submodules.
func GetModuleMapping() map[string][]string {
	return map[string][]string{
		ClassicRoot: []string{},
		FlexRoot: []string{
			"bigquery-to-bigtable",
			"bigquery-to-parquet",
			"cdc-parent/cdc-embedded-connector",
			"cdc-parent/cdc-common",
			"cdc-parent",
			"cdc-parent/cdc-agg",
			"cdc-parent/cdc-change-applier",
			"common",
			"datastream-to-sql",
			"datastream-to-bigquery",
			"datastream-to-mongodb",
			"datastream-to-postgres",
			"datastream-to-spanner",
			"elasticsearch-common",
			"file-format-conversion",
			"googlecloud-to-googlecloud",
			"googlecloud-to-elasticsearch",
			"hive-to-bigquery",
			"kafka-to-bigquery",
			"kafka-common",
			"kafka-to-gcs",
			"kafka-to-pubsub",
			"kudu-to-bigquery",
			"pubsub-binary-to-bigquery",
			"pubsub-cdc-to-bigquery",
			"pubsub-to-mongodb",
		},
	}
}

type moduleTrieNode struct {
	value    string
	children map[rune]*moduleTrieNode
}

func flexModulesAsTrie() *moduleTrieNode {
	root := &moduleTrieNode{
		value:    "",
		children: make(map[rune]*moduleTrieNode),
	}

	for _, m := range GetModuleMapping()[FlexRoot] {
		curr := root
		for _, r := range m {
			if _, ok := curr.children[r]; ok {
				curr = curr.children[r]
			} else {
				curr.children[r] = &moduleTrieNode{
					value:    "",
					children: make(map[rune]*moduleTrieNode),
				}
				curr = curr.children[r]
			}
		}
		curr.value = m
	}

	return root
}

func findUniqueFlexModules(paths []string) []string {
	trie := flexModulesAsTrie()
	modules := make(map[string]interface{})

	for _, path := range paths {
		curr := trie
		var possible *moduleTrieNode

		for _, r := range path {
			var ok bool
			curr, ok = curr.children[r]
			if !ok {
				break
			}
			if curr.value != "" {
				possible = curr
			}
		}

		if possible != nil {
			modules[possible.value] = nil
		}
		// We don't error from not finding anything, since it could be a root-level file
		// that isn't part of any module.
	}

	ret := make([]string, len(modules))
	i := 0
	for k := range modules {
		ret[i] = k
		i += 1
	}

	return ret
}

func GetModulesForPaths(paths []string) map[string][]string {
	if len(paths) == 0 {
		return make(map[string][]string)
	}

	m := make(map[string][]string)
	flex := make([]string, 0)

	v2 := fmt.Sprintf("v2%s", string(os.PathSeparator))

	for _, path := range paths {
		if strings.HasPrefix(path, v2) {
			flex = append(flex, strings.TrimPrefix(path, v2))
		} else {
			// TODO(zhoufek): Make this more granular, especially separating .github and cicd code
			// into separate "modules"
			m[ClassicRoot] = make([]string, 0)
		}
	}

	if len(flex) > 0 {
		// Even if nothing is found, we should still account for v2/ as its own module, since
		m[FlexRoot] = findUniqueFlexModules(flex)
	}

	return m
}
