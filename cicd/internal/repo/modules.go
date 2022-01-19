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
	"path/filepath"
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
	m := make(map[string][]string)
	m[ClassicRoot] = make([]string, 0)

	flexPoms, err := GetAllPomFiles(FlexRoot)
	if err != nil {
		// Panicking here seems reasonable, since something is deeply wrong with the filesystem
		// if this fails.
		panic(err)
	}
	flexModules := make([]string, len(flexPoms))
	for i := range flexPoms {
		if module, err := getModuleFromPomPath(flexPoms[i], FlexRoot); err != nil {
			panic(err)
		} else {
			flexModules[i] = module
		}
	}
	m[FlexRoot] = flexModules

	return m
}

// Extracts module name from POM path, with `rootModule` being used as the reference for
// the uppermost ancestor. The returned value should be usable with the `-pl` flag in relation
// to the POM file at `rootModule`.
func getModuleFromPomPath(pomPath string, rootModule string) (string, error) {
	dir := filepath.Dir(pomPath)
	allDirs := strings.Split(dir, string(os.PathSeparator))

	i := len(allDirs)
	for ; i > 0 && allDirs[i-1] != rootModule; i -= 1 {
		// Empty intentionally
	}

	if i == 0 {
		return "", fmt.Errorf("%s is not under %s", pomPath, rootModule)
	}

	return strings.Join(allDirs[i:], "/"), nil
}

// Gets all the unique modules for files whose path from the root directory is in `paths`. Example paths:
//		pom.xml -> Mapped to Classic root
//		v2/cdc-parent/pom.xml -> Mapped to cdc-parent under Flex Templates
// The return value has the following properties:
//		Key: The path of the root module, equivalent to ClassicRoot, FlexRoot, etc.
//		Value: List of modules (e.g. cdc-parent, cdc-parent/cdc-common). An empty entry represents the root itself.
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
		// changes might be made to important files, like v2/pom.xml
		m[FlexRoot] = findUniqueFlexModules(flex)
	}

	return m
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
