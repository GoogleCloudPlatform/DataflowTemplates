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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	RootDirName = "DataflowTemplates"
)

func getRootDir() (string, error) {
	_, path, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("could not determine a starting path to get to root directory")
	}

	dir := filepath.Dir(path)
	allDirs := strings.Split(dir, string(os.PathSeparator))
	i := len(allDirs)
	for ; i >= 0 && allDirs[i-1] != RootDirName; i -= 1 {
		// Empty intentionally
	}

	if i == 0 {
		return "", fmt.Errorf("%s is not in the %s project somehow", dir, RootDirName)
	}

	return strings.Join(allDirs[:i], string(os.PathSeparator)), nil
}

// Gets all the POM files under `dir`. `dir` is a relative path from the root of the repository.
// So if the root is located at `$HOME/go/src/github.com/GoogleCloudPlatform/DataflowTemplates`, then
// passing `v2` represents `$HOME/go/src/github.com/GoogleCloudPlatform/DataflowTemplates/v2`.
func GetAllPomFiles(dir string) ([]string, error) {
	root, e := getRootDir()
	if e != nil {
		return nil, e
	}
	poms := make([]string, 0)

	var start string
	if strings.HasPrefix(dir, RootDirName) {
		start = root
	} else {
		start = filepath.Join(root, dir)
	}

	e = filepath.Walk(start, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || info.Name() != "pom.xml" {
			return nil
		}

		poms = append(poms, path)
		return nil
	})

	if e != nil {
		return nil, e
	}
	return poms, nil
}
