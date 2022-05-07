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

package op

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
)

var (
	printMu sync.Mutex
)

// Runs a command and streams the output rather than waiting for it to complete.
func RunCmdAndStreamOutput(cmd string, args []string) error {
	log.Printf("Running command: %s %s", cmd, strings.Join(args, " "))
	op := exec.Command(cmd, args...)

	stdout, _ := op.StdoutPipe()
	stderr, _ := op.StderrPipe()
	op.Start()

	var wg sync.WaitGroup
	wg.Add(2)
	go processPipe(stdout, &wg)
	go processPipe(stderr, &wg)
	wg.Wait()

	return op.Wait()
}

func processPipe(pipe io.ReadCloser, wg *sync.WaitGroup) {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		printMu.Lock()
		fmt.Println(scanner.Text())
		printMu.Unlock()
	}

	wg.Done()
}
