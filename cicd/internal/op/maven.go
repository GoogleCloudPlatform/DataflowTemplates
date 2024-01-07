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
	"strings"
)

// Runs the given Maven command on a specified POM file. Considering the input, this is equivalent to:
//
//	mvn -B {cmd} -f {pom} {args...}
func RunMavenOnPom(pom string, cmd string, args ...string) error {
	fullArgs := []string{"-B"}
	fullArgs = append(fullArgs, strings.Split(cmd, " ")...)
	fullArgs = append(fullArgs, "-f", pom)
	fullArgs = append(fullArgs, "-e")
	fullArgs = append(fullArgs, args...)

	return RunCmdAndStreamOutput("mvn", fullArgs)
}

// Runs the given Maven command on a specified module. Considering the input, this is equivalent to:
//
//	mvn -B {cmd} -f {pom} -pl {module} {args...}
func RunMavenOnModule(pom string, cmd string, module string, args ...string) error {
	fullArgs := []string{"-pl", module}
	fullArgs = append(fullArgs, args...)
	return RunMavenOnPom(pom, cmd, fullArgs...)
}
