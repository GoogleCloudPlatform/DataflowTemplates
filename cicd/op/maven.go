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
	"fmt"
)

// Run the given Maven command on a specified POM file. Considering the input, this is equivalent to:
//		mvn {cmd} -f {pomDir}/pom.xml {args...}
func RunMavenOnPom(pomDir string, cmd string, args ...string) error {
	wa := []string{cmd, "-f", fmt.Sprintf("%s/pom.xml", pomDir)}
	return RunCmdAndStreamOutput("mvn", append(wa, args...))
}

// Rung the given Maven command on a specified module. Considering the input, this is equivalent to:
//		mvn {cmd} -f {pomDir}/pom.xml -pl {module} {args...}
func RunMavenOnModule(pomDir string, cmd string, module string, args ...string) error {
	fullArgs := []string{"-pl", module}
	fullArgs = append(fullArgs, args...)
	return RunMavenOnPom(pomDir, cmd, fullArgs...)
}
