# Copyright (C) 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

"""Checks that the ML dependency versions of the YAML template follow Beam.

ml_dependencies.py lists packages that the YAML template installs on the workers
for ML transforms. Their versions have to follow the versions that apache-beam
declares for the same packages in its extras (see setup.py of apache-beam), so
that the template and Beam do not require different versions of a package. This
script compares the two, using the installed apache-beam, which should be the
version pinned in the requirements.txt of the YAML template.

The `YAML Template ML Dependencies` GitHub workflow
(.github/workflows/yaml-ml-dependencies.yml) runs this script. To run it
locally, install the requirements of the YAML template, and run from the
`python` directory:

  PYTHONPATH=src/main/python/yaml-template \
      python src/test/python/yaml-template/check_ml_dependencies.py
"""

import importlib.metadata
import inspect
import os
import sys

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name

import ml_dependencies

# For each package listed in ml_dependencies.py, the apache-beam extra whose
# requirement for the package the template follows, or None if no extra of
# apache-beam declares the package. A version range in ml_dependencies.py has to
# be the same as the range of the extra, and a pinned version has to be in the
# range of the extra.
BEAM_EXTRAS = {
    'torch': 'transformers',
    'transformers': 'transformers',
    'tensorflow': 'tensorflow',
    # Needed by transformers to run TensorFlow models, since TensorFlow 2.16+
    # comes with Keras 3. Its version has to match the version of tensorflow.
    'tf-keras': None,
    # Only declared by the extras used to test apache-beam.
    'sentence-transformers': 'ml_test',
}

_SCRIPT = os.path.basename(__file__)


def _template_requirements():
  """Returns the requirements listed in ml_dependencies.py."""
  dependency_lists = [
      *ml_dependencies.RUN_INFERENCE_DEPENDENCIES.values(),
      ml_dependencies.HUGGING_FACE_TENSORFLOW_DEPENDENCIES,
      *ml_dependencies.ML_TRANSFORM_DEPENDENCIES.values(),
  ]
  # dict.fromkeys() removes duplicates, while keeping the order.
  return list(dict.fromkeys(dep for deps in dependency_lists for dep in deps))


def _beam_specifier(beam, extra, package):
  """Returns the versions of a package allowed by an apache-beam extra.

  Args:
    beam: the installed apache-beam distribution.
    extra: the name of the extra.
    package: the canonical name of the package.

  Returns:
    a SpecifierSet, or None if the extra does not declare the package.
  """
  environment = {'extra': canonicalize_name(extra)}
  specifiers = [
      requirement.specifier
      for requirement in map(Requirement, beam.requires or [])
      if canonicalize_name(requirement.name) == package and
      (requirement.marker is None or requirement.marker.evaluate(environment))
  ]
  if not specifiers:
    return None
  combined = SpecifierSet()
  for specifier in specifiers:
    combined &= specifier
  return combined


def _pinned_version(specifier):
  """Returns the version pinned by a SpecifierSet, or None for a range."""
  specs = list(specifier)
  if (len(specs) == 1 and specs[0].operator in ('==', '===') and
      not specs[0].version.endswith('.*')):
    return specs[0].version
  return None


def _compare(dep, beam):
  """Compares a requirement listed in ml_dependencies.py with apache-beam.

  Args:
    dep: the requirement.
    beam: the installed apache-beam distribution.

  Returns:
    a (status, details, problem) tuple. problem is None if the requirement
    follows apache-beam.
  """
  requirement = Requirement(dep)
  package = canonicalize_name(requirement.name)
  if package not in BEAM_EXTRAS:
    return (
        'ERROR',
        'missing from BEAM_EXTRAS',
        f'{package} is missing from BEAM_EXTRAS of {_SCRIPT}. Add it, with the '
        'apache-beam extra whose requirement for it the template follows, or '
        'None if no extra declares it.')
  extra = BEAM_EXTRAS[package]
  if extra is None:
    return ('SKIPPED', 'not declared by apache-beam', None)

  beam_specifier = _beam_specifier(beam, extra, package)
  if beam_specifier is None:
    return (
        'ERROR',
        f"not declared by the '{extra}' extra",
        f"The '{extra}' extra of apache-beam {beam.version} does not declare "
        f'{package}. Update BEAM_EXTRAS of {_SCRIPT}.')

  details = f"'{extra}' extra: {package}{beam_specifier}"
  pinned_version = _pinned_version(requirement.specifier)
  if pinned_version is None:
    follows = requirement.specifier == beam_specifier
    expectation = 'use the same version range as'
  else:
    follows = beam_specifier.contains(pinned_version, prereleases=True)
    expectation = 'pin a version allowed by'
  if follows:
    return ('OK', details, None)
  return (
      'MISMATCH',
      details,
      f"{dep} has to {expectation} the '{extra}' extra of apache-beam "
      f'{beam.version}, which requires {package}{beam_specifier}.')


def _report_error(dep, message):
  """Reports an error, as an annotation of ml_dependencies.py on GitHub."""
  if os.environ.get('GITHUB_ACTIONS') != 'true':
    print(f'ERROR: {message}')
    return
  path = inspect.getsourcefile(ml_dependencies)
  with open(path) as f:
    line = next(
        (i for i, text in enumerate(f, start=1) if f"'{dep}'" in text), 1)
  path = os.path.relpath(path, os.environ.get('GITHUB_WORKSPACE', '.'))
  print(f'::error file={path},line={line}::{message}')


def main():
  try:
    beam = importlib.metadata.distribution('apache-beam')
  except importlib.metadata.PackageNotFoundError:
    print(
        'apache-beam is not installed. Install the requirements of the YAML '
        'template first.')
    return 1

  print(f'Comparing ml_dependencies.py with apache-beam {beam.version}:')
  problems = []
  for dep in _template_requirements():
    status, details, problem = _compare(dep, beam)
    print(f'  {status:<9} {dep:<30} {details}')
    if problem:
      problems.append((dep, problem))

  if not problems:
    print('All ML dependencies follow apache-beam.')
    return 0
  print()
  for dep, problem in problems:
    _report_error(dep, problem)
  print(
      f'\nFound {len(problems)} problem(s). When updating ml_dependencies.py, '
      'also update the expected values in ml_dependencies_test.py.')
  return 1


if __name__ == '__main__':
  sys.exit(main())
