# -*- coding: utf-8 -*- #
# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Library for manipulating serverless local development setup."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import base64
import json
import os
import os.path
import re

from apitools.base.py import exceptions as apitools_exceptions
from googlecloudsdk.api_lib.app import yaml_parsing
from googlecloudsdk.api_lib.util import apis
from googlecloudsdk.command_lib.auth import auth_util
from googlecloudsdk.command_lib.code import yaml_helper
from googlecloudsdk.command_lib.iam import iam_util
from googlecloudsdk.core import config
from googlecloudsdk.core import exceptions
from googlecloudsdk.core import properties
from googlecloudsdk.core import yaml
from googlecloudsdk.core.console import console_io
from googlecloudsdk.core.util import encoding
from googlecloudsdk.core.util import files
import six

IAM_MESSAGE_MODULE = apis.GetMessagesModule('iam', 'v1')
CRM_MESSAGE_MODULE = apis.GetMessagesModule('cloudresourcemanager', 'v1')


class InvalidLocationError(exceptions.Error):
  """File is in an invalid location."""


class _DataType(type):
  """Dumb immutable data type."""

  # TODO(b/154131605): This a type that is an immutable data object. Can't use
  # attrs because it's not part of googlecloudsdk and can't use namedtuple
  # because it's not efficient on python 2 (it generates code, which needs
  # to be parsed and interpretted). Remove this code when we get support
  # for attrs or another dumb data object in gcloud.

  def __new__(mcs, classname, bases, class_dict):
    class_dict = class_dict.copy()
    names = class_dict.get('NAMES', tuple())
    class_dict.update(
        (name, mcs._CreateAccessor(i)) for i, name in enumerate(names))

    return super(_DataType, mcs).__new__(mcs, classname, bases, class_dict)

  @staticmethod
  def _CreateAccessor(i):
    """Create an tuple accessor property."""
    return property(lambda tpl: tpl[i])


class DataObject(six.with_metaclass(_DataType, tuple)):
  """Parent class of dumb data object."""

  def __new__(cls, **kwargs):
    names = getattr(cls, 'NAMES', tuple())
    invalid_names = set(kwargs) - set(names)
    if invalid_names:
      raise ValueError('Invalid names: ' + repr(invalid_names))

    tpl = tuple(kwargs[name] if name in kwargs else None for name in names)
    return super(DataObject, cls).__new__(cls, tpl)


class ServiceAccountSetting(DataObject):
  """Setting object representing a service account."""
  NAMES = ('name',)


class ApplicationDefaultCredentialSetting(DataObject):
  """Setting object representing the application default credential."""

  _instance = None

  def __new__(cls, **kwargs):
    if not cls._instance:
      cls._instance = super(ApplicationDefaultCredentialSetting,
                            cls).__new__(cls, **kwargs)

    return cls._instance


class BuildpackBuilder(DataObject):
  """Settings for building with a buildpack.

    Attributes:
      builder: Name of the builder.
      trust: True if the lifecycle should trust this builder.
      devmode: Build with devmode.
  """

  NAMES = ('builder', 'trust', 'devmode')


class DockerfileBuilder(DataObject):

  NAMES = ('dockerfile',)


def _GaeBuilder(runtime):
  """GCR package path for a builder that works on the given appengine runtime.

  Args:
    runtime: Name of a runtime from app.yaml, e.g. 'python38'.

  Returns:
    gcr.io image path.
  """
  return 'gcr.io/gae-runtimes/buildpacks/%s/builder:argo_current' % runtime


def _IsGcpBaseBuilder(builder):
  """Return true if the builder is the GCP base builder.

  Args:
    builder: Name of the builder.

  Returns:
    True if the builder is the GCP base builder.
  """
  return builder == 'gcr.io/buildpack/builder:v1'


class Settings(DataObject):
  """Settings for local development environments.

    Attributes:
      service_name: Name of the kuberntes service.
      image: Docker image tag.
      credential: Credential setting for either service account or application
        default credential.
      context: Path to directory to use as the current working directory for the
        docker build.
      builder: Buildpack builder.
      local_port: Local port to which to forward the service connection.
      env_vars: Container environment variables.
      cloudsql_instances: Cloud SQL instances.
      memory: Memory limit.
      cpu: CPU limit.
      namespace: Kubernetes namespace to run in.
      readiness_probe: If true, create readiness probe.
  """

  NAMES = ('service_name', 'image', 'credential', 'context', 'builder',
           'local_port', 'env_vars', 'cloudsql_instances', 'memory', 'cpu',
           'namespace', 'readiness_probe')

  @classmethod
  def FromArgs(cls, args):
    """Create a Settings object from an args object."""
    project_name = properties.VALUES.core.project.Get()
    if args.IsSpecified('service_name'):
      service_name = args.service_name
    else:
      dir_name = os.path.basename(files.GetCWD())
      # Service names may not include _ and upper case characters.
      service_name = dir_name.replace('_', '-').lower()

    if not args.IsSpecified('image'):
      if project_name:
        image = 'gcr.io/{project}/{service}'.format(
            project=project_name, service=service_name)
      else:
        image = service_name

      # Image names cannot have upper case characters. If the image name is
      # autogenerated, then make sure the image name is lower case. If the
      # user enters an image name manually, it's OK to stop the user and tell
      # them about the illegal image name.
      image = image.lower()
    else:
      image = args.image

    if args.IsSpecified('application_default_credential'):
      credential = ApplicationDefaultCredentialSetting()
    elif args.IsSpecified('service_account'):
      credential = ServiceAccountSetting(name=args.service_account)
    else:
      credential = None

    context = os.path.abspath(args.source or files.GetCWD())

    builder = _CreateBuilder(args, context)

    return cls(
        service_name=service_name,
        image=image,
        credential=credential,
        context=context,
        builder=builder,
        local_port=args.local_port,
        env_vars=args.env_vars or args.env_vars_file,
        cloudsql_instances=args.cloudsql_instances,
        memory=args.memory,
        cpu=args.cpu,
        namespace=args.namespace if 'namespace' in args else None,
        readiness_probe=args.readiness_probe)


def _CreateBuilder(args, context):
  """Create a builder object depending on the args.

  Args:
    args: Args namespace.
    context: Build context directory.

  Returns:
    A builder data object.
  """
  if args.IsSpecified('builder'):
    is_gcp_base_builder = _IsGcpBaseBuilder(args.builder)
    return BuildpackBuilder(
        builder=args.builder,
        trust=is_gcp_base_builder,
        devmode=is_gcp_base_builder)
  elif args.IsSpecified('appengine'):
    rel_path_to_app_yaml = os.path.relpath(os.path.join(context, 'app.yaml'))
    # Undo __future__.unicode_literals so we get a str in py2:
    app_yaml_str = six.ensure_str(rel_path_to_app_yaml)
    service_config = yaml_parsing.ServiceYamlInfo.FromFile(app_yaml_str)
    builder = _GaeBuilder(service_config.parsed.runtime)
    return BuildpackBuilder(builder=builder, trust=True, devmode=False)
  else:
    dockerfile = os.path.abspath(args.dockerfile)
    return DockerfileBuilder(
        dockerfile=_CheckDockerfilePath(dockerfile, context))


def _CheckDockerfilePath(dockerfile, context):
  if os.path.commonprefix([context, dockerfile]) != context:
    raise InvalidLocationError(
        'Invalid Dockerfile path. Dockerfile must be located in the build '
        'context directory.\n'
        'Dockerfile: {0}\n'
        'Build Context Directory: {1}'.format(dockerfile, context))
  if not os.path.exists(dockerfile):
    raise InvalidLocationError(dockerfile + ' does not exist.')
  return dockerfile


_POD_TEMPLATE = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {service}
  labels:
    service: {service}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {service}
  template:
    metadata:
      labels:
        app: {service}
    spec:
      containers: []
      terminationGracePeriodSeconds: 0
"""

_CONTAINER_TEMPLATE = """
name: {service}-container
image: {image}
env:
- name: PORT
  value: "8080"
ports:
- containerPort: 8080
"""

# The readiness probe container sits in an infinite wait loop and waits until
# port 8080 (1F90 in hex) is in use. This prevents the reload cycle from being
# considered "complete" until the developer's application binds to $PORT.
_READINESS_PROBE_CONTAINER_TEMPLATE = """
name: {service}-readiness-probe
image: gcr.io/gcp-runtimes/ubuntu_18_0_4:latest
command: ["sleep"]
args: ["infinity"]
readinessProbe:
  exec:
    command:
    - "grep"
    - ":1F90"
    - "/proc/net/tcp"
  periodSeconds: 1
"""


def CreateDeployment(service_name,
                     image_name,
                     memory_limit=None,
                     cpu_limit=None,
                     cpu_request=None,
                     readiness_probe=False):
  """Create a deployment specification for a service.

  Args:
    service_name: Name of the service.
    image_name: Image tag.
    memory_limit: Container memory limit.
    cpu_limit: Container cpu limit.
    cpu_request: Container cpu request.
    readiness_probe: If true, add a readiness probe.

  Returns:
    Dictionary object representing the deployment yaml.
  """
  deployment = yaml.load(_POD_TEMPLATE.format(service=service_name))
  container = yaml.load(
      _CONTAINER_TEMPLATE.format(service=service_name, image=image_name))
  if memory_limit is not None:
    limits = yaml_helper.GetOrCreate(container, ('resources', 'limits'))
    limits['memory'] = memory_limit
  if cpu_limit is not None:
    limits = yaml_helper.GetOrCreate(container, ('resources', 'limits'))
    limits['cpu'] = six.text_type(cpu_limit)
  if cpu_request is not None:
    requests = yaml_helper.GetOrCreate(container, ('resources', 'requests'))
    requests['cpu'] = six.text_type(cpu_request)
  containers = yaml_helper.GetOrCreate(
      deployment, ('spec', 'template', 'spec', 'containers'), constructor=list)
  containers.append(container)

  if readiness_probe:
    readiness_container = yaml.load(
        _READINESS_PROBE_CONTAINER_TEMPLATE.format(service=service_name))
    containers.append(readiness_container)

  return deployment


_SERVICE_TEMPLATE = """
apiVersion: v1
kind: Service
metadata:
  name: {service}
spec:
  type: LoadBalancer
  selector:
    app: {service}
  ports:
  - protocol: TCP
    port: 8080
    targetPort: 8080
"""


def CreateService(service_name):
  """Create a service specification.

  Args:
    service_name: Name of the service.

  Returns:
    Dictionary objects representing the service yaml.
  """
  yaml_text = _SERVICE_TEMPLATE.format(service=service_name)
  return yaml.load(yaml_text)


def AddEnvironmentVariables(deployment, container_name, env_vars):
  """Add environment variable settings to a container.

  Args:
    deployment: (dict) Yaml deployment configuration.
    container_name: (str) Container name.
    env_vars: (dict) Key value environment variable pairs.
  """
  containers = yaml_helper.GetOrCreate(
      deployment, ('spec', 'template', 'spec', 'containers'), constructor=list)
  container = _FindFirst(containers, lambda c: c['name'] == container_name)
  env_list = yaml_helper.GetOrCreate(container, ('env',), constructor=list)
  for key, value in sorted(env_vars.items()):
    env_list.append({'name': key, 'value': value})


def CreateDevelopmentServiceAccount(service_account_email):
  """Creates a service account for local development.

  Args:
    service_account_email: Email of the service account.

  Returns:
    The resource name of the service account.
  """
  project_id = _GetServiceAccountProject(service_account_email)
  service_account_name = 'projects/{project}/serviceAccounts/{account}'.format(
      project=project_id, account=service_account_email)

  exists = _ServiceAccountExists(service_account_name)
  if _IsReservedServiceAccountName(service_account_email):
    if not exists:
      raise ValueError('%s cannot be created because it is a service '
                       'account name' % service_account_email)
    else:
      return service_account_name

  if not exists:
    account_id = _GetServiceAccountId(service_account_email)
    _CreateAccount('Serverless Local Development Service Account', account_id,
                   project_id)

    permission_msg = ('The project editor role allows the service account '
                      'to create, delete, and modify most resources in the '
                      'project.')
    prompt_string = (
        'Add project editor role to {}?'.format(service_account_email))
    # Make the service account an editor on the project
    if console_io.PromptContinue(
        message=permission_msg, prompt_string=prompt_string):
      _AddBinding(project_id, 'serviceAccount:' + service_account_email,
                  'roles/editor')

  return service_account_name


# Regular expression for parsing a service account email address.
# Format is [id]@[project].iam.gserviceaccount.com
_PROJECT_SERVICE_ACCOUNT_RE = re.compile(
    r'(?P<id>[^@]+)@(?P<project>[^\.]+).iam.gserviceaccount.com')

# Regular expression for parsing a compute service account email address.
# Format is [project-id]-compute@developer.gserviceaccount.com
_APPENGINE_SERVICE_ACCOUNT = re.compile(
    r'(?P<project_id>[^\.]+).google.com@appspot.gserviceaccount.com')

# Regular expression for parsing a compute service account email address.
# Format is [project-number]-compute@developer.gserviceaccount.com
_COMPUTE_SERVICE_ACCOUNT = re.compile(
    r'(?P<project_number>\d+)-compute@developer.gserviceaccount.com')


def _GetServiceAccountProject(service_account_email):
  """Get the project id from a service account email.

  Args:
    service_account_email: (str) Email address of service account.

  Returns:
    The project id of the project to which the service account belongs.
  """
  matcher = _PROJECT_SERVICE_ACCOUNT_RE.match(service_account_email)
  if matcher:
    return matcher.group('project')

  matcher = _APPENGINE_SERVICE_ACCOUNT.match(service_account_email)
  if matcher:
    return matcher.group('project_id')

  matcher = _COMPUTE_SERVICE_ACCOUNT.match(service_account_email)
  if matcher:
    return _ProjectNumberToId(matcher.group('project_number'))

  raise ValueError(service_account_email +
                   ' is not a valid service account address')


_SERVICE_ACCOUNT_RE = re.compile(r'(?P<id>[^@]+)@.*\.gserviceaccount\.com')


def _GetServiceAccountId(service_account_email):
  matcher = _SERVICE_ACCOUNT_RE.match(service_account_email)
  if not matcher:
    raise ValueError(service_account_email +
                     ' is not a valid service account address')
  return matcher.group('id')


def _ProjectNumberToId(project_number):
  """Coverts project number to project id.

  Args:
    project_number: (str) The project number as a string.

  Returns:
    The project id.
  """
  resource_manager = apis.GetClientInstance('cloudresourcemanager', 'v1')
  req = CRM_MESSAGE_MODULE.CloudresourcemanagerProjectsGetRequest(
      projectId=project_number)
  project = resource_manager.projects.Get(req)
  return six.ensure_text(project.projectId)


def _IsReservedServiceAccountName(service_account_email):
  return (_APPENGINE_SERVICE_ACCOUNT.match(service_account_email) or
          _COMPUTE_SERVICE_ACCOUNT.match(service_account_email))


def _ServiceAccountExists(service_account_name):
  """Tests if service account email.

  Args:
    service_account_name: (str) Service account resource name.

  Returns:
    True if the service account exists.
  """
  service = apis.GetClientInstance('iam', 'v1')
  try:
    request = IAM_MESSAGE_MODULE.IamProjectsServiceAccountsGetRequest(
        name=service_account_name)
    service.projects_serviceAccounts.Get(request)
    return True
  except apitools_exceptions.HttpNotFoundError:
    return False


def _CreateAccount(display_name, account_id, project):
  """Create an account if it does not already exist.

  Args:
    display_name: (str) Display name.
    account_id: (str) User account id.
    project: (str) Project name.
  """
  service = apis.GetClientInstance('iam', 'v1')
  try:
    service_account_msg = IAM_MESSAGE_MODULE.ServiceAccount(
        displayName=display_name)
    request = IAM_MESSAGE_MODULE.CreateServiceAccountRequest(
        accountId=account_id, serviceAccount=service_account_msg)
    service.projects_serviceAccounts.Create(
        IAM_MESSAGE_MODULE.IamProjectsServiceAccountsCreateRequest(
            name='projects/' + project, createServiceAccountRequest=request))
  except apitools_exceptions.HttpConflictError:
    # If account already exists, we can ignore the error
    pass


def _AddBinding(project, account, role):
  """Adds a binding.

  Args:
    project: (str) Project name.
    account: (str) User account.
    role: (str) Role.
  """
  crm_client = apis.GetClientInstance('cloudresourcemanager', 'v1')
  policy = crm_client.projects.GetIamPolicy(
      CRM_MESSAGE_MODULE.CloudresourcemanagerProjectsGetIamPolicyRequest(
          resource=project))

  if not iam_util.BindingInPolicy(policy, account, role):
    iam_util.AddBindingToIamPolicy(CRM_MESSAGE_MODULE.Binding, policy, account,
                                   role)
    req = CRM_MESSAGE_MODULE.CloudresourcemanagerProjectsSetIamPolicyRequest(
        resource=project,
        setIamPolicyRequest=CRM_MESSAGE_MODULE.SetIamPolicyRequest(
            policy=policy))
    crm_client.projects.SetIamPolicy(req)


class KubeConfigGenerator(object):
  """The base code generator with default return values.

  Subclasses may override any of the member methods.
  """

  def CreateConfigs(self):
    """Create top level kubernetes configs.

    Returns:
      List of kubernetes configuration yamls encoded as dictionaries.
    """
    return []

  def ModifyDeployment(self, deployment):
    """Modify a deployment.

    Subclasses that override this method should use this method for adding
    or deleting resources (e.g. containers, volumes, metadata) to the
    deployment.

    Args:
      deployment: (dict) Deployment yaml in dictionary form.
    """

  def ModifyContainer(self, container):
    """Modify a container.

    Subclasses that override this method should use this method for adding,
    deleting, or modifying any of the yaml for a container.

    Args:
      container: (dict) Container yaml in dictionary form.
    """


class AppContainerGenerator(KubeConfigGenerator):
  """Generate deployment and service for a developer's app."""

  def __init__(self,
               service_name,
               image_name,
               env_vars=None,
               memory_limit=None,
               cpu_limit=None,
               cpu_request=None,
               readiness_probe=False):
    self._service_name = service_name
    self._image_name = image_name
    self._env_vars = env_vars
    self._memory_limit = memory_limit
    self._cpu_limit = cpu_limit
    self._cpu_request = cpu_request
    self._readiness_probe = readiness_probe

  def CreateConfigs(self):
    deployment = CreateDeployment(self._service_name, self._image_name,
                                  self._memory_limit, self._cpu_limit,
                                  self._cpu_request, self._readiness_probe)
    default_env_vars = {
        'K_SERVICE': self._service_name,
        'K_CONFIGURATION': 'dev',
        'K_REVISION': 'dev-0001',
    }
    AddEnvironmentVariables(deployment, self._service_name + '-container',
                            default_env_vars)
    if self._env_vars:
      AddEnvironmentVariables(deployment, self._service_name + '-container',
                              self._env_vars)
    service = CreateService(self._service_name)
    return [deployment, service]


class SecretInfo(object):
  """Information about a generated secret."""

  def __init__(self):
    self.secret_name = 'local-development-credential'
    self.path = ('/etc/' + self.secret_name.replace('-', '_') +
                 '/local_development_service_account.json')


class ADCMissingError(exceptions.Error):
  """Application Default Credential is missing."""


def GetServiceAccountSecret(account_name):
  """Get a service account secret file as text.

  Args:
    account_name: (str) Name of the service account.

  Returns:
    Context of the service account secret as a string.
  """
  service_account = CreateDevelopmentServiceAccount(account_name)
  return CreateServiceAccountKey(service_account)


def GetUserCredential():
  """Get a copy of the application default credential for a user.

  Returns:
    Text version of the user's application default credential.
  Raises:
    ADCMissingError: Application Default Credential is not present.
  """
  adc = auth_util.GetADCAsJson()
  if not adc:
    raise ADCMissingError('No user credential present. Run '
                          '`$ gcloud auth application-default login`.')
  return json.dumps(adc)


class CredentialGenerator(KubeConfigGenerator):
  """Configures service account secret."""

  def __init__(self, credential_fetcher):
    self._credential_fetcher = credential_fetcher

  def GetInfo(self):
    return SecretInfo()

  def CreateConfigs(self):
    """Create a secret."""
    return [LocalDevelopmentSecretSpec(self._credential_fetcher())]

  def ModifyDeployment(self, deployment):
    """Add a secret volume to a deployment."""
    secret_info = self.GetInfo()
    volumes = yaml_helper.GetOrCreate(deployment,
                                      ('spec', 'template', 'spec', 'volumes'),
                                      list)
    _AddSecretVolume(volumes, secret_info.secret_name)

  def ModifyContainer(self, container):
    """Add volume mount and set application credential environment variable."""
    secret_info = self.GetInfo()
    mounts = yaml_helper.GetOrCreate(container, ('volumeMounts',), list)
    _AddSecretVolumeMount(mounts, secret_info.secret_name)
    envs = yaml_helper.GetOrCreate(container, ('env',), list)
    _AddSecretEnvVar(envs, secret_info.path)


_CLOUD_PROXY_CONTAINER_NAME = 'cloud-sql-proxy'


class CloudSqlProxyGenerator(KubeConfigGenerator):
  """Generate kubernetes configurations for a Cloud SQL proxy connection."""

  def __init__(self, instance_names, secret_info):
    self._instance_names = instance_names
    self._secret_info = secret_info

  def ModifyDeployment(self, deployment):
    """Add sidecar container and empty volume for unix socket."""
    volumes = yaml_helper.GetOrCreate(
        deployment, ('spec', 'template', 'spec', 'volumes'), constructor=list)
    volumes.append({'name': 'cloudsql', 'emptyDir': {}})

    containers = yaml_helper.GetOrCreate(
        deployment, ('spec', 'template', 'spec', 'containers'),
        constructor=list)
    containers.append(
        _CreateCloudSqlProxyContainer(self._instance_names,
                                      self._secret_info.path))

  def ModifyContainer(self, container):
    """Add volume mount to continer.

    This method will not modify the CloudSql proxy container.

    Args:
      container: (dict) Container yaml as a dict.
    """
    if container['name'] == _CLOUD_PROXY_CONTAINER_NAME:
      return
    volume_mounts = yaml_helper.GetOrCreate(
        container, ('volumeMounts',), constructor=list)
    volume_mounts.append({
        'name': 'cloudsql',
        'mountPath': '/cloudsql',
        'readOnly': True
    })


_CLOUD_SQL_PROXY_VERSION = '1.16'


def _CreateCloudSqlProxyContainer(instances, secret_path):
  return {
      'name': _CLOUD_PROXY_CONTAINER_NAME,
      'image': 'gcr.io/cloudsql-docker/gce-proxy:' + _CLOUD_SQL_PROXY_VERSION,
      'command': ['/cloud_sql_proxy'],
      'args': [
          '-dir=/cloudsql', '-instances=' + ','.join(instances),
          '-credential_file=' + secret_path
      ],
      'volumeMounts': [{
          'name': 'cloudsql',
          'mountPath': '/cloudsql',
      }]
  }


_SECRET_TEMPLATE = """
apiVersion: v1
kind: Secret
metadata:
  name: local-development-credential
type: Opaque
"""


def CreateServiceAccountKey(service_account_name):
  """Create a service account key.

  Args:
    service_account_name: Name of service acccount.

  Returns:
    The contents of the generated private key file as a string.
  """
  default_credential_path = os.path.join(
      config.Paths().global_config_dir,
      _Utf8ToBase64(service_account_name) + '.json')
  credential_file_path = encoding.GetEncodedValue(os.environ,
                                                  'LOCAL_CREDENTIAL_PATH',
                                                  default_credential_path)
  if os.path.exists(credential_file_path):
    return files.ReadFileContents(credential_file_path)

  warning_msg = ('Creating a user-managed service account key for '
                 '{service_account_name}. This service account key will be '
                 'the default credential pointed to by '
                 'GOOGLE_APPLICATION_CREDENTIALS in the local development '
                 'environment. The user is responsible for the storage,'
                 'rotation, and deletion of this key. A copy of this key will '
                 'be stored at {local_key_path}.\n'
                 'Only use service accounts from a test project. Do not use '
                 'service accounts from a production project.').format(
                     service_account_name=service_account_name,
                     local_key_path=credential_file_path)
  console_io.PromptContinue(
      message=warning_msg, prompt_string='Continue?', cancel_on_no=True)

  service = apis.GetClientInstance('iam', 'v1')
  message_module = service.MESSAGES_MODULE

  create_key_request = (
      message_module.IamProjectsServiceAccountsKeysCreateRequest(
          name=service_account_name,
          createServiceAccountKeyRequest=message_module
          .CreateServiceAccountKeyRequest(
              privateKeyType=message_module.CreateServiceAccountKeyRequest
              .PrivateKeyTypeValueValuesEnum.TYPE_GOOGLE_CREDENTIALS_FILE)))
  key = service.projects_serviceAccounts_keys.Create(create_key_request)

  files.WriteFileContents(credential_file_path, key.privateKeyData)

  return six.ensure_text(key.privateKeyData)


def LocalDevelopmentSecretSpec(key):
  """Create a kubernetes yaml spec for a secret.

  Args:
    key: (str) The private key as a JSON string.

  Returns:
    Dictionary representing yaml dictionary.
  """
  yaml_config = yaml.load(_SECRET_TEMPLATE)
  yaml_config['data'] = {
      'local_development_service_account.json': _Utf8ToBase64(key)
  }
  return yaml_config


_SECRET_VOLUME_TEMPLATE = """
name: {secret_name}
secret:
  secretName: {secret_name}
"""


def _AddSecretVolume(volumes, secret_name):
  """Add a secret volume to a list of volumes.

  Args:
    volumes: (list[dict]) List of volume specifications.
    secret_name: (str) Name of the secret.
  """
  if not _Contains(volumes, lambda volume: volume['name'] == secret_name):
    volumes.append(
        yaml.load(_SECRET_VOLUME_TEMPLATE.format(secret_name=secret_name)))


_SECRET_MOUNT_TEMPLATE = """
name: {secret_name}
mountPath: "/etc/{secret_path}"
readOnly: true
"""


def _AddSecretVolumeMount(mounts, secret_name):
  """Add a secret volume mount.

  Args:
    mounts: (list[dict]) List of volume mount dictionaries.
    secret_name: (str) Name of the secret.
  """
  if not _Contains(mounts, lambda mount: mount['name'] == secret_name):
    yaml_text = _SECRET_MOUNT_TEMPLATE.format(
        secret_name=secret_name, secret_path=secret_name.replace('-', '_'))
    mounts.append(yaml.load(yaml_text))


def _IsApplicationCredentialVar(var):
  """Tests if the dictionary has name GOOGLE_APPLICATION_CREDENTIALS."""
  return var['name'] == 'GOOGLE_APPLICATION_CREDENTIALS'


def _AddSecretEnvVar(envs, path):
  """Adds a environmental variable that points to the secret file.

  Add a environment varible where GOOGLE_APPLICATION_CREDENTIALS is the name
  and the path to the secret file is the value.

  Args:
    envs: (list[dict]) List of dictionaries with a name entry and value entry.
    path: (str) Path to secret.
  """
  if not _Contains(envs, _IsApplicationCredentialVar):
    envs.append({'name': 'GOOGLE_APPLICATION_CREDENTIALS', 'value': path})


def _FindByName(configs, name):
  """Finds a yaml config where the metadata name is the given name.

  Args:
    configs: (iterable[dict]) Iterable of yaml dictionaries.
    name: (str) Name for which to search.

  Returns:
    Dictionary where the name field of the metadata section is the given name.
    If no config matches that criteria, return None.
  """
  return _FindFirst(configs, lambda config: config['metadata']['name'] == name)


def _FindFirst(itr, matcher):
  """Finds a value in an iterable that matches the matcher.

  Args:
    itr: (iterable[object]) Iterable.
    matcher: Function accepting a single value and returning a boolean.

  Returns:
    The first value for which the matcher returns True. If no value matches,
    return None.
  """
  return next((x for x in itr if matcher(x)), None)


def _Contains(itr, matcher):
  """Returns True if the iterable contains a value specified by a matcher.

  Args:
    itr: (iterable[object]) Iterable.
    matcher: Function accepting a single value and returning a boolean.

  Returns:
    True if there is an object in the iterable for which the matcher True.
    False otherwise.
  """
  return not _IsEmpty(x for x in itr if matcher(x))


def _IsEmpty(itr):
  """Returns True if a given iterable returns no values."""
  return next(itr, None) is None


def _Utf8ToBase64(s):
  """Encode a utf-8 string as a base 64 string."""
  return six.ensure_text(base64.b64encode(six.ensure_binary(s)))
