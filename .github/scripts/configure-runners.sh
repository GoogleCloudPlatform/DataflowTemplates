#!/bin/bash
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# For running the script, see
# https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/contributor-docs/maintainers-guide.md#provision-new-runners

# Defaults
NAME_SUFFIX="it"
SIZE=5
BASE_NAME="gitactions-runner"
REPO_NAME="DataflowTemplates"
REPO_OWNER="GoogleCloudPlatform"
GH_RUNNER_VERSION="2.299.1"

MACHINE_TYPE="n1-highmem-32"
BOOT_DISK_SIZE="200GB"

VERBOSE=0

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Configure github actions runners for running"
   echo "integration and performance tests."
   echo
   echo "Syntax: configure-runners [-p <PROJECT>] [-a <SA_EMAIL>] [-t <GITHUB_TOKEN>] [-n|S|r|o|s|v|V|h]"
   echo "options:"
   echo "p     Set the name of the Google Cloud project to use."
   echo "a     Set the service account email for the Google Cloud project."
   echo "t     Set the token used to authenticate the runners with the repo."
   echo "n     (optional) Set the name of the gitactions runner. Default '$BASE_NAME'"
   echo "S     (optional) Set the suffix of the gitactions runner. Default '$NAME_SUFFIX'"
   echo "r     (optional) Set the name of the GitHub repo. Default '$REPO_NAME'"
   echo "o     (optional) Set the owner of the GitHub repo. Default '$REPO_OWNER'"
   echo "s     (optional) Set the number of runners. Default $SIZE"
   echo "v     (optional) Set the gitactions runner version. Default $GH_RUNNER_VERSION"
   echo "m     (optional) Set the machine type for the GCE VM runner. $MACHINE_TYPE"
   echo "b     (optional) Set the boot disk size for the GCE VM runner. $BOOT_DISK_SIZE"
   echo "V     Verbose mode."
   echo "h     Print this Help."
   echo
}

############################################################
############################################################
# Main program                                             #
############################################################
############################################################

# Get the options
while getopts ":h:Vp:a:t:n:S:r:o:s:v:" option; do
   case $option in
      h) # display Help
         Help
         exit;;
      p) # Enter a project
         PROJECT=$OPTARG;;
      a) # Enter a service account
         SA_EMAIL=$OPTARG;;
      t) # Enter a token
         GITHUB_TOKEN=$OPTARG;;
      n) # Enter a name
         BASE_NAME=$OPTARG;;
      S) # Enter a suffix
         NAME_SUFFIX=$OPTARG;;
      r) # Enter a repo
         REPO_NAME=$OPTARG;;
      o) # Enter an owner
         REPO_OWNER=$OPTARG;;
      s) # Enter a number of runners
         SIZE=$OPTARG;;
      v) # Enter a version
         GH_RUNNER_VERSION=$OPTARG;;
      m) # Enter a machine type
         MACHINE_TYPE=$OPTARG;;
      b) # Enter a boot disk size
         BOOT_DISK_SIZE=$OPTARG;;
      V) # Verbose
         VERBOSE=1;;
      \?) # Invalid option
         echo "Error: Invalid option: -$OPTARG"
         echo
         Help
         exit;;
      *) # Invalid option
        echo "Invalid usage."
        echo
        Help
        exit;;
   esac
done

if [ -z $PROJECT ] || [ -z $SA_EMAIL ] || [ -z $GITHUB_TOKEN ]; then
  if [ -z $PROJECT ]; then
    echo "Missing required flag -p"
  fi;
  if [ -z $SA_EMAIL ]; then
    echo "Missing required flag -a"
  fi;
  if [ -z $GITHUB_TOKEN ]; then
    echo "Missing required flag -t"
  fi;
  exit 1;
fi;

# Construct names
RUNNER_NAME="${BASE_NAME}-${NAME_SUFFIX}"
REPO_URL="https://github.com/$REPO_OWNER/$REPO_NAME"

INSTANCE_TEMPLATE_NAME="nokill-${RUNNER_NAME}-template"
INSTANCE_GROUP_NAME="nokill-${RUNNER_NAME}-group"
SECRET_NAME="${RUNNER_NAME}-secret"

# Configuration
echo "Running script with following configuration:"
echo "  PROJECT=$PROJECT"
echo "  SERVICE_ACCOUNT=$SA_EMAIL"
echo "  GITHUB_TOKEN=******"
echo "  RUNNER_NAME=$RUNNER_NAME"
echo "  NUM_RUNNERS=$SIZE"
echo "  REPO_URL=$REPO_URL"

# Create helper scripts
if [ $VERBOSE -eq 1 ]; then echo; echo "Creating startup and shutdown scripts..."; fi
cat startup-script.sh | sed "s/GITACTION_SECRET_NAME/$SECRET_NAME/g" > startup-script-${NAME_SUFFIX}.sh
cat shutdown-script.sh | sed "s/GITACTION_SECRET_NAME/$SECRET_NAME/g" > shutdown-script-${NAME_SUFFIX}.sh

# Set gcloud project
if [ $VERBOSE -eq 1 ]; then echo; echo "Setting gcloud project to $PROJECT..."; fi
gcloud config set project ${PROJECT}

# Delete existing resources first
if [ $VERBOSE -eq 1 ]; then echo; echo "Deleting Google Cloud resources..."; fi
if [ $VERBOSE -eq 1 ]; then echo "  Deleting secret: $SECRET_NAME..."; fi
printf "y" | gcloud secrets delete $SECRET_NAME
if [ $VERBOSE -eq 1 ]; then echo "  Deleting instance-group: $INSTANCE_GROUP_NAME..."; fi
printf "y" | gcloud compute instance-groups managed delete $INSTANCE_GROUP_NAME
if [ $VERBOSE -eq 1 ]; then echo "  Deleting instance-template: $INSTANCE_TEMPLATE_NAME..."; fi
printf "y" | gcloud compute instance-templates delete $INSTANCE_TEMPLATE_NAME


# Add secret and assign to compute account
if [ $VERBOSE -eq 1 ]; then echo; echo "Creating secret: $SECRET_NAME..."; fi
gcloud secrets create $SECRET_NAME --replication-policy="automatic"
cat << EOF | gcloud secrets versions add $SECRET_NAME --data-file=-
REPO_NAME=${REPO_NAME}
REPO_OWNER=${REPO_OWNER}
GITHUB_TOKEN=${GITHUB_TOKEN}
REPO_URL=${REPO_URL}
GH_RUNNER_VERSION=${GH_RUNNER_VERSION}
GITACTIONS_LABELS=${NAME_SUFFIX}
EOF
if [ $VERBOSE -eq 1 ]; then echo; echo "Giving role 'roles/secretmanager.secretAccessor' to secret: $SECRET_NAME..."; fi
gcloud secrets add-iam-policy-binding $SECRET_NAME \
  --member serviceAccount:${SA_EMAIL} \
  --role roles/secretmanager.secretAccessor

# Create instance template
IMAGE_FAMILY="ubuntu-2004-lts"
IMAGE_PROJECT="ubuntu-os-cloud"
BOOT_DISK_TYPE="pd-balanced"
SCOPE="cloud-platform"
if [ $VERBOSE -eq 1 ]; then echo; echo "Creating instance template: $INSTANCE_TEMPLATE_NAME..."; fi
if [ $VERBOSE -eq 1 ]; then
  echo "Using following config for instance template:";
  echo "  IMAGE_FAMILY=$IMAGE_FAMILY"
  echo "  IMAGE_PROJECT=$IMAGE_PROJECT"
  echo "  BOOT_DISK_TYPE=$BOOT_DISK_TYPE"
  echo "  BOOT_DISK_SIZE=$BOOT_DISK_SIZE"
  echo "  MACHINE_TYPE=$MACHINE_TYPE"
  echo "  SCOPE=$SCOPE"
fi
gcloud compute instance-templates create $INSTANCE_TEMPLATE_NAME \
  --image-family=$IMAGE_FAMILY \
  --image-project=$IMAGE_PROJECT \
  --boot-disk-type=$BOOT_DISK_TYPE \
  --boot-disk-size=$BOOT_DISK_SIZE \
  --machine-type=$MACHINE_TYPE \
  --scopes=$SCOPE \
  --service-account=${SA_EMAIL} \
  --metadata-from-file=startup-script=startup-script-${NAME_SUFFIX}.sh,shutdown-script=shutdown-script-${NAME_SUFFIX}.sh

# Create runner group
ZONE="us-central1-a"
if [ $VERBOSE -eq 1 ]; then echo; echo "Creating instance group: $INSTANCE_GROUP_NAME with $SIZE runners in $ZONE..."; fi
gcloud compute instance-groups managed create $INSTANCE_GROUP_NAME \
  --size=${SIZE} \
  --base-instance-name=nokill-${BASE_NAME} \
  --template=$INSTANCE_TEMPLATE_NAME \
  --zone=$ZONE

if [ $VERBOSE -eq 1 ]; then echo; echo "Removing helper scripts..."; fi
rm -rf startup-script-${NAME_SUFFIX}.sh
rm -rf shutdown-script-${NAME_SUFFIX}.sh

echo
echo "DONE"
