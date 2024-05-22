#!/bin/bash
# Copyright 2024 Google LLC
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

# For running the script, see go/templates-gitactions-script

# Defaults
BASE_NAME="gitactions-proxy"
VERBOSE=0
DELETE=0

############################################################
# Help                                                     #
############################################################
Help()
{
   # Display Help
   echo "Configure github actions proxy for CloudSQL-based Datastream"
   echo "integration and performance tests."
   echo
   echo "Syntax: configure-runners [-p <PROJECT>] [-a <SA_EMAIL>] [-t <GITHUB_TOKEN>] [-n|S|r|o|s|v|V|h]"
   echo "options:"
   echo "p     Set the name of the Google Cloud project to use."
   echo "a     Set the service account email for the Google Cloud project."
   echo "n     (optional) Set the name of the proxy VM. Default '$BASE_NAME'"
   echo "D     (optional) Delete resources."
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
while getopts ":h:VDp:a:n:" option; do
   case $option in
      h) # display Help
         Help
         exit;;
      p) # Enter a project
         PROJECT=$OPTARG;;
      a) # Enter a service account
         SA_EMAIL=$OPTARG;;
      n) # Enter a name
         BASE_NAME=$OPTARG;;
      D) # Delete
         DELETE=1;;
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

if [ -z $PROJECT ] || [ -z $SA_EMAIL ]; then
  if [ -z $PROJECT ]; then
    echo "Missing required flag -p"
  fi;
  if [ -z $SA_EMAIL ]; then
    echo "Missing required flag -a"
  fi;
  exit 1;
fi;


# Construct names
INSTANCE_TEMPLATE_NAME="nokill-${BASE_NAME}-template"
INSTANCE_GROUP_NAME="nokill-${BASE_NAME}-group"
CLOUD_SQL_PREFIX=${PROJECT}:us-central1:nokill-gitactions

# Configuration
echo "Running script with following configuration:"
echo "  PROJECT=$PROJECT"
echo "  SERVICE_ACCOUNT=$SA_EMAIL"
echo "  BASE_NAME=$BASE_NAME"

# Set gcloud project
if [ $VERBOSE -eq 1 ]; then echo; echo "Setting gcloud project to $PROJECT..."; fi
gcloud config set project ${PROJECT}

# Delete existing resources first
if [ $VERBOSE -eq 1 ]; then echo; echo "Deleting Google Cloud resources..."; fi
if [ $VERBOSE -eq 1 ]; then echo "  Deleting instance-group: $INSTANCE_GROUP_NAME..."; fi
printf "y" | gcloud compute instance-groups managed delete $INSTANCE_GROUP_NAME
if [ $VERBOSE -eq 1 ]; then echo "  Deleting instance-template: $INSTANCE_TEMPLATE_NAME..."; fi
printf "y" | gcloud compute instance-templates delete $INSTANCE_TEMPLATE_NAME

if [ $DELETE -eq 1 ]; then
  echo "-D specified, exiting without creating resources.";
  exit 1;
fi

# Create helper scripts
if [ $VERBOSE -eq 1 ]; then echo; echo "Creating startup and shutdown scripts..."; fi
cat proxy-startup-script.sh | sed "s/CLOUD_SQL_PREFIX/$CLOUD_SQL_PREFIX/g" > proxy-startup-script-temp.sh

# Create instance template
IMAGE_FAMILY="ubuntu-2004-lts"
IMAGE_PROJECT="ubuntu-os-cloud"
BOOT_DISK_TYPE="pd-balanced"
BOOT_DISK_SIZE="10GB"
MACHINE_TYPE="n1-standard-1"
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
  --service-account=$SA_EMAIL \
  --metadata-from-file=startup-script=proxy-startup-script-temp.sh

# Create runner group
ZONE="us-central1-a"
if [ $VERBOSE -eq 1 ]; then echo; echo "Creating instance group: $INSTANCE_GROUP_NAME with $SIZE runners in $ZONE..."; fi
gcloud compute instance-groups managed create $INSTANCE_GROUP_NAME \
  --size=1 \
  --base-instance-name=nokill-$BASE_NAME \
  --template=$INSTANCE_TEMPLATE_NAME \
  --zone=$ZONE

if [ $VERBOSE -eq 1 ]; then echo; echo "Removing helper scripts..."; fi
rm -rf proxy-startup-script-temp.sh

echo
echo "DONE"
