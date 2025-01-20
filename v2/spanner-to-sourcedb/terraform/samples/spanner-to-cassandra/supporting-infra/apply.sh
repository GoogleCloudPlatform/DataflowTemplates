#Create Dataflow Templates Bucket
echo "----------------"
echo "Create Bucket"
echo "----------------"
cd resources/cloud-storage
terraform init 
terraform plan
terraform apply --auto-approve

#Create Spanner
echo "--------------------"
echo "Create Spanner"
echo "--------------------"
cd ../spanner
terraform init
terraform plan
terraform apply --auto-approve

#Create VPC
echo "--------------------"
echo "Create VPC"
echo "--------------------"
cd ../vpc
terraform init
terraform plan
terraform apply --auto-approve

#Create Subnets
echo "--------------------"
echo "Create Subnets"
echo "--------------------"
cd ../subnets
terraform init
terraform plan
terraform apply --auto-approve

#Create Cassandra
echo "-----------------------"
echo "Create Cassandra VM"
echo "-----------------------"
cd ../compute-engine/cassandra
terraform init
terraform plan
terraform apply --auto-approve

#Configure Firewall
echo "-----------------------"
echo "Create Firewall"
echo "-----------------------"
cd ../../firewall
terraform init
terraform plan
terraform apply --auto-approve


