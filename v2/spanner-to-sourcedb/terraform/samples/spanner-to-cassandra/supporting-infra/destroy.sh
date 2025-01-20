#Destroy Bucket
echo "----------------"
echo "Destroy Bucket"
echo "----------------"
cd resources/cloud-storage
terraform destroy --auto-approve

#Destroy Spanner
echo "--------------------"
echo "Destroy Spanner"
echo "--------------------"
cd ../spanner
terraform destroy --auto-approve

#Destroy Firewall
echo "-----------------------"
echo "Destroy Firewall"
echo "-----------------------"
cd ../firewall
terraform destroy --auto-approve

#Destroy Cassandra
echo "-----------------------"
echo "Destroy Cassandra VM"
echo "-----------------------"
cd ../compute-engine/cassandra
terraform destroy --auto-approve

#Destroy Subnets
echo "--------------------"
echo "Destroy Subnets"
echo "--------------------"
cd ../../subnets
terraform destroy --auto-approve

#Destroy VPC
echo "--------------------"
echo "Destroy VPC"
echo "--------------------"
cd ../vpc
terraform destroy --auto-approve







