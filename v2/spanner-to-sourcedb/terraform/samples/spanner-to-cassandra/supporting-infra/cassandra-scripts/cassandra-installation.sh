sudo apt update -y
curl -L https://downloads.apache.org/cassandra/KEYS | sudo apt-key add -
wget -q -O - https://downloads.apache.org/cassandra/KEYS | sudo gpg --dearmor -o /etc/apt/keyrings/apache-cassandra.gpg
echo "deb [signed-by=/etc/apt/keyrings/apache-cassandra.gpg] https://debian.cassandra.apache.org 41x main" | sudo tee /etc/apt/sources.list.d/cassandra.list
sudo apt update -y
sudo apt install cassandra -y
service cassandra start