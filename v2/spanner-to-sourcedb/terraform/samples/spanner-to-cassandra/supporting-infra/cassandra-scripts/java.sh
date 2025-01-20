sudo apt update -y
sudo apt install openjdk-11-jdk -y
update-alternatives --config java
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
source ~/.bashrc
echo $JAVA_HOME
java -version