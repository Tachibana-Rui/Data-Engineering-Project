# Data-Engineering-Project
Data Engineering Project

## Spark

### Install Spark
Download Spark to your computer.
```
# For some reason it downloads a tar file
https://ftp.acc.umu.se/mirror/apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz

# Pass file to instance
scp spark-3.1.1-bin-hadoop2.7.tgz ubuntu@130.238.29.16:/home/ubuntu

# update apt repo metadata
sudo apt update && sudo apt -y upgrade

# Extract
tar -xvf spark-3.1.1-bin-hadoop2.7.tar
```

```
# Give ownership avoid SUDO later
sudo chown -R $USER /usr/local/
```

Move Spark to /usr/local/spark
```
# Rename and move
mv spark-3.1.1-bin-hadoop2.7 /usr/local/spark

```

Install Dependencies
```
# install java
sudo apt-get install -y openjdk-8-jdk

# verify java version
java -version

# Add Python to PATH - python3 comes with Ubuntu
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
source ~/.bashrc
```

Add PATH variables to the bash
```
# Add these to the file & which java to set the path
echo "export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/jre" >> ~/.bashrc
echo "export SPARK_HOME=/usr/local/spark" >> ~/.bashrc
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bashrc

# Update changes
source ~/.bashrc
```

Verify Installation
```
#Verify installation
/usr/local/spark/bin/spark-shell

# Init Master

```

### Install Other Dependencies
```
# install git
sudo apt-get install -y git

# install the python package manager 'pip' -- it is recommended to do this directly 
sudo apt-get install -y python3-pip

# check the version -- this is a very old version of pip:
#python3 -m pip --version

# install pyspark (the matching version as the cluster), and some other useful deps
python3 -m pip install pyspark==3.1.1 --user
python3 -m pip install pandas --user
python3 -m pip install matplotlib --user

# install jupyterlab
python3 -m pip install jupyterlab

# start the notebook!
python3 -m jupyterlab
```

### SSH CONFIG
```
chmod 600 path_to_key.pem

## ------ SPARK -----
Host 130.238.29.16 
  User ubuntu
  IdentityFile PATH TO PEM
  # Master Node UI
  LocalForward 8080 192.168.2.179:8080

## ------ Worker ----
Host 130.238.28.38
  User ubuntu
  IdentityFile PATH TO PEM


# Connect via SSH
ssh 130.238.29.16
```

### Prepare a worker instance
Launch a new instance with Instance Snapshot Volume (Delete volume on instance delete). The Snapshot Name "Team6_main" with flavour ssc.xsmall. Connect to instance and execute worker

### Start Spark
```
# Connect to the master instance and execute the command

. /usr/local/spark/sbin/start-master.sh 

## Connect to the worker instance and start worker
. /usr/local/spark/sbin/start-worker.sh spark://192.168.2.179:7077
```

### Stop Spark
```
# Connect to the master instance
. /usr/local/spark/sbin/stop-worker.sh

# Connect to the worker instance
. /usr/local/spark/sbin/stop-master.sh 
```