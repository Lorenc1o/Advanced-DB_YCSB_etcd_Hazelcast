## Quick Start

This section describes how to run YCSB on etcd. 

### 1. Install (if needed) and start etcd

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    
Copy the folder etcd into YCSB and add the version and the binding to the pom.xml. Add the following lines

Inside \<properties\>:
    \<etcd.version\>[version]\</etcd.version\>
    
Inside \<modules\>:
    \<module\>etcd\</module\>
    
Compile the module:

    mvn -pl site.ycsb:etcd-binding -am clean package

### 4. Provide etcd Connection Parameters (For now this is not implemented)
    
Set host, port, password, and cluster mode in the workload you plan to run. 

At the moment, the connection is made with the localhost and to the port 2379.

### 5. Load data and run tests

First, you need to go to YCSB/bin/ycsb and to the list DATABASES add:
	["etcd": "site.ycsb.db.EtcdClient",]

Load the data:

    ./bin/ycsb load etcd -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run etcd -s -P workloads/workloada > outputRun.txt

