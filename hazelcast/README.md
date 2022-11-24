## Quick Start

This section describes how to run YCSB on Hazelcast. 

### 1. Install (if needed) and start Hazelcast
It is important to note that you will need to start a cluster named "YCSB-hz", to make data persistent between the execution of the load and the run phases. In the folder *launch_cluster* you can find a Java project that performs this.

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    
Copy the folder Hazelcast into YCSB and add the version and the binding to the pom.xml. Add the following lines

Inside \<properties\>:
    \<Hazelcast.version\>[version]\</Hazelcast.version\>
    
Inside \<modules\>:
    \<module\>Hazelcast\</module\>
    
Compile the module:

    mvn -pl site.ycsb:Hazelcast-binding -am clean package

### 4. Provide Hazelcast Connection Parameters (For now this is not implemented)
    
Set host, port, password, and cluster mode in the workload you plan to run. 

At the moment, the connection is made with the localhost and to the port 5701.

### 5. Load data and run tests

First, you need to go to YCSB/bin/ycsb and to the list DATABASES add:
	["Hazelcast": "site.ycsb.db.HazelcastDBClient",]

Load the data:

    ./bin/ycsb load etcd -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run etcd -s -P workloads/workloada > outputRun.txt

