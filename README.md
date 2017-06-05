# SONATA: Network Monitoring as a Streaming Analytics Problem

## Installation: Vagrant Setup

### Prerequisite

To get started install these softwares on your ```host``` machine:

1. Install ***Vagrant***, it is a wrapper around virtualization softwares like VirtualBox, VMWare etc.: http://www.vagrantup.com/downloads

2. Install ***VirtualBox***, this would be your VM provider: https://www.virtualbox.org/wiki/Downloads

3. Install ***Git***, it is a distributed version control system: https://git-scm.com/downloads

4. Install X Server and SSH capable terminal
    * For Windows install [Xming](http://sourceforge.net/project/downloading.php?group_id=156984&filename=Xming-6-9-0-31-setup.exe) and [Putty](http://the.earth.li/~sgtatham/putty/latest/x86/putty.exe).
    * For MAC OS install [XQuartz](http://xquartz.macosforge.org/trac/wiki) and Terminal.app (builtin)
    * Linux comes pre-installed with X server and Gnome terminal + SSH (buitlin)   

###Basics

* Clone the ```Sonata``` repository from Github:
```bash 
$ git clone https://github.com/Sonata-Princeton/SONATA-DEV.git
```

* Change the directory to ```Sonata```:
```bash
$ cd SONATA-DEV
```

* Now run the vagrant up command. This will read the Vagrantfile from the current directory and provision the VM accordingly:
```bash
$ vagrant up
```

The provisioning scripts will install all the required software (and their dependencies) to run the `Sonata` demo. Now ssh in to the VM:
```bash
$ vagrant ssh
```

Inside the VM, 
```bash
$ cd ~/dev/sonata/examples/distinct_only/
```

### End-2-end Testing

* Express the queries for the test application (TODO: create a test directory for all test applications):
```bash
$ cd SONATA-DEV/dev
$ vim runtime/test_app.py
```


* Inside the VM, run the cleanup script:
```bash
$ cd ~/dev
$ sudo sh cleanup.sh
```

* Set up `SPARK_HOME`:
```bash
$ export SPARK_HOME=/home/vagrant/spark/
```

* Start the runtime:
```bash
$ sudo PYTHONPATH=$PYTHONPATH:/home/vagrant/bmv2/mininet:$PWD $SPARK_HOME/bin/spark-submit runtime/test_app.py
```

This will start the runtime.
On start, runtime will determine the query partitioning & iterative refinement plan.
It will also start, (1) Fabric Manager, (2) Tuple Emitter, and (3) Streaming Manager.
It will then create `p4_query` & `spark_query` objects, and pushes them to
respective `fabric` & `streaming` managers.
Fabric Manager will receive data processing pipelines from runtime. It then
compiles them into `.p4` source code and pushes it down to create a pipeline of tables
and registers in the data plane.
Similarly, streaming managers receives data processing pipeline from the runtime, and
it translates into `DStream` objects to run over `SPARK` cluster.



* Send the data to the data plane switch (TODO: make this test specific):
```bash
$ sudo python runtime/send.py
```



Now follow the instructions in that directory, i.e. 
https://github.com/Sonata-Princeton/SONATA-DEV/tree/master/examples/distinct_only
