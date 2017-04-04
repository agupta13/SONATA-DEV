#!/bin/bash
    sudo apt-get update
    sudo apt-get install -y python-dev
    wget https://bootstrap.pypa.io/get-pip.py 
    sudo python ./get-pip.py
    sudo apt-get install -y python-pip
    sudo apt-get install -y apache2-utils
    sudo apt-get install -y git
    sudo apt-get install -y software-properties-common
    sudo add-apt-repository ppa:webupd8team/java
    sudo apt-get update
    sudo apt-get install -y python-dateutil
    sudo pip install test_helper netaddr
    sudo pip install coloredlogs
    pip install --user scipy
    sudo apt-get install -y python-matplotlib
    sudo apt-get install -y mininet
    echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
    sudo apt-get install -y -q oracle-java8-installer
    wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.1-bin-hadoop2.6.tgz
    tar xvf spark-1.6.1-bin-hadoop2.6.tgz
    mv spark-1.6.1-bin-hadoop2.6 spark
    rm spark-1.6.1-bin-hadoop2.6.tgz
    sudo apt-get install -y vim-gtk
