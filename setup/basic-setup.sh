#!/usr/bin/env bash

while [ "$1" != "" ]; do
    if [ $1 = "--no-mininet" ]; then
	NO_MININET=1
    fi
    shift
done

DIR="/home/sonata"
DEVDIR="/home/sonata/SONATA-DEV"

# install packages
sudo apt-get -f -y install

sudo apt-get update

sudo apt-get install -y build-essential fakeroot debhelper autoconf \
automake libssl-dev graphviz python-all python-qt4 \
python-twisted-conch libtool git tmux vim python-pip python-paramiko \
python-sphinx mongodb dos2unix wireshark

sudo pip install -U pip

sudo pip install -r $DEVDIR/setup/pip-basic-requires

sudo apt-get install -y ssh git emacs sshfs graphviz feh
sudo apt-get install -y libstring-crc32-perl

echo 'Defaults    env_keep += "PYTHONPATH"' | sudo tee --append /etc/sudoers
echo 'export PYTHONPATH=$PYTHONPATH:/home/sonata:/home/sonata/bmv2/mininet' >> ~/.profile
echo 'export SPARK_HOME=/home/sonata/spark/' >> ~/.profile

mkdir ~/.vim

if [ -z "$NO_MININET" ]; then
    # set up some shortcuts
    mkdir ~/bin/
    echo "sudo mn -c; sudo mn --topo single,3 --mac --switch ovsk --controller remote" > ~/bin/mininet.sh
    chmod 755 ~/bin/mininet.sh
fi
