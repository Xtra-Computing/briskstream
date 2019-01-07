#!/bin/sh
#apt-get install vim -y &&
#sh ~/Downloads/parallel_studio_xe_2016_update2/install.sh&&
tar -xf ~/Downloads/jdk-8u77-linux-x64.tar.gz -C ~/Documents &&
tar -xf ~/Downloads/smartgit-linux-7_1_2.tar.gz -C ~/Documents &&
tar -xf ~/Downloads/apache-storm-0.9.5.tar.gz -C ~/Documents &&
tar -xf ~/Downloads/ideaIC-2016.1.1.tar.gz -C ~/Documents &&
tar -xf ~/Downloads/zookeeper-3.4.8.tar.gz -C ~/Documents &&

echo "JAVA_HOME=~/Documents/jdk1.8.0_77" >> ~/.bashrc &&
echo "export PATH=$PATH:~/parallel_studio/vtune_amplifier_xe_2016/bin64:~/Documents/apache-compatibility-1.0.0/bin">> ~/.bashrc &&
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections &&
sudo add-apt-repository ppa:webupd8team/java &&
sudo apt-get update &&
sudo apt-get install oracle-java8-installer -y
