#!/bin/bash

echo "disable swappiness"
echo

echo "vm.swappiness=0" >> /etc/sysctl.d/99-sysctl.conf

echo "enable java 8"
update-alternatives --set java /usr/java/jdk1.8.0_121/jre/bin/java
java -version
echo

echo "make partition"
echo

parted -s /dev/sdb mklabel gpt
parted -s /dev/sdb mkpart 1 xfs 1M 48T
parted -s /dev/sdb print

echo "format partition"
echo

mkfs.xfs /dev/sdb1
parted -s /dev/sdb print

echo "auto mount"
echo

mkdir -p /data
echo "/dev/sdb1 /data xfs defaults 0 0" >> /etc/fstab

echo "finished, need a reboot"
echo

rm ~powerop/init-machine.sh
