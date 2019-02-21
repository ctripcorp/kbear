#!/bin/bash

echo "disable swappiness"
echo

echo "vm.swappiness=0" >> /etc/sysctl.d/99-sysctl.conf

echo "make partition"
echo

parted -s /dev/xvdb mklabel gpt
parted -s /dev/xvdb mkpart 1 xfs 1M 11T
parted -s /dev/xvdb print

echo "format partition"
echo

mkfs.xfs /dev/xvdb1
parted -s /dev/xvdb print

echo "auto mount"
echo

mkdir -p /data
echo "/dev/xvdb1 /data xfs defaults 0 0" >> /etc/fstab

echo "finished, need a reboot"
echo

rm ~powerop/init-machine-aws.sh
