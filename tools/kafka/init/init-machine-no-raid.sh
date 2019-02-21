#!/bin/bash

echo "disable swappiness"
echo

echo "vm.swappiness=0" >> /etc/sysctl.d/99-sysctl.conf

set_up_partition()
{
    dev_name=$1
    data_folder=$2

    echo "make partition for: $dev_name"
    echo

    parted -s /dev/$dev_name mklabel gpt
    parted -s /dev/$dev_name mkpart 1 ext4 1M 8T
    parted -s /dev/$dev_name print

    echo "format partition"
    echo

    mkfs.ext4 /dev/${dev_name}1
    parted -s /dev/$dev_name print

    echo "auto mount"
    echo

    mkdir -p $data_folder
    echo "/dev/${dev_name}1 $data_folder ext4 defaults 0 0" >> /etc/fstab

    echo "finished"
    echo
}

echo "make partitions"
echo

i=1
for dev in sdb sdc sdd sde sdf sdg sdh sdi sdj sdk sdl sdm
do
    set_up_partition $dev /data$i
    let i=i+1
done

echo "finished, need a reboot"
echo
