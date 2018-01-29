#!/bin/bash -x
if [ -z "$3" ] ; then
  echo 'usage: lvm-partition.sh osds-per-device devices-per-host journal-size-GB'
  echo 'if journal-size-GB is 0, then no journal device is created'
  exit 1
fi
osds_per_dev=$1
devs_per_host=$2
journal_size_GB=$3

# remove old LVM volumes

for n in /dev/vg_nvm*/*-lv* ; do 
  dd if=/dev/zero of=$n bs=16384k oflag=direct count=1k
done
sync
sleep 1
for n in /dev/vg_nvm*/*-lv* ; do 
  lvremove -y -f $n
done
for k in `seq 1 $devs_per_host` ; do 
  (( devnum = $k - 1 ))
  vgremove -y vg_nvme${devnum}n1
  pvremove -y /dev/nvme${devnum}n1
  wipefs -a /dev/nvme${devnum}n1
done
lvscan | grep nvm
if [ $? = 0 ] ; then 
  echo 'could not clean up old LVM volumes'
  exit 1
fi

# now create new LVM volumes

for d in `seq 1 $devs_per_host` ; do 
  ((devnum = $d - 1))
  devname="nvme${devnum}n1"
  chown ceph:ceph /dev/$devname
  pvcreate /dev/$devname
  vgcreate vg_$devname /dev/$devname
  totalPE=`vgdisplay vg_$devname | awk '/Total PE/{print $NF}'`
  PEsize=`vgdisplay vg_$devname | awk '/PE Size/{ print $3}' | cut -f1 -d'.'`
  (( totalGB = $totalPE * $PEsize / 1024 ))
  (( sz_per_osd = $totalGB / $osds_per_dev ))
  for v in `seq 1 $osds_per_dev` ; do
    if [ $journal_size_GB -gt 0 ] ; then 
      lvcreate --wipesignatures y -y --name journal-lv$v --size ${journal_size_GB}G vg_$devname
      chown ceph:ceph /dev/vg_$devname/journal-lv$v
      (( data_size_GB = $sz_per_osd - $journal_size_GB ))
      lvcreate --wipesignatures y -y --name data-lv$v --size ${data_size_GB}G vg_$devname
    else
      lvcreate --wipesignatures y -y --name data-lv$v --size ${sz_per_osd}G vg_$devname
    fi
    chown ceph:ceph /dev/vg_$devname/data-lv$v
  done
done

  
  
