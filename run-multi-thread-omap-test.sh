#!/bin/bash
logdir=/tmp/rados-omap-logs
pool=rbd
objnameprefix=ben
keys=2048
threads=128
value_size=16

find /var/lib/pbench-agent -name 'pbench-user-benchmark*' -exec rm -rf {} \;
rm -rf /var/lib/pbench-agent/pbench-user-benchmark* 
rm -rf $logdir
mkdir $logdir
for n in `seq 1 $threads` ; do 
  if [ "$one_obj" = "y" ] ; then
    objname=$objnameprefix
  else
    objname=$objnameprefix.$n
  fi
  eval "./rados-omap.py --keys $keys --object-name $objname --key-prefix k$n --value-size $value_size > $logdir/rados-omap-thr-$n.log 2>&1 &" 
done
time wait
for n in `seq 1 $threads` ; do 
  rados rm -p rbd $objnameprefix.$n
done
mv -v /var/lib/pbench-agent/pbench-user-benchmark* /var/www/html/pub/ && \
 chmod -R a+r /var/www/html/pub && \
 chown -R apache:apache /var/www/html/pub

