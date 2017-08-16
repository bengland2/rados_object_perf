#!/bin/bash

pool=radosperftest
pgcount=512
kvpairs_per_call=256
threads=2
logtarget=/var/www/html/pub/bluestore-results
total_spc_GiB=1
total_omap_spc_GiB=1 

# should not have to edit below here

(( bytes_per_GiB = 1024 * 1024 * 1024 ))

# run test in $1 with pbench and archive logs from test 
# inside pbench result dir, and name result dir $2

pb() {
  pbench-user-benchmark -- "$1"
  pbd=`ls -d /var/lib/pbench-agent/pbench-user-benchmark*`
  if [ -n "$pbd" -a -d "$pbd" ] ; then
    linkval=`readlink rados_logs/latest`
    latestdir=`basename $linkval`
    mv -v rados_logs/$latestdir $pbd/
    mv -v $pbd $logtarget/$2.latestdir
  else
    echo "failed to output pbench directory $pbd"
  fi
}

rados rmpool $pool $pool --yes-i-really-really-mean-it
sleep 1
ceph osd pool create $pool $pgcount $pgcount
sleep 5

(( total_spc_bytes = total_spc_GiB * bytes_per_GiB ))
#for sz in 4096 131072 4194304 ; do
for sz in 4194304 ; do
  (( count = $total_spc_bytes / $sz ))
  pb "./rados-obj-perf.sh --obj-size $sz --obj-count $count --request-type write --adjust-think-time false" obj.write.sz.$sz.cnt.$count
  pb "./rados-obj-perf.sh --obj-size $sz --obj-count $count --request-type read --adjust-think-time false" obj.read.sz.$sz.cnt.$count
done
#for kcount in 64 1024 ; do
for kcount in 1024 ; do
  (( count = $kcount * 1024 ))
  (( kvsize = $total_omap_spc_GiB * $bytes_per_GiB / count ))
  cmd="./rados-obj-perf.sh"
  cmd="$cmd --omap-key-count $count --omap-value-size $kvsize --omap-kvpairs-per-call $kvpairs_per_call"
  cmd="$cmd --adjust-think-time true "
  pb "$cmd --request-type omap-write" omap.write.sz.$kvsize.cnt.$count.kvpc.$kvpairs_per_call
  pb "$cmd --request-type omap-read"  omap.read.sz.$kvsize.cnt.$count.kvpc.$kvpairs_per_call
done
