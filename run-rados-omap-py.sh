#!/bin/bash
rados_omap="./rados-omap.py"
pool='ben'
samples=$1
kvpairs_per_call="$2"
total_kvpairs="$3"

if [ -z "$3" ] ; then
  echo "usage: $0 <samples> <kvpair-per-call-set> <total-kvpair-set>"
  exit 1
fi
echo "samples for each data point, $samples"
echo "key-value pairs per call to use, $kvpairs_per_call"
echo "key-value pair totals to try, $total_kvpairs"
echo "results: sample, total-kvpairs, kvpairs-per-call, direction, elapsed"

for s in `seq 1 $samples` ; do 
  for kpc in $kvpairs_per_call ; do 
   for tk in $total_kvpairs ; do 
    prefix="$rados_omap --pool-name $pool "

    # do the write test
    rm -f /tmp/r
    $prefix --keys $tk --keys-per-call $kpc --direction write > /tmp/r
    if [ $? != 0 ] ; then
	elapsed=100000.0
    else
        elapsed=`awk '/elapsed/{print $3}' /tmp/r`
    fi
    echo "result: $s, $tk, $kpc, write, $elapsed"

    # do the corresponding read test
    rm -f /tmp/r
    $prefix --keys $tk --keys-per-call $kpc --direction read > /tmp/r
    if [ $? != 0 ] ; then
	elapsed=100000.0
    else
        elapsed=`awk '/elapsed/{print $3}' /tmp/r`
    fi
    echo "result: $s, $tk, $kpc, read, $elapsed"
   done
  done
done
exit 0

