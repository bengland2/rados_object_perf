#!/bin/bash
samples=$1
value_sizes="$2"
kvpairs_per_call="$3"
total_kvpairs="$4"

if [ -z "$4" ] ; then
  echo "usage: $0 <samples> <value-sizes> <kvpair-per-call-set> <total-kvpair-set>"
  exit 1
fi
echo "samples for each data point, $samples"
echo "value sizes to use, $value_sizes"
echo "key-value pairs per call to use, $kvpairs_per_call"
echo "key-value pair totals to try, $total_kvpairs"

rados_omap="./rados-omap"


echo "results: sample, total-kvpairs, kvpairs-per-call, value-size, elapsed"
for s in `seq 1 $samples` ; do 
 for vsz in $value_sizes ; do
  for kpc in $kvpairs_per_call ; do 
   for tk in $total_kvpairs ; do 
    # remove the original object
    rados rm -p ben hw
    # run this sample
    rm -f /tmp/r
    $rados_omap --total-kvpairs $tk --kvpairs-per-call $kpc --value-size $vsz > /tmp/r
    if [ $? != 0 ] ; then
	elapsed=100000.0
    else
        elapsed=`awk '/elapsed/{print $4}' /tmp/r`
    fi
    echo "result: $s, $tk, $kpc, $vsz, $elapsed"
   done
  done
 done
done
exit 1

