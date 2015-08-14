#!/bin/bash
#
# rados-obj-perf.sh - script to launch distributed test
# using threads running rados_object_perf.py
#
# GNU V2 license at
#   https://github.com/bengland2/rados_object_perf/blob/master/LICENSE
#

conffile=/etc/ceph/ceph.conf
client_list=/root/osds.list
osd_list=/root/osds.list

# should not have to edit below this line normally

clients=(`cat $client_list`)
servers=(`cat $osd_list`)
perf_obj=rados_object_perf
poolnm=radosperftest

# parse command line inputs

OK=0
NOTOK=1
wltype=$1
objsize=$2
threads=$3
duration=$4
objcount=$5
if [ -z "$5" ] ; then
  echo "usage: rados-obj-perf.sh workload-type object-size-bytes threads duration-sec objects-per-thread-max"
  exit $NOTOK
fi

# set up log directory and record test parameters

timestamp=`date +%Y-%m-%d-%H-%M`
logdir="rados_logs/$timestamp"
mkdir -pv $logdir
rm -f rados_logs/latest
ln -sv ./$timestamp rados_logs/latest

( \
echo "ceph config file: $conffile" ; \
echo "ceph pool name: $poolnm" ; \
echo "workload type: $wltype" ; \
echo "object size (bytes): $objsize" ; \
echo "threads: $threads" ; \
echo "test duration maximum: $duration" ; \
echo "max objects per thread: $objcount" ) \
  | tee $logdir/summary.log

# check that pool exists

rados df -p $poolnm
if [ $? != $OK ] ; then
  echo "ERROR: could not check pool $poolnm status"
  echo "create the pool first, then run the test"
  exit $NOTOK
fi

# kill off any straggler processes on remote hosts

hostcount=${#clients[*]}
if [ $hostcount == 0 ] ; then
  echo "no RADOS client list found"
  exit $NOTOK
fi
radoscmd="rados -c $conffile -p $poolnm "
par-for-all.sh $client_list 'killall -q rados_object_perf.py || echo -n'
if [ $wltype = "read" ] ; then
  par-for-all.sh $osd_list 'sync ; echo 3 > /proc/sys/vm/drop_caches'
fi
par-for-all.sh $client_list 'killall -q rados_object_perf.py || echo -n'
sleep 1
echo "make sure rados_object_perf.py on clients is same as we have here"
for c in ${clients[*]} ; do 
  rsync -ravu rados_object_perf.py $c:
done

# create a RADOS object to maintain shared state
$radoscmd rm $perf_obj
$radoscmd create $perf_obj
# threads do not start workload until all threads are ready
$radoscmd setxattr $perf_obj threads_ready 0
# threads stop measuring once threads_done is non-zero
$radoscmd setxattr $perf_obj threads_done 0

# compute think time
think_time=''
if [ $wltype == "read" -o $wltype == "create" ] ; then 
  # FIXME: need to take into account different hardware configs
  (( think_time = $threads - 1 ))
fi

# start threads

hx=0
pids=''
targethost=()
cmd=()
n=0
for padded_n in `seq -f "%03g" 1 $threads` ; do 
  (( n = $n + 1 ))
  host=${clients[${hx}]}
  # compute host index as thread number modulo hostcount
  ((hx = $hx + 1))
  if [ $hx -ge $hostcount ] ; then hx=0 ; fi

  # launch next thread
  rsptimepath="/tmp/rados-wl-thread-${padded_n}.csv"
  next_launch="ssh $host 'RSPTIME_CSV=$rsptimepath ./rados_object_perf.py $conffile $poolnm $threads $duration $objsize $objcount $wltype $n $threads $think_time'"
  cmd[$unpadded_n]="$next_launch"
  echo "$next_launch"
  eval "$next_launch" > $logdir/rados-wl-thread-$padded_n.log &
  pids="$pids $!"  # save next thread PID
  # throttle launches so ssh doesn't lock up
  if [ $hx = 0 ] ; then sleep 1 ; fi
done 

# wait for them to finish, report if problem

n=0
for p in $pids ; do 
  (( n = $n + 1 ))
  wait $p
  s=$?
  if [ $s != $OK ] ; then 
    echo "pid $p returns status $s from host ${targethost[${n}]} for cmd:  ${cmd[$n]}"
  fi
done

# output logs and summary

hx=0
for padded_n in `seq -f "%03g" 1 $threads` ; do 
  host=${clients[${hx}]}
  ((hx = $hx + 1))
  echo
  echo "--- $host thread $padded_n ---"
  rsptimepath="/tmp/rados-wl-thread-${padded_n}.csv"
  scp -q $host:$rsptimepath $logdir/
  cat $logdir/rados-wl-thread-$padded_n.log
  if [ $hx -ge $hostcount ] ; then hx=0 ; fi
done

# record aggregate result

( echo ; echo "SUMMARY" ; echo "------" ; \
echo -n "object access rate (objs/sec): "  ; \
grep 'throughput' $logdir/rados-wl-thread-*log | awk '{ sum += $(NF-1) }END{print sum}' ; \
echo -n "data transfer rate (MB/sec): "  ; \
grep 'transfer rate' $logdir/rados-wl-thread-*log | awk '{ sum += $(NF-1) }END{print sum}' ) \
  | tee -a $logdir/summary.log
$radoscmd rm $perf_obj

