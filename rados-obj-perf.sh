#!/bin/bash
#
# rados-obj-perf.sh - script to launch distributed test
# using threads running rados_object_perf.py
#
# GNU V2 license at
#   https://github.com/bengland2/rados_object_perf/blob/master/LICENSE
#

conffile=/etc/ceph/ceph.conf
client_list=clients.list
osd_list=osds.list
inv=~/to-installer/internal-ansible-hosts

# should not have to edit below this line normally

clients=(`cat $client_list`)
servers=(`cat $osd_list`)
perf_obj=rados_object_perf
poolnm=radosperftest

# parse command line inputs

OK=0
NOTOK=1

usage() {
  echo "ERROR: $1"
  echo "usage: ./rados-obj-perf.sh --obj-size bytes --obj-count objects --threads count --request-type create|read|cleanup"
  exit $NOTOK
}

while [ -n "$1" ] ; do
  if [ -z "$2" ] ; then
     usage "$2: missing parameter value"
  fi
  case $1 in
    --request-type)
      wltype=$2
      ;;
    --obj-size)
      objsize=$2     
      ;;
    --threads)
      threads=$2
      ;;
    --obj-count)
      objcount=$2
      ;;
    --think-time)
      thinktime=$2
      ;;
    *)
      usage "unrecognized parameter name: $1"
      ;;
  esac
  shift
  shift
done

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
echo "think time: $thinktime" ; \
echo "max objects per thread: $objcount" ) \
  | tee $logdir/summary.log

# check that pool exists

rados df -p $poolnm
if [ $? != $OK ] ; then
  echo "ERROR: could not check pool $poolnm status"
  echo "create the pool first, then run the test"
  exit $NOTOK
fi
rados rm -p $poolnm threads_done
rados rm -p $poolnm threads_ready

# kill off any straggler processes on remote hosts

hostcount=${#clients[*]}
if [ $hostcount == 0 ] ; then
  echo "no RADOS client list found"
  exit $NOTOK
fi
radoscmd="rados -c $conffile -p $poolnm "
ansible -i $inv -m shell -a 'killall -q rados_object_perf.py || echo -n' all
if [ $wltype = "read" ] ; then
  ansible -i $inv -m shell -a 'sync ; echo 3 > /proc/sys/vm/drop_caches' osds
fi
ansible -i $inv -m shell -a 'killall -q rados_object_perf.py || echo -n' all
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
#thinktime='0.0'
#if [ $wltype == "read" -o $wltype == "create" ] ; then 
  # FIXME: need to take into account different hardware configs
#  (( thinktime = $threads - 1 ))
#fi

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
  next_launch="ssh $host ./rados_object_perf.py --conf $conffile --pool $poolnm --object-size $objsize --object-count $objcount --request-type $wltype --thread-id $n --thread-total $threads "
  if [ -n "$thinktime" ] ; then
    next_launch="$next_launch --think-time $thinktime"
  fi
  cmd[$unpadded_n]="$next_launch"
  echo "$next_launch"
  (echo "$next_launch" ; eval "$next_launch" ) > $logdir/rados-wl-thread-$padded_n.log &
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

