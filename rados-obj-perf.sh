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

# should not have to edit below this line normally

clients=(`cat $client_list`)
servers=(`cat $osd_list`)
perf_obj=rados_object_perf
poolnm=radosperftest
wltype=cleanup
threads=1
thinktime=0.1
adjustthink=true
dropcache=True

# parse command line inputs

OK=0
NOTOK=1

usage() {
  echo "ERROR: $1"
  echo "usage: ./rados-obj-perf.sh --obj-size bytes --obj-count objects --threads count --request-type write|read|cleanup --think-time millisec "
  exit $NOTOK
}

cleanup() {
  echo "killing any leftover threads"
  ansible -i clients.list -m shell -a 'killall -q rados_object_perf.py || echo -n' all > /tmp/killthreads 2>&1
  sleep 1
}

myabort() {
  cleanup
  exit $NOTOK
}

trap myabort 1 2 3

if [ -z "$1" ] ; then usage 'no parameters at all?' ; fi
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
    --omap-value-size)
      omapvaluesize=$2
      ;;
    --omap-key-count)
      omapkeycount=$2
      ;;
    --omap-kvpairs-per-call)
      omapkvpairspercall=$2
      ;;
    --think-time)
      thinktime=$2
      ;;
    --adjust-think-time)
      adjustthink=$2
      ;;
    --drop-cache)
      u=`echo $2 | tr '[a-z]' '[A-Z]'`
      if [ "$u" == '0' -o "$u" == 'NO' -o "$u" == "FALSE" ] ; then
        dropcache=False
      else
        dropcache=True
      fi
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
echo "drop cache? $drop_cache" ; \
echo "threads: $threads" ; \
echo "test duration maximum: $duration" ; \
echo "think time: $thinktime" ; \
echo "omap key-value pairs: $omapkeycount" ; \
echo "omap value size: $omapvaluesize" ; \
echo "key-value-pairs per call: $omapkvpairspercall" ; \
echo "max objects per thread: $objcount" ) \
  | tee $logdir/summary.log

# check client list

hostcount=${#clients[*]}
if [ $hostcount == 0 ] ; then
  echo "no RADOS client list found"
  exit $NOTOK
fi

# check that pool exists

radoscmd="rados -c $conffile -p $poolnm "
$radoscmd df
if [ $? != $OK ] ; then
  echo "ERROR: could not check pool $poolnm status"
  echo "create the pool first, then run the test"
  exit $NOTOK
fi
$radoscmd rm threads_done
$radoscmd rm threads_ready

# drop cache if this is a read test

if [ $dropcache = "True" ] ; then
  echo dropping cache
  ansible -i osds.list -m shell -a 'sync ; echo 3 > /proc/sys/vm/drop_caches' osds > /tmp/a 2>&1
fi

# kill off any straggler processes on remote hosts

cleanup

# make sure rados_object_perf.py on clients is same as we have here"

for c in ${clients[*]} ; do 
  rsync -rau rados_object_perf.py $c:
done

# create a RADOS object to maintain shared state
$radoscmd rm threads_done > /tmp/quiet 2>&1
$radoscmd rm threads_ready >> /tmp/quiet 2>&1
#$radoscmd create $perf_obj
# threads do not start workload until all threads are ready
#$radoscmd setxattr $perf_obj threads_ready 0
# threads stop measuring once threads_done is non-zero
#$radoscmd setxattr $perf_obj threads_done 0

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

  # determine rados_object_perf.py command to launch

  rsptimepath="/tmp/rados-wl-thread-${padded_n}.csv"
  l="ssh $host ./rados_object_perf.py --output-format json --response-time-file $rsptimepath" 
  l="$l --conf $conffile --pool $poolnm "
  l="$l --request-type $wltype --thread-id $n --thread-total $threads"
  if [ -n "$objsize" ] ; then
    l="$l --object-size $objsize "
  fi
  if [ -n "$objcount" ] ; then
    l="$l --object-count $objcount"
  fi
  if [ -n "$adjustthink" ] ; then
    l="$l --adjust-think-time $adjustthink"
  fi
  if [ -n "$omapkeycount" ] ; then
    l="$l --omap-key-count $omapkeycount"
  fi
  if [ -n "$omapvaluesize" ] ; then
    l="$l --omap-value-size $omapvaluesize"
  fi
  if [ -n "$omapkvpairspercall" ] ; then
    l="$l --omap-kvpairs-per-call $omapkvpairspercall"
  fi
  if [ -n "$thinktime" ] ; then
    l="$l --think-time $thinktime"
  fi

  # launch next thread

  next_launch="$l"
  cmd[$unpadded_n]="$next_launch"
  #echo "$next_launch"
  eval "$next_launch" > $logdir/rados-wl-thread-$padded_n.log &
  pids="$pids $!"  # save next thread PID
  # throttle launches so ssh doesn't lock up
  if [ $hx = 0 ] ; then sleep 1 ; fi
done 
echo "all threads launched"

# wait for them to finish, report if problem

worst_status=$OK
n=0
for p in $pids ; do 
  (( n = $n + 1 ))
  wait $p
  s=$?
  if [ $s != $OK ] ; then 
    echo "pid $p returns status $s from host ${targethost[${n}]} for cmd:  ${cmd[$n]}"
    worst_status=$s
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
  #cat $logdir/rados-wl-thread-$padded_n.log
  if [ $hx -ge $hostcount ] ; then hx=0 ; fi
done

# record aggregate result

( echo ; echo "SUMMARY" ; echo "------" ; \
  ./analyze-roperf-logs.py --directory $logdir 2>&1) \
  | tee -a $logdir/summary.log

cleanup
exit $worst_status
