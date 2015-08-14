#!/usr/bin/python
#
# rados_object_perf.py - program to test performance of librados python binding
# using async I/O interface.
# command line parameters:
#  conffile - Ceph configuration file pointing to cluster
#    this file must also specify ceph.client.admin.keyring if cephx authentication
#  pool - ceph storage pool name
#  qdepth - for reads and writes, how many async I/O requests should be queued
#  seconds - max duration of test (0 means unlimited)
#  objsize - size of object in bytes
#  objcount - number of objects accessed by this process
#  workload - one of: create, read, cleanup, list
#    create - creates objects of specified size
#    read - reads specified number of bytes from existing objects
#    cleanup - deletes specified objects - if they do not exist, no error
#    list - read metadata for objects  
#  thread_id - optional, you only need this if you are running multiple threads
#  thread_total - optional, total number of threads in test
#    this allows the process to wait until all threads are ready to run 
#  think_time - optional, wait this long between requests
#
# if you run a multi-threaded test, then measurement stops roughly at the time
# that the first thread finishes
#
import rados, sys, time, socket, os
from rados import Ioctx

debug=0
dbgstr = os.getenv('DEBUG') 
if dbgstr: debug = int(dbgstr)

perf_obj='rados_object_perf'
qdrain_timeout=40000  # in msec

# must define globals at present to make them visible to call back routines,
# FIXME: there is a better way (lambda?)
objcount = 0
objsize = 0
thread_id = ''
rqs_done = 0
threads_total = 0

# used to determine if we should check for a completed thread

last_checked = 0  # save previous value to compute delta
check_every = 0   # check every so often to see if a thread finished

# make a string of the specified number of bytes to write to object

def build_data_buf(sz):
  starting_buf = '0123456789abcdef'
  while len(starting_buf) < sz:
    starting_buf += starting_buf
  return starting_buf[0:sz]

# general-purpose input error handler

def usage(msg):
  print 'ERROR: ' + msg
  print 'usage: rados_object_perf.py conffile object_perf pool qdepth secs objsize objcount [create|read|cleanup|list] thread_id thread_total'
  sys.exit(1)

# for reads, we check that data read was of expected length and increment rqs done
# no race condition here because only this routine modifies rqs_done

def on_rd_rq_done(completion,data_read):
  data_len = len(data_read)
  #print 'complete read %d bytes'%data_len
  assert(data_len == objsize)
  global rqs_done
  rqs_done += 1

# for creates, increment rqs done, again this is only routine touching it

def on_wr_rq_done(completion):
  global rqs_done
  rqs_done += 1

# have threads wait different amounts of time for lock

def backoff_lock():
  delay=(1.0 + int(thread_id)/100.0)
  if debug: print('starting gun lock retry in %f sec'%delay)
  time.sleep(delay)

# wait for all threads to arrive at starting line

def await_starting_gun(ioctx):
  if len(thread_id) == 0: return # skip this unless there are multiple processes running this test
  poll_timeout=(threads_total*0.2) +  5.0  # see backoff_lock delay calculation
  not_yet_counted_myself = True
  while not_yet_counted_myself: 
    try:
      ioctx.write_full(perf_obj+'/locked-by', thread_id)

      # we now have the lock
      thrds_ready = int(ioctx.get_xattr(perf_obj, 'threads_ready'))
      if debug: print('threads ready: %d'%thrds_ready)
      ioctx.set_xattr(perf_obj, 'threads_ready', str(thrds_ready+1))
      ioctx.remove_object(perf_obj+'/locked-by')
      not_yet_counted_myself = False
    except rados.Error as e:
      if debug: print('thread %d retrying starting gun lock'%thread_id);
      backoff_lock()
      continue  # go back and try for the lock again

  # wait until all threads are ready to run

  poll_count=0
  while poll_count < poll_timeout:
    poll_count += 1
    threads_ready=int(ioctx.get_xattr(perf_obj, 'threads_ready'))
    if debug: print('threads_ready now %d'%threads_ready)
    if threads_ready >= threads_total:
      break
    time.sleep(max(threads_total/100, 2))
  if poll_count >= poll_timeout:
     raise Exception('threads did not become ready within %d poll cycles'%poll_timeout)
  if debug: print('thread %s saw starting gun fired'%thread_id)
  time.sleep(2) # give threads time to find out that starting gun has fired

# when thread is done, signal other threads to stop measuring

def post_done(ioctx):
  if len(thread_id) > 0:
    not_yet_posted_completion = True
    while not_yet_posted_completion:
      try:
        ioctx.write_full(perf_obj+'/locked-by', thread_id)
        thrds_done_str = ioctx.get_xattr(perf_obj, 'threads_done')
        thrds_done = int(thrds_done_str) + 1
        ioctx.set_xattr(perf_obj, 'threads_done', str(thrds_done))
        ioctx.remove_object(perf_obj+'/locked-by')
        not_yet_posted_completion = False
      except rados.Error as e:
        if debug: print('thread %d retrying lock'%thread_id)
        backoff_lock()
        continue

# check every so often to see if a thread has finished
# but not too often or we'll slow down Ceph
# we estimate how long to wait 
# based on a metric that incorporates both objects and aggregate data processed

def time_estimator(obj_cnt_in):
  return (obj_cnt_in * (objsize + 1000000))

def other_threads_done(ioctx):
    thrds_done_str = ioctx.get_xattr(perf_obj, 'threads_done')
    thrds_done = int(thrds_done_str)
    if debug: print ('threads done = %d'%thrds_done)
    return (thrds_done > 0)

measurement_over = False
def check_measurement_over(objs_done, ioctx):
  global last_checked, measurement_over
  if debug & 8: print('check_meas_over: thread_id %s'%thread_id)
  if len(thread_id) == 0: return False
  if measurement_over: return True
  est_cost = time_estimator(objs_done)
  if debug: print('est cost = %d time units'%est_cost)
  if (est_cost - last_checked) > check_every:
    last_checked = est_cost
    measurement_over = other_threads_done(ioctx)
  return measurement_over

# every time an async request is posted, we increment rqs_posted
rqs_posted = 0

# wait for the queue size to shrink
# we don't have a way to wait for this event
# so we just sleep for 1 msec for now,
# FIXME: can probably use event to block

def await_q_drain():
  global rqs_posted, max_qdepth_seen
  max_qdepth_seen = max(rqs_posted - rqs_done, max_qdepth_seen)
  if debug & 0x2: print('max_qdepth_seen = %d'%max_qdepth_seen)
  rqs_posted += 1
  timeout = qdrain_timeout
  while (rqs_posted - rqs_done > aio_qdepth) or ((rqs_posted == objcount) and (rqs_done < objcount)):
    time.sleep(0.001)
    #timeout -= 1.0
    #if timeout < 0.0:
    #  print 'ERROR: queue never drained in %f sec!'%(qdrain_timeout/1000)
    #  sys.exit(1)

hostname = socket.gethostname().split('.')[0]

def next_objnm( thread_id, index ):
  return 'o%07d-%s'%(index, thread_id)

def duration_based_exit(start_time_in, duration_in):
  if duration_in == 0: return False
  now = time.time()
  elapsed = now - start_time_in
  if debug & 4:
   print('duration_based_exit: elapsed=%f duration_in=%d'%(elapsed, duration_in))
  return (elapsed >= duration_in)

# append response time to a list
# inputs:
#   response time list
#   start time of preceding call

def append_rsptime( rsptime_list, call_start_time ):
  now = time.time()
  call_duration = now - call_start_time
  rsptime_list.append( (now, call_duration) )
  return call_duration

# parse inputs

if len(sys.argv) < 7:
  usage('not enough command line params')

ceph_conf_file=sys.argv[1]
mypool = sys.argv[2]
aio_qdepth = int(sys.argv[3])
duration = int(sys.argv[4])
objsize = int(sys.argv[5])
objcount = int(sys.argv[6])
optype = sys.argv[7]  # we want this last because we vary it the most
thread_id = ''
think_time = None
think_time_sec = 0.0
if len(sys.argv) > 8:
  thread_id = sys.argv[8]  # this is only used if we run this from some other script
  threads_total = int(sys.argv[9])
  if len(sys.argv) > 10:
    think_time = int(sys.argv[10])
    think_time_sec = think_time / 1000.0  # convert from millisec to sec
print 'conf file %s , pool %s , qdepth %d , duration %d , obj.size %d bytes, obj.count %d'%\
      (ceph_conf_file, mypool, aio_qdepth, duration, objsize, objcount)
if len(thread_id) > 0:
  print('thread ID %s, total threads %d, msec delay between calls %s'%\
    (thread_id, threads_total, str(think_time)))

max_qdepth_seen = 0
# check every 1% of time points
check_every = time_estimator(objcount) / 100
if debug: print('check_every %d time units'%check_every)

response_times = []

# if you add this to ceph.conf file, 
# then you don't need to specify keyring in Rados constructor
#   keyring = /root/ben/ceph.client.admin.keyring
# alternatively don't use cephx

with rados.Rados(conffile=ceph_conf_file) as cluster:
  print cluster.get_fsid()
  pools = cluster.list_pools()
  #print pools
  if not pools.__contains__(mypool):
    cluster.create_pool(mypool)
    print 'created pool ' + mypool
  objs_done = 0
  with cluster.open_ioctx(mypool) as ioctx:

    # wait until all threads are ready to run

    await_starting_gun(ioctx)

    # do the workload

    start_time = time.time()
    elapsed_time = -1.0
    if optype == 'create':
      bigbuf = build_data_buf(objsize)
      for j in range(0,objcount):
        objnm = next_objnm(thread_id, j)
        if debug & 1: print('creating %s'%objnm)
        if think_time: time.sleep(think_time_sec)
        call_start_time = time.time()
        ioctx.aio_write_full(objnm, bigbuf, oncomplete=on_wr_rq_done)
        next_elapsed_time = append_rsptime( response_times, call_start_time )
        if think_time: think_time_sec = (4.0*think_time_sec + next_elapsed_time) / 5.0
        await_q_drain()
        if not (measurement_over or duration_based_exit(start_time, duration)):
          objs_done += 1
        else:
          # measurement is over but keep creating objects so that
          # subsequent tests have all the objects that you expect
          elapsed_time = time.time() - start_time
        check_measurement_over(j, ioctx)
    elif optype == 'read':
      for j in range(0,objcount):
        objnm = next_objnm(thread_id, j)
        if think_time: time.sleep(think_time_sec)
        call_start_time = time.time()
        ioctx.aio_read(objnm, objsize, 0, oncomplete=on_rd_rq_done)
        next_elapsed_time = append_rsptime( response_times, call_start_time )
        if think_time: think_time_sec = (4.0*think_time_sec + next_elapsed_time) / 5.0
        await_q_drain()
        if not (measurement_over or duration_based_exit(start_time, duration)):
          objs_done += 1
        else:
          # measurement is finished but keep accessing objects anyway
          # so that other threads see same response time during rest
          # of their measurement intervals
          elapsed_time = time.time() - start_time
        check_measurement_over(j, ioctx)
    elif optype == 'cleanup':
      for j in range(0,objcount):
        objnm = next_objnm(thread_id, j)
        try:
          ioctx.remove_object(objnm)
        except rados.ObjectNotFound as e:
          pass
        # dont want to do this when cleaning up: 
        # if (duration_based_exit(start_time, duration)): break
        objs_done += 1
    elif optype == 'list':
      if debug & 32: print('stats: ' + str(ioctx.get_stats()))
      objcount = 0
      for o in ioctx.list_objects():
        if think_time: time.sleep(think_time_sec)
        if o.key == perf_obj: continue
        if debug: print(o.key)
        objs_done += 1
        call_start_time = time.time()
        for a in ioctx.get_xattrs(o.key):
           if debug: print(a)
           v = ioctx.get_xattr(o.key, a)
           print '  %s = %s'%(a, str(v))
        append_rsptime( response_times, call_start_time )
        if measurement_over or duration_based_exit(start_time, duration):
          elapsed_time = time.time() - start_time
        if objs_done > objcount:
          break
        check_measurement_over(objs_done, ioctx)
      print 'objects processed = %d'%objs_done
    else:
       usage('invalid operation type, must be CREATE, READ or CLEANUP')

    # measure throughput

    if elapsed_time < 0.0: elapsed_time = time.time() - start_time
    print 'elapsed time = %f , objects requested = %d, objects done in measurement interval = %d'%\
      (elapsed_time, objcount, objs_done)
    if elapsed_time < 0.001:
      usage('elapsed time %f is too short, no stats for you!'%elapsed_time)
    thru = objs_done / elapsed_time
    print 'throughput = %f obj/sec'%thru
    if optype == "create" or optype == "read":
      transfer_rate = thru * objsize / 1024.0 / 1024.0
      print 'transfer rate = %f MB/s'%transfer_rate
      print 'think time converged to %f sec'%think_time_sec
    rsptimefile=os.getenv('RSPTIME_CSV')
    if rsptimefile:
      with open(rsptimefile, "w") as rspf:
        for (call_start, call_duration) in response_times:
          rspf.write('%f, %f\n'%(call_start, call_duration))

    # let other threads know that you are done

    post_done(ioctx)
  
