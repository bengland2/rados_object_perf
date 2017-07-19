#!/usr/bin/python
#
# rados_object_perf.py - program to test performance of librados python binding
# using async I/O interface.
# command line parameters:
#  conf-file - Ceph configuration file pointing to cluster
#    this file must also specify ceph.client.admin.keyring if cephx authentication
#  pool - ceph storage pool name
#  qdepth - for reads and writes, how many async I/O requests should be queued
#  duration - max duration of test (0 means unlimited)
#  object-size - size of object in bytes
#  object-count - number of objects accessed by this process
#  request-type - one of: create, read, cleanup, list
#    create - creates objects of specified size
#    read - reads specified number of bytes from existing objects
#    cleanup - deletes specified objects - if they do not exist, no error
#    list - read metadata for objects  
#  user - which keyring to use for authx 
#    (e.g. 'openstack' -> /etc/ceph/ceph.client.openstack.keyring)
#  thread-id - optional, you only need this if you are running multiple threads
#  thread-total - optional, total number of threads in test
#    this allows the process to wait until all threads are ready to run 
#  think-time - optional, wait this long between requests
#
# if you run a multi-threaded test, then measurement stops roughly at the time
# that the first thread finishes
#

import rados, sys, time, socket, os, json
from rados import Ioctx

debug=0
dbgstr = os.getenv('DEBUG') 
if dbgstr: debug = int(dbgstr)

qdrain_timeout = 40000  # in msec
threads_ready_obj = 'threads_ready'
threads_done_obj = 'threads_done'
hostname = socket.gethostname().split('.')[0]
poll_timeout = 5

# must define globals at present to make them visible to call back routines,
# FIXME: there is a better way (lambda?)
# remember you have to use "global" statement to modify
# these inside a subroutine.

rqs_done = 0
last_checked = 0  # save previous value to compute delta
check_every = 0   # check every so often to see if a thread finished
rqs_posted = 0 # increment every time an async request is posted
measurement_over = False  # true after first thread finishes
objs_done = 0

# declare  command line parameters up front so they have scope 

ceph_conf_file = '/etc/ceph/ceph.conf'
keyring = '/etc/ceph/ceph.client.%s.keyring'

username = 'admin'
keyring_path = keyring % username
mypool = 'rados_object_perf'
aio_qdepth = 1
duration = 0
objsize = 4194304
objcount = 10
optype = 'cleanup'
thread_id = ''
threads_total = 1
think_time = 0
think_time_sec = 0.0
output_json = False
rsptime_path = None
transfer_unit = 'MB'

# make a string of the specified number of bytes to write to object

def build_data_buf(sz):
  starting_buf = '0123456789abcdef'
  while len(starting_buf) < sz:
    starting_buf += starting_buf
  return starting_buf[0:sz]


# for reads, we check that data read was of expected length and increment rqs done
# no race condition here because only this routine modifies rqs_done

def on_rd_rq_done(completion,data_read):
  global rqs_done
  data_len = len(data_read)
  #print 'complete read %d bytes' % data_len
  assert(data_len == objsize)
  rqs_done += 1


# for creates, increment rqs done, again this is only routine touching it

def on_wr_rq_done(completion):
  global rqs_done
  rqs_done += 1


# count number of threads ready or done

def count_threads_in_omap(omap_obj):
  with rados.ReadOpCtx() as op:
    omaps, ret = ioctx.get_omap_vals(op, "", "", -1)
    ioctx.operate_read_op(op, omap_obj)
    keys = (k for k, __ in omaps)
    # can't use len(keys) because keys is a generator
    ct = 0
    for k in keys:
      if debug: print('in omap for %s: %s' % (omap_obj, k))
      ct += 1
    return ct


# have threads wait different amounts of time for lock

def backoff_lock():
  delay=(1.0 + int(thread_id)/100.0)
  if debug: print('starting gun lock retry in %f sec' % delay)
  time.sleep(delay)


# wait for all threads to arrive at starting line

def await_starting_gun(ioctx):
  if len(thread_id) == 0: return # skip this unless there are multiple processes running this test

  # if multiple threads write to the object, this is harmless
  # just ensuring that object exists before we update its omap

  ioctx.write_full(threads_ready_obj, '%8s\n' % thread_id) # ensure object exists before writing to omap
  ioctx.write_full(threads_done_obj, '%8s\n' % thread_id)  # ensure this object exists too

  # tell other threads that this thread has arrived at the starting gate

  with rados.WriteOpCtx() as op:
    ioctx.set_omap(op, (thread_id,), (b'',))
    ioctx.operate_write_op(op, threads_ready_obj)

  # wait until all threads are ready to run

  poll_count=0
  # calculate delay based on how long it takes to start up threads
  sleep_delay = max(threads_total/10.0, 2)
  while poll_count < poll_timeout:
    poll_count += 1
    threads_ready = count_threads_in_omap(threads_ready_obj)
    if debug: print('threads_ready now %d' % threads_ready)
    if threads_ready >= threads_total:
      break
    if debug: print('waiting %f sec until next thread count check' % sleep_delay)
    time.sleep(sleep_delay)
    if debug: print
  if poll_count >= poll_timeout:
     raise Exception('threads did not become ready within %d polls with interval %f' % (poll_timeout, sleep_delay))
  if debug: print('thread %s saw starting gun fired' % thread_id)
  time.sleep(2) # give threads time to find out that starting gun has fired


# when thread is done, signal other threads to stop measuring

def post_done(ioctx):
  if len(thread_id) == 0: return # skip if only 1 thread
  with rados.WriteOpCtx() as op:
    ioctx.set_omap(op, (thread_id,), (b'',))
    ioctx.operate_write_op(op, threads_done_obj)


# check every so often to see if a thread has finished
# but not too often or we'll slow down Ceph
# we estimate how long to wait 
# based on a metric that incorporates both objects and aggregate data processed

def time_estimator(obj_cnt_in):
  return (obj_cnt_in * (objsize + 1000000))


# see if any other threads have finished their assigned objects

def other_threads_done(ioctx):
    thrds_done = count_threads_in_omap(threads_done_obj)
    if debug: print ('threads done = %d' % thrds_done)
    return (thrds_done > 0)


# measuremeht is over for this thread when any other thread has finished

def check_measurement_over(objs_done, ioctx):
  global last_checked, measurement_over
  if debug & 8: print('check_meas_over: thread_id %s' % thread_id)
  if len(thread_id) == 0: return False
  if measurement_over: return True
  est_cost = time_estimator(objs_done)
  if debug: print('est cost = %d time units' % est_cost)
  if (est_cost - last_checked) > check_every:
    last_checked = est_cost
    measurement_over = other_threads_done(ioctx)
  return measurement_over


# wait for the queue size to shrink
# we don't have a way to wait for this event
# so we just sleep for 1 msec for now,
# FIXME: can probably use event to block

def await_q_drain():
  global rqs_posted, max_qdepth_seen
  max_qdepth_seen = max(rqs_posted - rqs_done, max_qdepth_seen)
  if debug & 0x2: print('max_qdepth_seen = %d' % max_qdepth_seen)
  rqs_posted += 1
  timeout = qdrain_timeout
  while (rqs_posted - rqs_done > aio_qdepth) or ((rqs_posted == objcount) and (rqs_done < objcount)):
    time.sleep(0.001)
    #timeout -= 1.0
    #if timeout < 0.0:
    #  print 'ERROR: queue never drained in %f sec!' % (qdrain_timeout/1000)
    #  sys.exit(1)


# generate next object name for this thread

def next_objnm( thread_id, index ):
  return 'o%07d-%s' % (index, thread_id)


# if user requested duration-based test, see if our time is up

def duration_based_exit(start_time_in, duration_in):
  if duration_in == 0: return False
  now = time.time()
  elapsed = now - start_time_in
  if debug & 4:
   print('duration_based_exit: elapsed=%f duration_in=%d' % (elapsed, duration_in))
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


# general-purpose input error handler

def usage(msg):
  print('ERROR: ' + msg)
  print('usage: rados_object_perf.py ')
  print('--conf ceph-conf-file (default ceph.conf)')
  print('--pool pool-name')
  print('--user username')
  print('--qdepth queue-depth (default 1)')
  print('--duration secs (default 0 means all objects)')
  print('--object-size bytes (default 4MiB)')
  print('--object-count objects (default 10)')
  print('--request-type [create|read|cleanup|list] (default cleanup)')
  print('--thread-id string (default thr1)')
  print('--thread-total (default 1)')
  print('--output-format json (default is text)')
  print('--response-time-file path')
  print('--transfer-unit MB|MiB (default is MB)')
  sys.exit(1)


# parse inputs

arg_index = 1
from sys import argv
while arg_index < len(argv):
  if arg_index + 1 == len(argv): usage('every parameter must have a value ')
  pname = argv[arg_index]
  if not pname.startswith('--'): usage('every parameter name must start with --')
  pname = pname[2:]
  pval = argv[arg_index+1]
  arg_index += 2
  if pname == 'conf':
    ceph_conf_file = pval;
  elif pname == 'pool':
    mypool = pval
  elif pname == 'user':
    username = pval
    keyring_path = keyring % username
  elif pname == 'qdepth':
    aio_qdepth = int(pval)
  elif pname == 'object-size':
    objsize = int(pval)
  elif pname == 'object-count':
    objcount = int(pval)
  elif pname == 'request-type':
    optype = pval
  elif pname == 'thread-id':
    thread_id = pval
  elif pname == 'thread-total':
    threads_total = int(pval)
  elif pname == 'think-time':
    think_time_msec = int(pval)
    think_time_sec = float(think_time_msec) / 1000.0
  elif pname == 'response-time-file':
    rsptime_path = pval
  elif pname == 'output-format':
    if pval != 'json': usage('invalid output format')
    output_json = True
  elif pname == 'transfer-unit':
    if pval == 'MB' or pval == 'MiB':
      transfer_unit = pval
    else:
      usage('transfer unit must be either MB (millions of bytes) or MiB (power-of-2 megabytes)')
  else: usage('--%s: invalid parameter name' % pname)

# display input parameter values (including defaults)

if not output_json:
  print('ceph cluster conf file = %s' % ceph_conf_file)
  print('ceph storage pool = %s' % mypool)
  print('username = %s' % username)
  print('keyring at %s' % keyring_path)
  print('I/O request queue depth = %d' % aio_qdepth)
  print('RADOS object size = %d' % objsize)
  print('RADOS object count = %d' % objcount)
  print('operation type = %s' % optype)
  print('thread_id = %s' % thread_id)
  print('total threads in test = %d' % threads_total)
  print('think time (sec) = %f' % think_time_sec)
else:
  json_obj = {}
  params = {}
  params['conf_file'] = ceph_conf_file
  params['pool'] = mypool
  params['user'] = username
  params['keyring'] = keyring_path
  params['qdepth'] = aio_qdepth
  params['obj_size'] = objsize
  params['obj_count'] = objcount
  params['op_type'] = optype
  params['thread_id'] = thread_id
  params['total_threads'] = threads_total
  params['think_time'] = think_time_sec
  json_obj['params'] = params

max_qdepth_seen = 0
# check every 1% of time points
check_every = time_estimator(objcount) / 100
if debug: print('check_every %d time units' % check_every)

response_times = []

# if you add this to ceph.conf file, 
# then you don't need to specify keyring in Rados constructor
#   keyring = /root/ben/ceph.client.admin.keyring
# alternatively don't use cephx

with rados.Rados(conffile=ceph_conf_file, conf=dict(keyring=keyring_path)) as cluster:
  #print cluster.get_fsid()
  pools = cluster.list_pools()
  #print pools
  if not pools.__contains__(mypool):
    cluster.create_pool(mypool) # FIXME: race condition if multiple threads
    print 'created pool ' + mypool
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
        if debug & 1: print('creating %s' % objnm)
        if think_time: time.sleep(think_time_sec)
        call_start_time = time.time()
        ioctx.aio_write_full(objnm, bigbuf, oncomplete=on_wr_rq_done)
        next_elapsed_time = append_rsptime( response_times, call_start_time )
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
        call_start_time = time.time()
        try:
          ioctx.remove_object(objnm)
        except rados.ObjectNotFound as e:
          pass
        append_rsptime( response_times, call_start_time )
        # dont want to do this when cleaning up: 
        # if (duration_based_exit(start_time, duration)): break
        objs_done += 1
    elif optype == 'list':
      if debug & 32: print('stats: ' + str(ioctx.get_stats()))
      objcount = 0
      for o in ioctx.list_objects():
        if think_time: time.sleep(think_time_sec)
        if o.key == threads_ready_obj or o.key == threads_done_obj: continue
        if debug: print(o.key)
        objs_done += 1
        call_start_time = time.time()
        for a in ioctx.get_xattrs(o.key):
           if debug: print(a)
           v = ioctx.get_xattr(o.key, a)
           print '  %s =  %s' % (a, str(v))
        append_rsptime( response_times, call_start_time )
        if measurement_over or duration_based_exit(start_time, duration):
          elapsed_time = time.time() - start_time
        if objs_done > objcount:
          break
        check_measurement_over(objs_done, ioctx)
      print 'objects processed = %d' % objs_done
    else:
       usage('invalid operation type, must be CREATE, READ or CLEANUP')

    # let other threads know that you are done

    post_done(ioctx)

    # measure throughput

    if elapsed_time < 0.0:
      # for some workload types, end time is when first thread exits
      # for cleanup it's not
      elapsed_time = time.time() - start_time
    thru = 0.0
    transfer_rate = 0.0
    if elapsed_time > 0.0:
      thru = objs_done / elapsed_time
      if optype == "create" or optype == "read":
        if transfer_unit == 'MB':
          transfer_rate = thru * objsize / 1000.0 / 1000.0
        else:
          transfer_rate = thru * objsize / 1024.0 / 1024.0

    # output results in requested format

    if not output_json:
      print(('elapsed time = %f , ' + 
             'objects requested = %d, ' + 
             'objects done in measurement interval = %d') % \
        (elapsed_time, objcount, objs_done))
      if elapsed_time < 0.001:
        usage('elapsed time %f is too short, no stats for you!' % elapsed_time)
      print 'throughput = %f obj/sec' % thru
      if optype == "create" or optype == "read":
        print 'transfer rate = %f MiB/s' % transfer_rate
    else:
      results = {}
      results['elapsed'] = elapsed_time
      results['objs_done'] = objs_done
      results['transfer_rate'] = transfer_rate
      json_obj['results'] = results
      print(json.dumps(json_obj, indent=4))

    # save response time data if desired

    if rsptime_path:
      with open(rsptime_path, "w") as rspf:
        for (call_start, call_duration) in response_times:
          rspf.write('%f, %f\n' % (call_start, call_duration))
  
