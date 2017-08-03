#!/usr/bin/python
#
# rados_object_perf.py - program to test performance of librados python binding
# to get online help:
#  # ./rados_object_perf.py -h
# to run a single thread:
#  # ./rados_object_perf.py
# to run multiple threads, use rados-obj-perf.sh
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
key_prefix = 'key'
omap_obj_name = 'omap_object'

# must define globals at present to make them visible to call back routines,
# FIXME: there is a better way (lambda?)
# remember you have to use "global" statement to modify
# these inside a subroutine.

rqs_done = 0
last_checked = 0  # save previous value to compute delta
check_every = 0   # check every so often to see if a thread finished
rqs_posted = 0 # increment every time an async request is posted
measurement_over = False  # true after first thread finishes
units_done = 0
done_checks = 0  # how many times we check to see if other threads are done
ioctx = None

# declare  command line parameters up front with defaults 
# so they have scope 
# some of them are specific to a particular workload type
# so defer the defaulting to when we know what the workload type is

ceph_conf_file = '/etc/ceph/ceph.conf'
keyring = '/etc/ceph/ceph.client.%s.keyring'
username = 'admin'
keyring_path = keyring % username
mypool = 'rados_object_perf'
aio_qdepth = 1
duration = 0
objsize = None
objcount = None
omap_kvpairs_per_call = None
omap_key_count = None
omap_value_size = None
optype = 'cleanup'
unit = 'object'
thread_id = ''
threads_total = 1
think_time_sec = 0.0
adjusting_think_time = False
output_json = False
rsptime_path = None
transfer_unit = 'MB'
threads_done_fraction = 0.1

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


# for writes, increment rqs done, again this is only routine touching it

def on_wr_rq_done(completion):
  global rqs_done
  rqs_done += 1


# count number of threads ready or done

def count_threads_in_omap(omap_obj):
  with rados.ReadOpCtx() as op:
    omaps, ret = ioctx.get_omap_vals(op, "", "", -1)
    ioctx.operate_read_op(op, omap_obj)
    # can't use len(keys) because keys is a generator
    ct = 0
    for (k, _) in omaps:
      if debug: print('in omap for %s: %s' % (omap_obj, k))
      ct += 1
    return ct


# have threads wait different amounts of time for lock

def backoff_lock():
  delay=(1.0 + int(thread_id)/100.0)
  if debug: print('starting gun lock retry in %f sec' % delay)
  time.sleep(delay)


# wait for all threads to arrive at starting line

def await_starting_gun():
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

def post_done():
  if len(thread_id) == 0: return # skip if only 1 thread
  with rados.WriteOpCtx() as op:
    ioctx.set_omap(op, (thread_id,), (b'',))
    ioctx.operate_write_op(op, threads_done_obj)


# check every so often to see if a thread has finished
# but not too often or we'll slow down Ceph
# we estimate how long to wait 
# based on a metric that incorporates both objects and aggregate data processed

def object_time_estimator(obj_cnt_in):
  return  obj_cnt_in * (objsize + 1000000)

def omap_time_estimator(kvpair_cnt_in):
  return kvpair_cnt_in * (omap_value_size + 10000)

def objlist_time_estimator(obj_cnt_in):
  return obj_cnt_in

# see if any other threads have finished their assigned objects

def other_threads_done():
    thrds_done = count_threads_in_omap(threads_done_obj)
    if debug: print ('threads done = %d' % thrds_done)
    return (thrds_done > (threads_total * threads_done_fraction))


# measurement is over for this thread when
# a specified fraction of other threads finish
# if there is only 1 thread then this can never happen
# think time is never adjusted if there is only one thread

def check_measurement_over(start_time, time_estimator):
  global last_checked, measurement_over, think_time_sec, units_done, done_checks

  next_elapsed_time = append_rsptime( response_times, start_time )
  if adjusting_think_time:
    think_time_sec = adjust_think_time(units_done, sampled_rsp_times, next_elapsed_time)

  #if debug & 8: print('check_meas_over: thread_id %s' % thread_id)
  if (threads_total == 1) or not measurement_over:
    units_done += 1

    # decide if it's time to check again

  if (threads_total > 1) and not measurement_over:
    est_cost = time_estimator(units_done)
    done_checks += 1
    if debug: print('est cost = %d time units' % est_cost)
    if (est_cost - last_checked) > check_every:
      last_checked = est_cost
      measurement_over = other_threads_done()
    if duration_based_exit(start_time, duration):
      measurement_over = True
    if measurement_over: # if this call detected that it was over
      elapsed_time = time.time() - start_time

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


# adjust think time based on total threads and last response time
# inputs:
#   rqnum - request number
#   prev_rsp_times - fixed-length array of response times from previous requests
#                    we treat it as a ring buffer
#   rsp_time - floating point response time from this just-finished request

def adjust_think_time(rqnum, prev_rsp_times, rsp_time):
  sample_ct = len(prev_rsp_times)
  avg = 0.0
  avg = reduce(lambda x, y: x+y, prev_rsp_times)
  prev_rsp_times[rqnum % sample_ct] = rsp_time
  avg += rsp_time
  avg /= (sample_ct + 1)  # convert from total to average
  next_think_time = avg * threads_total / 3.0   # 
  if debug: print('prev_rsp_times %s avg %f next_think_time %f' % (str(prev_rsp_times), avg, next_think_time))
  return next_think_time

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
  print('--omap-key-count keys (default 128)')
  print('--omap-value-size bytes (default 16)')
  print('--omap-kvpairs-per-call (default 1)')
  print('--request-type [write|read|list|omap-write|omap-read|cleanup]')
  print('--thread-id string (default thr1)')
  print('--thread-total (default 1)')
  print('--output-format json (default is text)')
  print('--response-time-file path')
  print('--transfer-unit MB|MiB (default is MB)')
  print('--adjust-think-time true|false (default false)')
  print('--threads_done_percent percentage')
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
    if optype == 'omap-write' or optype == 'omap-read':
      unit = 'kvpair'
      # establish defaults
      if not omap_key_count: omap_key_count = 16
      if not omap_value_size: omap_value_size = 32
      if optype == 'omap-write':
        if not omap_kvpairs_per_call: omap_kvpairs_per_call = 1 
    elif optype == 'write' or optype == 'read' or optype == 'list' or optype == 'cleanup':
      unit = 'object'
      if not objsize: objsize = 4194304
      if not objcount: objcount = 1024
    else:
      usage('invalid request type: %s' % pval)
  elif pname == 'thread-id':
    thread_id = pval
  elif pname == 'thread-total':
    threads_total = int(pval)
  elif pname == 'think-time':
    think_time_sec = float(pval)
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
  elif pname == 'adjust-think-time':
    lc_pval = pval.lower()
    if lc_pval != 'true' and lc_pval != 'false':
      usage('adjust-think-time requires boolean value true or false')
    elif lc_pval == 'true':
      adjusting_think_time = True
    else:
      adjusting_think_time = False
  elif pname == 'threads-done-percent':
    threads_done_fraction = float(pval) / 100.0
  elif pname == 'omap-key-count':
    omap_key_count = int(pval)
  elif pname == 'omap-value-size':
    omap_value_size = int(pval)
  elif pname == 'omap-kvpairs-per-call':
    omap_kvpairs_per_call = int(pval)
  else: usage('--%s: invalid parameter name' % pname)

if threads_total == 1:
  print('disabling think time for single-thread test')
  adjusting_think_time = False
  think_time_sec = 0.0

# display input parameter values (including defaults)

if not output_json:
  print('')
  print('input parameters:')
  print('ceph cluster conf file = %s' % ceph_conf_file)
  print('ceph storage pool = %s' % mypool)
  print('username = %s' % username)
  print('keyring at %s' % keyring_path)
  print('I/O request queue depth = %d' % aio_qdepth)
  if unit == 'kvpair':
    print('omap key count = %d' % omap_key_count)
    print('omap value size = %d' % omap_value_size)
    if omap_kvpairs_per_call:
      print('omap key-value-pairs per call = %d' % omap_kvpairs_per_call)
  else:
    if optype == 'read' or optype == 'write':
      print('RADOS object size = %d' % objsize)
    print('RADOS object count = %d' % objcount)
  print('request type = %s' % optype)
  if threads_total > 1:
    print('thread_id = %s' % thread_id)
    print('total threads in test = %d' % threads_total)
    print('threads-done percent: %f' % (threads_done_fraction * 100.0))
    print('think time (sec) = %f' % think_time_sec)
    print('adjust think time? %s' % adjusting_think_time)
  print('transfer unit: %s' % transfer_unit)
else:
  json_obj = {}
  params = {}
  params['conf_file'] = ceph_conf_file
  params['pool'] = mypool
  params['user'] = username
  params['keyring'] = keyring_path
  params['qdepth'] = aio_qdepth
  if unit == 'kvpair':
    params['omap_key_count'] = omap_key_count
    params['omap_value_size'] = omap_value_size
    if omap_kvpairs_per_call:
      params['omap_kvpairs_per_call'] = omap_kvpairs_per_call
    # check every 1% of time points
    check_every = omap_time_estimator(omap_key_count) / 100
  else:
    if optype == 'read' or optype == 'write':
      params['obj_size'] = objsize
    params['obj_count'] = objcount
    check_every = object_time_estimator(objcount) / 100
  params['rq_type'] = optype
  if threads_total > 1:
    params['thread_id'] = thread_id
    params['total_threads'] = threads_total
    params['threads_done_percent'] = threads_done_fraction * 100.0
    params['think_time'] = think_time_sec
    params['adjust_think_time'] = adjusting_think_time
  params['transfer-unit'] = transfer_unit
  json_obj['params'] = params

if threads_done_fraction <= 0.0 or threads_done_fraction >= 1.0:
  usage('threads-done-percent must be a number in between 0 and 100')
if omap_kvpairs_per_call > 0 and optype != 'omap-write':
  usage('only define omap-kvpairs-per-call for an omap-write test')
max_qdepth_seen = 0
if debug: print('check_every %d time units' % check_every)
response_times = []
sampled_rsp_times = [ 0.01 for k in range (0, 3) ]
per_thread_obj_name = '%s-%s' % (omap_obj_name, thread_id)

# if you add this to ceph.conf file, 
# then you don't need to specify keyring in Rados constructor
#   keyring = /root/ben/ceph.client.admin.keyring
# alternatively don't use cephx

with rados.Rados(conffile=ceph_conf_file, conf=dict(keyring=keyring_path)) as cluster:
    #print cluster.get_fsid()
    pools = cluster.list_pools()
    if not pools.__contains__(unicode(mypool, 'utf-8')):
      cluster.create_pool(mypool) # FIXME: race condition if multiple threads
      print 'created pool ' + mypool
    ioctx = cluster.open_ioctx(mypool)

    # wait until all threads are ready to run

    await_starting_gun()

    # do the workload

    start_time = time.time()
    elapsed_time = -1.0

    if optype == 'write':
      bigbuf = build_data_buf(objsize)
      for j in range(0,objcount):
        objnm = next_objnm(thread_id, j)
        if debug & 1: print('creating %s' % objnm)
        if think_time_sec > 0.0: time.sleep(think_time_sec)
        call_start_time = time.time()
        ioctx.aio_write_full(objnm, bigbuf, oncomplete=on_wr_rq_done)
        await_q_drain()
        check_measurement_over(call_start_time, object_time_estimator)
        #if measurement_over: break
            
    elif optype == 'read':
      for j in range(0,objcount):
        objnm = next_objnm(thread_id, j)
        if think_time_sec > 0.0: time.sleep(think_time_sec)
        call_start_time = time.time()
        ioctx.aio_read(objnm, objsize, 0, oncomplete=on_rd_rq_done)
        next_elapsed_time = append_rsptime( response_times, call_start_time )
        await_q_drain()
        check_measurement_over(call_start_time, object_time_estimator)
        #if measurement_over: break

    elif optype == 'list':
      if debug & 32: print('stats: ' + str(ioctx.get_stats()))
      for o in ioctx.list_objects():
        if think_time_sec: time.sleep(think_time_sec)
        if o.key == threads_ready_obj or o.key == threads_done_obj: continue
        if debug: print(o.key)
        units_done += 1
        call_start_time = time.time()
        for a in ioctx.get_xattrs(o.key):
           if debug: print(a)
           v = ioctx.get_xattr(o.key, a)
           print '  %s =  %s' % (a, str(v))
        if units_done > objcount:
          break
        check_measurement_over(call_start_time, objlist_time_estimator)
        #if measurement_over: break

    elif optype == 'omap-write':
      try:
        ioctx.remove_object(per_thread_obj_name)
      except rados.ObjectNotFound:
        pass  # ensure object isn't there so we have fresh omap
      ioctx.write_full(per_thread_obj_name, 'hi there')
      base_key = 0
      value = b''
      while base_key < omap_key_count:
        if think_time_sec: time.sleep(think_time_sec)
        call_start_time = time.time()
        with rados.WriteOpCtx() as op:
          for k in range(omap_kvpairs_per_call):
            omap_key_name = '%s-%09d' % (key_prefix, (omap_kvpairs_per_call - k) + base_key)
            if omap_value_size > 0:
              v = omap_key_name
              while len(v) < omap_value_size: v = v + '.' + v
              value = v[:omap_value_size]
            # syntax weirdometer alert
            ioctx.set_omap(op, (omap_key_name,), (value,))
          ioctx.operate_write_op(op, per_thread_obj_name)
        base_key += omap_kvpairs_per_call
        check_measurement_over(call_start_time, omap_time_estimator)
        #if measurement_over: break

    elif optype == 'omap-read':
      ioctx.read(per_thread_obj_name)
      with rados.ReadOpCtx() as op:
        keycount = 0
        last_key=''
        while True:
          iter, ret = ioctx.get_omap_vals(op, last_key, "", -1)
          assert(ret == 0)
          ioctx.operate_read_op(op, per_thread_obj_name)
          pairs_in_iter = 0
          for (k,v) in list(iter):
            call_start_time = time.time()
            # count omap keys as objects for throughput calculation
            keycount += 1
            if debug: print('%s, %s' % (k, str(v)))
            if k < last_key:
              print('key %s < last key %s' % (k, last_key))
            last_key = k
            pairs_in_iter += 1
            check_measurement_over(call_start_time, omap_time_estimator)
            #if measurement_over: break
          if (pairs_in_iter == 0):
            break
        if keycount < omap_key_count:
          usage('must first write an omap key list at least as long as %d keys' % omap_key_count)

    elif optype == 'cleanup':
      for j in range(0,objcount):
        objnm = next_objnm(thread_id, j)
        call_start_time = time.time()
        try:
          ioctx.remove_object(objnm)
        except rados.ObjectNotFound as e:
          pass
        append_rsptime( response_times, call_start_time )
        # dont want to do check_measurement_over when cleaning up: 
        units_done += 1

    else:
       usage('should have parsed operation type by now')

    # let other threads know that you are done

    post_done()
    ioctx.close()

    # measure throughput

    if elapsed_time < 0.0:
      # for some workload types, 
      # end time is when enough threads exit
      # for cleanup it's not
      elapsed_time = time.time() - start_time
    thru = 0.0
    transfer_rate = 0.0
    if elapsed_time > 0.0:
      thru = units_done / elapsed_time
      if optype == 'omap-write':
        thru *= omap_kvpairs_per_call
        units_done *= omap_kvpairs_per_call
      if optype == "write" or optype == "read":
        if transfer_unit == 'MB':
          transfer_rate = thru * objsize / 1000.0 / 1000.0
        else:
          transfer_rate = thru * objsize / 1024.0 / 1024.0

    # output results in requested format

    if not output_json:
      print('')
      print('results:')
      print('elapsed time = %f' % elapsed_time)
      print('%ss done in measurement interval = %d' % (unit, units_done))
      if adjusting_think_time and (think_time_sec > 0.0):
        print('last_think_time: %f' % think_time_sec)
      if elapsed_time < 0.001:
        usage('elapsed time %f is too short, no stats for you!' % elapsed_time)
      print('throughput = %f %ss/sec' % (thru, unit))
      if optype == "write" or optype == "read":
        print('transfer rate = %f MiB/s' % transfer_rate)
      if done_checks > 0:
        print('checks for test done = %d' % done_checks)
    else:
      results = {}
      results['elapsed'] = elapsed_time
      results['units_done'] = units_done
      results['transfer_rate'] = transfer_rate
      if adjusting_think_time and (think_time_sec > 0.0):
        results['last_think_time'] = think_time_sec
      json_obj['results'] = results
      print(json.dumps(json_obj, indent=4))

    # save response time data if desired

    if rsptime_path:
      with open(rsptime_path, "w") as rspf:
        for (call_start, call_duration) in response_times:
          rspf.write('%f, %f\n' % (call_start, call_duration))
  
