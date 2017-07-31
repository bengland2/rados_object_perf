#!/usr/bin/python

import os
import time
import sys
import rados

def usage(msg):
  print('ERROR: ' + msg)
  print('usage: rados-omap-perf.py ')
  print('  --object-name which-object')
  print('  --value-size unsigned ')
  print('  --keys-per-call unsigned ')
  print('  --keys unsigned ')
  print('  --direction read|write|writeread')
  print('  --pool-name my-pool')
  print('  --debug Y|(N)')
  sys.exit(1)

def parse_int(valstr, inttype):
  val = int(valstr)  # can throw exception ValueError
  if inttype == 'positive':
    if val <= 0:
        raise ValueError('must be positive integer')
  elif inttype == 'non-negative':
    if val < 0:
        raise ValueError('must be non-negative integer')
  return val

keys_per_call = 1
key_prefix = 'key'
value_size = 0
total_keys = 10
direction = 'write'
pool_name = 'rbd'
obj_name = 'my-omap-object'
debug = False
think_time = 0.0

k=1
argc = len(sys.argv)
while k < argc:
  if argc - k < 2:
        usage('must supply both parameter name and parameter value')
  if not sys.argv[k].startswith('--'):
        usage('parameter must begin with "--"')
  prmname = sys.argv[k][2:]
  prmval = sys.argv[k+1]
  k += 2

  if prmname == 'keys-per-call':
    try:
      keys_per_call = parse_int(prmval, 'positive')
    except ValueError as e:
      usage('error parsing --%s: %s' % (prmname, e))
  elif prmname == 'value-size':
    try:
      value_size = parse_int(prmval, 'positive')
    except ValueError as e:
      usage('error parsing --%s: %s' % (prmname, e))
  elif prmname == 'object-name':
    obj_name = prmval
  elif prmname == 'keys':
    try:
      total_keys = parse_int(prmval, 'positive')
    except ValueError as e:
      usage('error parsing --%s: %s' % (prmname, e))
  elif prmname == 'key-prefix':
    key_prefix = prmval
  elif prmname == 'direction':
    if prmval != 'read' and prmval != 'write' and prmval != 'writeread':
      usage('--direction can be either set to "read" or "write" or "readwrite"')
    direction = prmval
  elif prmname == 'think-time':
    try:
      think_time = float(prmval)
    except ValueError as e:
      usage('error parsing floating-point --%s: %s' % (prmname, e))
  elif prmname == 'pool-name':
    pool_name = prmval
  elif prmname == 'debug':
    if prmval == 'y' or prmval == 'Y':
      debug = True
    else:
      debug = False
  else:
    usage('--%s: unrecognized parameter name' % prmname)

print('keys: %d' % total_keys)
print('keys-per-call: %d' % keys_per_call)
print('key-prefix: %s' % key_prefix)
print('direction: %s' % direction)
print('value-size: %d bytes' % value_size)
print('pool-name: %s' % pool_name)
print('object-name: %s' % obj_name)
print('think-time: %f sec' % think_time)

conn = rados.Rados(conffile='/etc/ceph/ceph.conf')
conn.connect()
ioctx = conn.open_ioctx(pool_name)

if direction == 'write' or direction == 'writeread':
  try:
    ioctx.remove_object(obj_name)
  except rados.ObjectNotFound:
    pass  # ensure object isn't there

  ioctx.write_full(obj_name, 'hi there')
  time.sleep(5) # give multiple threads time to set up
  start_time = time.time()
  next_power_of_4 = 4
  base_key = 0
  value = b''
  while base_key < total_keys:
    with rados.WriteOpCtx() as op:
      for k in range(keys_per_call):
        omap_key_name = '%s-%09d' % (key_prefix, (keys_per_call - k) + base_key)
        if debug: print('omap key: %s' % omap_key_name)
        if value_size > 0:
          v = omap_key_name
          while len(v) < value_size: v = v + '.' + v
          value = v[:value_size]
        # syntax weirdometer alert
        ioctx.set_omap(op, (omap_key_name,), (value,))
      ioctx.operate_write_op(op, obj_name)
      if think_time > 0.0:
        time.sleep(think_time)
      base_key += keys_per_call

      # we read the entire omap when it reaches 4^k in size
      # this means amortized cost of omap read should be O(N) 

      if direction == 'writeread' and base_key > next_power_of_4:
        if debug: print('next_power_of_4: %d' % next_power_of_4)
        next_power_of_4 *= 4
        read_start_time = time.time()
        read_keycount = 0
        with rados.ReadOpCtx() as read_op:
          read_omap, ret = ioctx.get_omap_vals(read_op, "", "", -1)
          assert(ret == 0)
          ioctx.operate_read_op(read_op, obj_name)
          read_keycount = 0
          for (k, v) in read_omap:
            read_keycount += 1
            if debug: print(k)
          read_end_time = time.time()
          print('read keycount = %d' % read_keycount)
          read_delta_time = read_end_time - read_start_time
          print('elapsed read time: %f' % read_delta_time)
          read_throughput = read_keycount / read_delta_time
          print('read throughput = %f' % read_throughput)
          sys.stdout.flush()
else: 
  print(ioctx.read(obj_name))
  time.sleep(5) # give multiple threads time to set up
  start_time = time.time()
  with rados.ReadOpCtx() as op:
    iter, ret = ioctx.get_omap_vals(op, "", "", -1)
    assert(ret == 0)
    ioctx.operate_read_op(op, obj_name)
    print(obj_name)
    keycount = 0
    last_key=''
    for (k,v) in list(iter):
       keycount += 1
       if debug: print('%s, %s' % (k, str(v)))
       if k < last_key:
           print('key %s < last key %s' % (k, last_key))
    if keycount < total_keys:
      usage('must first write an omap key list at least as long as %d keys' % total_keys)

end_time = time.time()
ioctx.close()

delta_time = end_time - start_time
print('elapsed time: %f' % delta_time)
throughput = total_keys  / delta_time
print('throughput = %f' % throughput)
