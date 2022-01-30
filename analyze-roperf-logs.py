#!/usr/bin/python3

# analyze-roperf-logs.py - reduce per-thread data to aggregate stats

import os, sys, json
from sys import argv

bytes_per_GiB = 1<<30
bytes_per_MiB = 1<<20

def usage(msg):
	print('ERROR: %s' % msg)
	print('usage: analyze-roperf-logs.py --directory path')
	sys.exit(1)

# define default values

directory = None
pct_done_threshold = 70.0

# parse command line

arg_index = 1
while arg_index < len(argv):
  if arg_index + 1 == len(argv): usage('every parameter must have a value ')
  pname = argv[arg_index]
  if not pname.startswith('--'): usage('every parameter name must start with --')
  pname = pname[2:]
  pval = argv[arg_index+1]
  arg_index += 2
  if pname == 'directory':
    if not os.path.isdir(pval):
      usage('%s: not a directory' % pval)
    directory = pval

if not directory:
  usage('you must supply directory containing json results')

threads = {}
contents = os.listdir(directory)
for f in contents:
  if not f.startswith('rados-wl-thread') or not f.endswith('.log'):
    continue
  if f.endswith('.log') and f.startswith('rados'):
    path = os.path.join(directory,f)
    fn_filename = f.split('.')[0]
    thrd_id = int(fn_filename.split('-')[3])
    #print fn
    with open(path, 'r') as jsonf:
      try:
        next_thread = json.load(jsonf)
        threads[thrd_id] = next_thread
      except ValueError:
        print('unable to load from %s' % path)
      #print(json.dumps(next_thread, indent=4))
if len(threads) == 0:
  usage('no thread results found')

# first thread is always thread ID 1
any_thread = threads[1]
op_type = any_thread['params']['rq_type']
if op_type == 'write' or op_type == 'read' or op_type == 'cleanup' or op_type == 'list':
  unit = 'object'
else:
  unit = 'key-value-pair'

total_xfer_rate_MiB = 0.0
total_units_done = 0
max_elapsed = 0.0
min_elapsed = 100000000000.0
total_units_requested = 0
total_data_requested = 0.0
for t in threads.values():
  params = t['params']
  try:
    obj_size = int(params['obj_size'])
  except KeyError:
    obj_size = None
  units_done = t['results']['units_done']
  total_units_done += units_done
  max_elapsed = max(max_elapsed, t['results']['elapsed'])
  min_elapsed = min(min_elapsed, t['results']['elapsed'])
  if unit == 'object':
    total_units_requested += params['obj_count']
    if obj_size is not None:
      total_data_requested += ((obj_size * units_done) / bytes_per_GiB)
  else:
    total_units_requested += params['omap_key_count']
if op_type == 'write' or op_type == 'read':
  #total_xfer_rate += t['results']['transfer_rate']
  total_xfer_rate_MiB = (total_units_done * obj_size / bytes_per_MiB) / min_elapsed

print('max elapsed time: %f' % max_elapsed)
print('min elapsed time: %f' % min_elapsed)
print('total objects done while all threads running: %d' % total_units_done)
print('total objects requested: %d' % total_units_requested)
print('average throughput while all threads running (objs/sec): %f' % (total_units_done / max_elapsed))
if obj_size is not None:
  print('total data requested (GiB): %f' % total_data_requested )
pct_units_done = 100.0 * total_units_done / total_units_requested
if total_xfer_rate_MiB > 0.0:
  print('avg. transfer rate while all threads running (MiB/s): %f' % total_xfer_rate_MiB)
print('%% %ss done: %f' % (unit, pct_units_done))
print('log directory is %s' % directory)
if pct_units_done < pct_done_threshold:
  print('WARNING: fewer than %d%% requested %ss were processed in measurement interval' % (
         pct_done_threshold, unit))

