#!/usr/bin/python3

# analyze-roperf-logs.py - reduce per-thread data to aggregate stats

import os,sys,json
from sys import argv

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
  fn = os.path.join(directory,f)
  if f.endswith('.log') and f.startswith('rados'):
    #print fn
    with open(fn, 'r') as jsonf:
      try:
        next_thread = json.load(jsonf)
        tid = next_thread['params']['thread_id']
        threads[tid] = next_thread
      except ValueError:
        print('unable to load from %s' % fn)
      #print(json.dumps(next_thread, indent=4))
if len(threads) == 0:
  usage('no thread results found')

any_thread_id = [*threads.keys()][0]
any_thread = threads[any_thread_id]
op_type = any_thread['params']['rq_type']
if op_type == 'write' or op_type == 'read' or op_type == 'cleanup' or op_type == 'list':
  unit = 'object'
else:
  unit = 'key-value-pair'

total_xfer_rate = 0.0
total_units_done = 0
max_elapsed = 0.0
total_units_requested = 0
for t in threads.values():
  params = t['params']
  total_units_done += t['results']['units_done']
  max_elapsed = max(max_elapsed, t['results']['elapsed'])
  if unit == 'object':
    total_units_requested += params['obj_count']
  else:
    total_units_requested += params['omap_key_count']
  if op_type == 'write' or op_type == 'read':
    total_xfer_rate += t['results']['transfer_rate']

print('elapsed time: %f' % max_elapsed)
print('total objects done: %d' % total_units_done)
print('total objects requested: %d' % total_units_requested)
print('average throughput: %f' % (total_units_done / max_elapsed))
pct_units_done = 100.0 * total_units_done / total_units_requested
if total_xfer_rate > 0.0:
  print('total transfer rate: %f' % total_xfer_rate)
print('%% %ss done: %f' % (unit, pct_units_done))
print('log directory is %s' % directory)
if pct_units_done < pct_done_threshold:
  print('WARNING: fewer than %d%% requested %ss were processed in measurement interval' % (
         pct_done_threshold, unit))

