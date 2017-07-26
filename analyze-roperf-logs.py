#!/usr/bin/python

# analyze-roperf-logs.py - reduce per-thread data to aggregate stats

import os, sys, json
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
    print fn
    with open(fn, 'r') as jsonf:
      next_thread = json.load(jsonf)
      tid = next_thread['params']['thread_id']
      threads[tid] = next_thread
      print(json.dumps(next_thread, indent=4))

total_xfer_rate = 0.0
total_objs_done = 0
max_elapsed = 0.0
total_objs_requested = 0
for t in threads.values():
  total_xfer_rate += t['results']['transfer_rate']
  total_objs_done += t['results']['objs_done']
  max_elapsed = max(max_elapsed, t['results']['elapsed'])
  total_objs_requested += t['params']['obj_count']

print('elapsed time: %f' % max_elapsed)
print('total objects done: %d' % total_objs_done)
print('total objects requested: %d' % total_objs_requested)
print('objects/sec average: %f' % (total_objs_done / max_elapsed))
pct_objs_done = 100.0 * total_objs_done / total_objs_requested
print('total transfer rate: %f' % total_xfer_rate)
print('%% objects done: %f' % pct_objs_done)
if pct_objs_done < pct_done_threshold:
  print('WARNING: fewer than %d%% requested objects were processed in measurement interval' % pct_done_threshold)

