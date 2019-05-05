#!/usr/bin/env python
#
# script to compute how much data must be backfilled if we lose a node
# assumption: only 1 OSD per node
# Ceph placement groups must be placed on permutations shown below
# so by computing what happens to permutations we can approximate 
# how much data must be backfilled when placement groups adjust to the loss
# of a node.

from itertools import permutations
from math import factorial as f
from sys import argv, exit

def usage(msg):
    print('ERROR: %s' % msg)
    print('usage: backfill.py node-count replica-count')
    exit(1)

if len(argv) < 3:
    usage('too few command line parameters')
try:
    node_count = int(argv[1])
    replication = int(argv[2])
except ValueError as e:
    usage(str(e))

# N choose M
combos = f(node_count) // (f(replication) * f(node_count - replication))
print('combinations of %d nodes taken %d at a time: %d' % (node_count, replication, combos))

# without loss of generality, see how many permutations are lost when we remove 
# node 1
all_perms = []
remaining = []
permute_iterator = permutations(range(1,node_count+1), replication)
for p in permute_iterator:
  list_p = list(p)
  all_perms.append(list_p)
  if not list_p.__contains__(1):
    remaining.append(list_p)
all_len = len(all_perms)
remaining_len = len(remaining)

print('%d permutations' % all_len)
for p in all_perms:
  print(p)
print('')
print('%d permutations remaining:' % remaining_len)
for p in remaining:
  print(p)

lost = all_len - remaining_len
lost_pct = 100.0 * lost / all_len
print('backfilling required for %6.2f percent of data' % lost_pct)

free_space_needed = lost_pct / (node_count - 1)
print('free space needed on remaining OSDs = %6.2f' % free_space_needed)

