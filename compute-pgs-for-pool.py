#!/usr/bin/env python
#
# this script computes the PG count to use for
# a Ceph storage pool, given these input parameters:
#   - number of OSDs used by the pool
#   - OPTIONAL - number of PGs per OSD to use, default 120
# The default value of target_pgs_per_osd assumes
# that this storage pool will consume almost all the space
# in the cluster and do almost all of the I/O.
# for more complex configurations, use the Ceph PG
# calculator
# 
# this pgs function can be imported, or this module
# can be run as a standalone script.
#

import sys, math

target_pgs_per_osd = 120

# OSDs involved per RADOS object in this storage pool
size = 3

def pgs(osds):
    pg_estimate = osds * target_pgs_per_osd // size
    # no point if we don't have enough OSDs to store an object
    assert( osds > size )
    # round up to nearest power of 2
    return math.pow(2, math.ceil(math.log(pg_estimate, 2)))

if __name__ == '__main__':
    o = int(sys.argv[1])
    if len(sys.argv) > 2:
        target_pgs_per_osd = int(sys.argv[2])
    print('for %d OSDs, with target_pgs_per_osd = %d, PG count is %d' % 
          (o, target_pgs_per_osd, pgs(o)))
