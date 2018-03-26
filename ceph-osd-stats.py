#!/usr/bin/env python

# script to monitor Ceph OSDs via ceph daemon commands
# and convert counters to meaningful statistics
# to collect a JSON output file from a run, just
# redirect stdout.  For example:
#   ./ceph-osd-stats.py \
#      --samples 3 --poll-interval 10 --host-list-fn hosts.list \
#      > /tmp/osd-stats.json

import os, sys, subprocess, json, time
from subprocess import CalledProcessError

# default values for input parameters

host_list_fn = 'hosts.list'
samples = 2
poll_interval = 5.0

# miscellaneous constants

pct_format = '%3.3f'
undefined_pct = -10.123
param_set = 'perf dump'
bytes_per_GB = 1000000000.0
debug = 0

bluefs_rate_fields = [ 
    'bluefs',            # list element 0 is the outer key in ceph daemon JSON
    'db_used_bytes', 
    'db_total_bytes', 
    'wal_used_bytes', 
    'wal_total_bytes',
    'slow_used_bytes', 
    'slow_total_bytes',
    'files_written_wal', 
    'files_written_sst',
    'bytes_written_wal', 
    'bytes_written_sst',
    'reclaim_bytes', 
    'log_compactions', 
    'log_bytes', 
    'logged_bytes', 
    'num_files' ]

bluestore_rate_fields = [
    'bluestore', 
    'compress_success_count',
    'compress_rejected_count',
    'write_pad_bytes',
    'deferred_write_ops',
    'deferred_write_bytes',
    'write_penalty_read_ops',
    'bluestore_allocated',
    'bluestore_stored',
    'bluestore_compressed',
    'bluestore_compressed_allocated',
    'bluestore_compressed_original',
    'bluestore_onodes',
    'bluestore_onode_hits',
    'bluestore_onode_misses',
    'bluestore_onode_shard_hits',
    'bluestore_onode_shard_misses',
    'bluestore_extents',
    'bluestore_blobs',
    'bluestore_buffers',
    'bluestore_buffer_bytes',
    'bluestore_buffer_hit_bytes',
    'bluestore_buffer_miss_bytes',
    'bluestore_write_big',
    'bluestore_write_big_bytes',
    'bluestore_write_big_blobs',
    'bluestore_write_small',
    'bluestore_write_small_bytes',
    'bluestore_write_small_unused',
    'bluestore_write_small_deferred',
    'bluestore_write_small_pre_read',
    'bluestore_write_small_new',
    'bluestore_txc',
    'bluestore_onode_reshard',
    'bluestore_blob_split',
    'bluestore_extent_compress',
    'bluestore_gc_merged' ]

osd_rate_fields = [
    'osd',  # element 0 is key to get to these fields
    'op',
    'op_in_bytes',
    'op_out_bytes',
    'op_r',
    'op_r_out_bytes',
    'op_w',
    'op_w_in_bytes',
    'op_rw',
    'op_rw_in_bytes',
    'op_rw_out_bytes',
    'subop',
    'subop_in_bytes',
    'subop_w',
    'subop_w_in_bytes',
    'subop_pull',
    'subop_push',
    'subop_push_in_bytes',
    'pull',
    'push',
    'push_out_bytes',
    'recovery_ops',
    'buffer_bytes',
    'history_alloc_Mbytes',
    'history_alloc_num',
    'cached_crc',
    'cached_crc_adjusted',
    'missed_crc',
    'numpg',
    'numpg_primary',
    'numpg_replica',
    'numpg_stray',
    'heartbeat_to_peers',
    'map_messages',
    'map_message_epochs',
    'map_message_epoch_dups',
    'messages_delayed_for_map',
    'osd_map_cache_hit',
    'osd_map_cache_miss',
    'osd_map_cache_miss_low',
    'osd_map_bl_cache_hit',
    'osd_map_bl_cache_miss',
    'stat_bytes',
    'stat_bytes_used',
    'stat_bytes_avail',
    'copyfrom',
    'tier_promote',
    'tier_flush',
    'tier_flush_fail',
    'tier_try_flush',
    'tier_try_flush_fail',
    'tier_evict',
    'tier_whiteout',
    'tier_dirty',
    'tier_clean',
    'tier_delay',
    'tier_proxy_read',
    'tier_proxy_write',
    'agent_wake',
    'agent_skip',
    'agent_flush',
    'agent_evict',
    'object_ctx_cache_hit',
    'object_ctx_cache_total',
    'op_cache_hit',
    'osd_pg_info',
    'osd_pg_fastinfo',
    'osd_pg_biginfo' ]

class CephCountException(Exception):
    pass

def usage(msg):
    print('ERROR: ' + msg)
    print('usage: ceph-osd-stats.py\n  [ --samples positive-int ]\n  [ --host-list-fn pathname ]\n  [ --poll-interval positive-int ]')
    sys.exit(1)

space_record = '                                                        '

def indent(spaces, json_str):
    lines = [ space_record[:spaces] + l for l in json_str.split('\n') ]
    return '\n'.join(lines)

def grab_json_from_osd( target_host, osd_num, param_set_name ):
    cmd = 'ssh %s ceph daemon osd.%d %s' % (
        target_host, osd_num, param_set_name)
    json_obj = None
    try:
        json_out = subprocess.check_output( cmd.split() )
        json_obj = json.loads(json_out)
    except CalledProcessError:
        pass
    return json_obj


def find_osds_in_host( target_host ):
    cmd = 'ssh %s ls -d /var/lib/ceph/osd/*-*' % target_host
    osdnums = []
    try:
        osd_subdirs_raw = subprocess.check_output( cmd.split() )
        osd_subdirs = osd_subdirs_raw.split('\n')
        for d in osd_subdirs:
            if len(d.strip()) == 0:
                continue
            osdnums.append(int(d.split('-')[1].strip()))
    except CalledProcessError:
        pass
    return osdnums


# calculate percentage as a floating point number
# from 2 integers returned by "ceph daemon" command

def pct_used( used, total ):
    if total == 0.0:
        return undefined_pct
    return 100.0 * used / total

# convert a fraction used/total into a percentage 
# and format for readability
# only add the value to the dictionary if the denominator is non-zero

def add_pct_used( stat_set, key, used, total ):
    pct = pct_used(used, total)
    if pct != undefined_pct:
        stat_set[key] = pct_format % pct

# calculate rates on all counters 
# only return non-zero ones

def calc_rates_from_samples( sample1, sample2 ):
    stat_set = {}
    for rate_field_list in [ bluestore_rate_fields, bluefs_rate_fields, osd_rate_fields ] : 
        outer_key = rate_field_list[0]
        for b in rate_field_list[1:] :
            try:
                rate = ( sample1[outer_key][b] - sample2[outer_key][b] ) / poll_interval
                if rate > 0.0:
                    if rate > 1000:
                        rate_str = str(int(rate))
                    elif rate > 10:
                        rate_str = '%7.2f' % rate
                    else:
                        rate_str = '%6.3f' % rate
                    stat_set[outer_key+'.rate.'+b] = rate_str
            except KeyError:
                #print('key %s not seen' % b)
                pass
    
    try:
        bstore1 = sample1['bluestore']
        bstore2 = sample2['bluestore']

        # to calculate cache efficiency cache
        # calculate the rate of cache hits, the rate of cache misses and then
        # calculate the percentage of hits in the interval.
        # for onode cache:

        onode_hits = bstore2['bluestore_onode_hits'] - bstore1['bluestore_onode_hits']
        onode_misses = bstore2['bluestore_onode_misses'] - bstore1['bluestore_onode_misses']
        add_pct_used( stat_set, 'bluestore.onode_hit_pct', onode_hits, onode_hits + onode_misses )

        onode_shard_hits = bstore2['bluestore_onode_shard_hits'] - bstore1['bluestore_onode_shard_hits']
        onode_shard_misses = bstore2['bluestore_onode_shard_misses'] - bstore1['bluestore_onode_shard_misses']
        add_pct_used( stat_set, 'bluestore.onode_shard_hit_pct', onode_shard_hits, onode_shard_hits + onode_shard_misses )

        data_hits = bstore2['bluestore_buffer_hit_bytes'] - bstore1['bluestore_buffer_hit_bytes']
        data_misses = bstore2['bluestore_buffer_miss_bytes'] - bstore1['bluestore_buffer_miss_bytes']
        add_pct_used( stat_set, 'bluestore.data_hit_pct', data_hits, data_hits + data_misses )

        # write_big means that we write a multiple of the min_alloc_size so no journal data write required
        # write small is the opposite
        # measure the percentage big writes

        big_writes = bstore2['bluestore_write_big'] - bstore1['bluestore_write_big']
        small_writes = bstore2['bluestore_write_small'] - bstore1['bluestore_write_small']
        add_pct_used( stat_set, 'bluestore.big_writes',  big_writes, big_writes + small_writes )

    except KeyError:
        pass
    return stat_set


def calc_usage_from_sample( sample ):
    stat_set = {}
    try:
        bluefs = sample['bluefs']
        db_total_bytes = bluefs['db_total_bytes']
        db_used_bytes = bluefs['db_used_bytes']
        add_pct_used( stat_set, 'bluefs.used.db_pct', db_used_bytes, db_total_bytes )

        slow_total_bytes = bluefs['slow_total_bytes']
        slow_used_bytes = bluefs['slow_used_bytes']

        # calculate max spillover percent based on RocksDB partition space

        add_pct_used( stat_set, 'bluefs.rocksdb_max_spillover_pct', slow_total_bytes, db_total_bytes )
        stat_set['bluefs.rocksdb_max_spillover_GB'] = '%7.3f' % (slow_total_bytes / bytes_per_GB)

        # calculate spillover percent based on RocksDB partition space

        add_pct_used( stat_set, 'bluefs.rocksdb_spillover_pct', slow_used_bytes, db_total_bytes )
        stat_set['bluefs.rocksdb_spillover_GB'] = '%7.3f' % (slow_used_bytes / bytes_per_GB)
        
        wal_total_bytes = bluefs['wal_total_bytes']
        wal_used_bytes = bluefs['wal_used_bytes']
        add_pct_used( stat_set, 'bluefs.used.wal_pct', wal_used_bytes, wal_total_bytes )
    except KeyError:
        pass

    try:
        osd = sample['osd']
        osd_total_bytes = osd['stat_bytes']
        osd_used_bytes = osd['stat_bytes_used']
        add_pct_used( stat_set, 'osd.used.space_pct', osd_used_bytes, osd_total_bytes )
    except KeyError:
        pass
    return stat_set


# parse command line parameters
# and display parameter values

debug = os.getenv("DEBUG")

argindex = 1
argct = len(sys.argv)

while argindex < argct:
    if argindex + 1 == argct:
        usage('all parameters must be --param-name value')
    pname = sys.argv[argindex]
    pval = sys.argv[argindex+1]
    argindex += 2
    if not pname.startswith('--'):
        usage('all parameter names must start with "--"')
    pname = pname[2:]
    if pname == 'samples':
        samples = int(pval)
    elif pname == 'poll-interval':
        poll_interval = float(pval)
    elif pname == 'host-list-fn':
        host_list_fn = pval
    else:
        usage('unrecognized parameter name: --%s' % pname)

params = {}
params['samples'] = samples
params['host-list-fn'] = host_list_fn
params['poll-interval'] = poll_interval
print('{')
print('    "recording-parameters": ')
print(indent(8, json.dumps(params, indent=2)))
print('    ,"statistics": { ')
sys.stdout.flush()

# find out what OSDs are in each host

try:
    with open(host_list_fn, 'r') as f:
        host_list = [ h.strip() for h in f.readlines() ]
except IOError:
    usage('could not read ' + host_list_fn)


if debug:
    sys.stderr.write('\n')
    sys.stderr.write('OSDs discovered on hosts:\n')
osds_in_host = {}
for h in host_list:
    osds_in_host[h] = find_osds_in_host(h)
    if debug:
        sys.stderr.write('osds in host %s: %s\n' % (h, ','.join([ str(osdnum) for osdnum in osds_in_host[h] ])))
sys.stderr.flush()
sys.stdout.flush()

# start polling counters

sample_list = []
sample_keys = None
start_time = time.time()
for s in range(0, samples):
    sample_start_time = time.time()
    osd_counters = {}
    for h in host_list:
        for o in osds_in_host[h]:
            json_counters = grab_json_from_osd( h, o, param_set )
            if json_counters:
                osd_counters[o] = json_counters
    sample_list.append(osd_counters)
    all_stats = {}
    if s > 0:
        if not sample_keys: sample_keys = sorted(sample_list[s].keys())
        for o in sample_keys:
            c_prev = None
            c_now = None
            try:
                c_prev = sample_list[s-1][o]
                c_now = sample_list[s][o]
            except KeyError:
                sys.stderr.write('sample %d for OSD %d not seen\n' % (s, o))
                continue
            stat_set = calc_rates_from_samples(c_prev, c_now)
            usage_stat_set = calc_usage_from_sample(c_now)
            for k in usage_stat_set.keys():
                stat_set[k] = usage_stat_set[k]
            all_stats[o] = stat_set

    # wait for next polling time

    sample_end_time = time.time()
    sample_elapsed_time = sample_end_time - sample_start_time
    if sample_elapsed_time > poll_interval:
        sys.stderr.write('WARNING: took %f sec to poll all OSDs\n' % 
                          sample_elapsed_time)
    else:
        sleep_time = poll_interval - sample_elapsed_time
        if debug: 
            sys.stderr.write('sleeping for %6.1f sec\n' % sleep_time)
            sys.stderr.flush()
        time.sleep(sleep_time)

    # print out this polling interval's stats

    sys.stderr.flush()
    if s > 0:
        if s > 1:
            print('       ,"%d": {' % s)
        else:
            print('       "%d": {' % s)
        print('            "osds": ')
        all_stats['time-after-start'] = \
                              (time.time() - start_time) - poll_interval
        print(indent(12, json.dumps(all_stats, indent=4, sort_keys=True)))
        print('       }')
        sys.stdout.flush()
# close off statistics
print('    }')
# close off data collection
print('}')

