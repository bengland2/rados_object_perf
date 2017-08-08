# rados_object_perf
distributed RADOS workload generator for scalable Ceph performance testing

## Why another test

* This test supports fully distributed operation, with synchronized start and stop of thread measurements
* it is designed to scale very big.
* It supports omaps, which turn out to be quite important to RADOS applications
* It aggregates and analyzes results to some extent, and per-thread outputs are in JSON format 
* A new feature automatically injects think time into threads between each request so  all threads participate equally

## Requirements for operation

This script assumes that you have password-less SSH access to the RADOS client machines that will run rados_object_perf.py.  You must also supply a Ceph configuration file to them so that they can communicate with the Ceph monitors and hence with the rest of the Ceph cluster.   This configuration file can optionally point to a keyring if you use cephx authentication.   Both the configuration file and the keyring can be private.

## To configure

Edit the top of the script above the line that says
"should not have to edit below this line".  The parameters are:

  conffile=/etc/ceph/ceph.conf

is your traditional ceph configuration file that mainly points it to where the monitors are.  The default is /etc/ceph/ceph.conf but feel free to change this.

  client_list=/root/clients.list

is a list of hostnames or IP addresses in a text file, one per line.  The workload threads will be launched on these hosts round-robin.

  osd_list=/root/osds.list

This is a list of OSD hosts, it is only here so that the test can drop cache on the OSD servers before running tests.

By default, the script uses the pool named "radosperftest", which the user must create.  This name was chosen so that you won't accidentally overwrite a storage pool in a production system using this test, since it's unlikely to use this name.  You can change the "poolnm" variable in the script if you like.

## To run

 ./rados-obj-perf.sh -h

For reminder on command line parameters.  An example would be 

 rados-obj-perf.sh --obj-size 4096 --obj-count 4096 --rq-type write --adjust-think-time true --threads 32

This will have 32 threads spread evenly across clients in client.list writing 4096 objects of size 4 KiB.

Parameter names are preceded by **--** .  They are:

- workload-type: **write** or **read** or **omap-write** or **omap-read** or **list** or **cleanup** 
- threads: number of python rados_object_perf.py processes spread across clients
- obj-count: number of objects per thread (only with **write** or **read** or **list** or **cleanup**)
- obj-size: object size in bytes (only with **write** or **read**)
- omap-key-count: number of omap key-value pairs to access
- omap-value-size: size of omap value in bytes
- omap-kvpairs-per-call: use only with **omap-write**, submits batches of key-value pairs
- duration: maximum duration of test in seconds (defaults to zero for unlimited)
- threads-done-percent: measurement stops when this fraction of threads are done

## Results

this test coordinates start and stop of measurement interval so that per-thread throughputs can be meaningfully aggregated.  To do this, it uses RADOS itself to store shared state about the test.  More about this later.  

If the environment variable RSPTIME_ENV is set, then rados_object_perf will dump its measured response times for all requests to a .csv-format file using the pathname in the environment variable.  These can then be post-processed to obtain percentiles, etc.

## Thread synchronization

Since thread startup can take a significant amount of time in a large test, the threads all wait for a "starting gun" to be fired.  each thread adds a key-value pair (with null value) to the threads_ready object in the pool, and they all wait until the desired number of threads have registered in this object.   

Threads periodically check to see if a specified fraction of threads have already finished in a similar way, by registering their own key-value pair in **threads_done** object omap and then periodically counting how many threads have done so.  When enough threads have finished, all threads stop measuring but continue processing all objects requested.  This enables you to issue a **read** test after a **write** test completes with confidence that all expected objects have been created so reads will not fail.

The net effect is that all threads are generating workload in almost exactly the same time interval, so it is valid to generate an aggregate throughput by adding the per-thread throughputs.  This is the same method used by iozone, for example.

## To-do list

- would like rados_object_perf.py to support multi-host mode so that we would not need rados-obj-perf.sh
-- more like smallfile_cli.py
- editing rados-obj-perf.sh should not be necessary, everything should be a parameter with smart defaults
- This test does not yet support random I/O
- Would like to add postprocessing of response times to obtain stats on percentiles.
- convert rados-obj-perf.sh to .py and stop using ssh command, use pdsh or paramiko instead.
