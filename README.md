# rados_object_perf
distributed RADOS workload generator for scalable Ceph performance testing

Why another test:

Unlike "rados bench", this test supports fully distributed operation, and lets you apply workload to a single storage pool.  This is much closer to the way that typical RADOS applications use storage pools, with one pool for VMs, a different pool for RGW, and a different pool for CephFS.  If you try to run rados bench on multiple hosts against the same pool, you will get errors, because it wasn't designed for this.

Requirements for operation:

This script assumes that you have password-less SSH access to the RADOS client machines that will run rados_object_perf.py.  You must also supply a Ceph configuration file to them so that they can communicate with the Ceph monitors and hence with the rest of the Ceph cluster.   This configuration file can optionally point to a keyring if you use cephx authentication.   Both the configuration file and the keyring can be private.

To configure:

Edit the top of the script above the line that says
"should not have to edit below this line".  The parameters are:

  conffile=/etc/ceph/ceph.conf

is your traditional ceph configuration file that mainly points it to where the monitors are.  The default is /etc/ceph/ceph.conf but feel free to change this.

  poolnm=targetpool

is the Ceph storage pool that you want to use, in this example "targetpool"

  client_list=/root/osds.list

is a list of hostnames or IP addresses in a text file, one per line.  The workload threads will be launched on these hosts round-robin.

  osd_list=/root/osds.list

This is a list of OSD hosts, it is only here so that the test can drop cache on the OSD servers before running tests.

To run:

# ./rados-obj-perf.sh -h

For reminder on command line parameters.  An example would be 

# rados-obj-perf.sh write 131072 32 200 4000

This will have 32 threads spread evenly across clients in client.list writing up to 4000 objects of size 128 KB for at most 200 seconds.

Parameters are:

- workload-type: "write" or "read" 
- object-size: object size in bytes
- threads: number of python rados_object_perf.py processes spread across clients
- duration: maximum duration of test in seconds (can set to zero for unlimited)
- object-count: number of objects per thread

Results:

this test coordinates start and stop of measurement interval so that per-thread throughputs can be meaningfully aggregated.  To do this, it uses RADOS itself to store shared state about the test.  More about this later.  

If the environment variable RSPTIME_ENV is set, then rados_object_perf will dump its measured response times for all requests to a .csv-format file using the pathname in the environment variable.  These can then be post-processed to obtain percentiles, etc.

Thread synchronization:

Since thread startup can take a significant amount of time in a large test, the threads all wait for a "starting gun" to be fired.  The event that triggers the start of all threads is that the "threads_ready" attribute of the "rados_object_perf" object reaches the thread-count parameter above.  So when each thread is getting ready to run, it increments this counter and then waits for it to reach this threshold before beginning the workload.  Once the threshold is reached, it then waits a couple of seconds before beginning the workload, so that all the other threads also have a chance to see that the test has started.  This results in fairly reliable synchronization of all threads.

Although each thread will attempt to process all of the objects specified for 
it, it will stop measuring throughput when the RADOS object "rados_object_perf" xattr "threads_done" exceeds 0 (i.e. detected soon after first thread has finished).   

The net effect is that all threads are generating workload in almost exactly the same time interval, so it is valid to generate an aggregate throughput by adding the per-thread throughputs.  This is the same method used by iozone, for example.

To-do list:

This test does not yet support random I/O, but this is fairly simple to do.

Would like to add postprocessing of response times to obtain stats on percentiles.


