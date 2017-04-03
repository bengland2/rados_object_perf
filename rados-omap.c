/* program to test omap writes of very large lists of key-value pairs
 * to compile & link:
 *   # cc -g -o rados-omap rados-omap.c -lrados
 * to run:
 *   # ./rados-omap
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <rados/librados.h>

#define NULLCP ((char * )0L)
#define NULL_DELIM ((char )0)

static rados_t cluster;
static int cluster_exists = 0;
static rados_ioctx_t io;
static int io_exists = 0;

enum omap_optype {
   omap_read = 0,
   omap_write = 1
};

enum int_parse_type {
	signed_int=0,
	non_negative=1,
	positive=2
};
	
static char * op_type_strs[] = { "read", "write" };

/* you can put your fprintf statement as first parameter */

static void cleanup(int fprintf_result)
{
	if (io_exists) rados_ioctx_destroy(io);
	if (cluster_exists) rados_shutdown(cluster);
	fprintf(stderr, 
		"\nusage: rados-omap --operation read|write --kvpairs-per-call <int> --total-kvpairs <int> --value-size <int> \n");
        exit(EXIT_FAILURE);
}

int parse_int( const char * v, const char * msg, enum int_parse_type parse_type )
{
	int result = atoi(v);
	if (result == 0 && strcmp(v, "0"))
		cleanup(
		  fprintf(stderr, "%s: not an integer", v));
	if (parse_type == non_negative) {
		if (result >= 0) return result;
	} else if (parse_type == positive) {
		if (result > 0) return result;
	} else return result;
	cleanup(
	  fprintf(stderr, "%s: not a positive integer value", v));
	return -5;
}

/* construct the key-value pairs to input to librados */

void mk_kvpairs(const int kvpair_num, 
		const int starting_key,
		const int value_size,
		char ***keys_out, 
		char ***values_out, 
		size_t **lens_out )
{
	const int safety_margin = 10;  /* allocate a little more than you need */
        char ** keys = (char ** )calloc(kvpair_num+1, sizeof(char * ));
	char ** vals = (char ** )calloc(kvpair_num+1, sizeof(char * ));
	size_t * lens = (size_t * )calloc(kvpair_num+1, sizeof(size_t));
	int i, k;

	if (!keys || !vals || !lens)
		cleanup(fprintf(stderr, "could not allocate %d key-value pair arrays", kvpair_num));

	for (i=0; i<kvpair_num; i++) {
		char * cp2;

		lens[i] = value_size;

		keys[i] = (char * )malloc(30);
		sprintf(keys[i], "%08d", starting_key+i);

		vals[i] = (char * )malloc(value_size+safety_margin);
		for (cp2 = vals[i], k=0; k<value_size; k++, cp2++) {
			*cp2 = '0' + ((starting_key + i + k) % 10);
		}
		*cp2 = NULL_DELIM;
	}
	*keys_out = keys;
	*values_out = vals;
	*lens_out = lens;
}

struct timespec time_now(void)
{
	struct timespec now;

	clock_gettime(CLOCK_REALTIME, &now);
	return now;
}

int main (int argc, const char **argv)
{

        /* Declare the cluster handle and required arguments. */
	uint64_t rados_create2_flags = 0;
        char cluster_name[] = "ceph";
        char user_name[] = "client.admin";
        rados_ioctx_t io;
        char *poolname = "ben";
	rados_write_op_t my_write_op;
        int err;
        char ** keys = NULL;
	char ** vals = NULL;
	size_t * lens = NULL;
	int kvpairs_per_call = 1, total_kvpairs = 10;
	int value_size = 2;
	enum omap_optype optype = omap_write;
	int arg = 1;
	int i, k;
	int debug = (getenv("DEBUG") != NULL);
	const char * prmname, * prmval;
	struct timespec t0, tf, delta;

	/* parse command line */

	while (arg < argc) {
		if (strncmp(argv[arg], "--", 2))
			cleanup(fprintf(stderr, "%s: not a valid parameter name", argv[arg]));
		if (arg == argc - 1)
			cleanup(fprintf(stderr, "%s: no parameter value seen", argv[arg]));
		prmname = &argv[arg][2];
		prmval = argv[arg+1];
		if (!strcmp(prmname, "total-kvpairs"))
			total_kvpairs = parse_int(prmval, prmname, non_negative);
		else if (!strcmp(prmname, "kvpairs-per-call"))
			kvpairs_per_call = parse_int(prmval, prmname, positive);
		else if (!strcmp(prmname, "value-size"))
			value_size = parse_int(prmval, prmname, non_negative);
		else if (!strcmp(prmname, "operation")) {
			if (!strcmp(prmname, "read")) {
				optype = omap_read;
				cleanup(fprintf(stderr, "%s: read not yet supported\n", prmname));
			} else if (!strcmp(prmname, "write"))
				optype = omap_write;
			else cleanup(fprintf(stderr, "%s: invalid operation type", prmname));
		} else cleanup(fprintf(stderr, "--%s: invalid parameter name", prmname));
		arg += 2;
	}
	printf("%11d : key-value pairs per call\n", kvpairs_per_call);
	printf("%11d : total key-value pairs\n", total_kvpairs);
	printf("%11d : value size in bytes\n", value_size);
	printf("%11s : operation type\n", op_type_strs[(int )optype]);

        /* Initialize the cluster handle with the "ceph" cluster name and the "client.admin" user */

        err = rados_create2(&cluster, cluster_name, user_name, rados_create2_flags);
        if (err)
                cleanup(
		  fprintf(stderr, "%s: Couldn't create the cluster handle! %s\n", 
			argv[0], strerror(-err)));
	cluster_exists=1; /* release this before exiting */

        /* Read a Ceph configuration file to configure the cluster handle. */

        err = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
        if (err)
                cleanup(
		  fprintf(stderr, "%s: cannot read config file: %s\n", 
			argv[0], strerror(-err)));

        /* Read command line arguments */

        err = rados_conf_parse_argv(cluster, argc, argv);
        if (err)
                cleanup(
		  fprintf(stderr, "%s: cannot parse command line arguments: %s\n", 
			argv[0], strerror(-err)));

        /* Connect to the cluster */

        err = rados_connect(cluster);
        if (err)
                cleanup(
		  fprintf(stderr, "%s: cannot connect to cluster: %s\n", 
			argv[0], strerror(-err)));

	/* create an I/O context handle */

        err = rados_ioctx_create(cluster, poolname, &io);
        if (err)
                cleanup(
		  fprintf(stderr, "%s: cannot open rados pool %s: %s\n", 
			argv[0], poolname, strerror(-err)));
	io_exists = 1; /* release this before exit */

        /* Write data to the cluster synchronously. */

        err = rados_write(io, "hw", "Hello World!", 12, 0);
        if (err)
                cleanup(
		  fprintf(stderr, "%s: Cannot write object \"hw\" to pool %s: %s\n", 
			argv[0], poolname, strerror(-err)));

        /* write to the omap for this object */

	t0 = time_now();
	for (i=0; i<total_kvpairs; i += kvpairs_per_call) {
		mk_kvpairs(kvpairs_per_call, i, value_size, &keys, &vals, &lens);
		my_write_op = rados_create_write_op();
		if (!my_write_op)
			cleanup(fprintf(stderr, "cannot create write op\n"));
		rados_write_op_omap_set(my_write_op, 
			(const char ** ) keys, (const char ** )vals, (const size_t * )lens, 
			(size_t )kvpairs_per_call);
		if (debug)
			for (k=0; k<kvpairs_per_call; k++)
				printf(" key %s val %s len %lu\n", 
					keys[k], vals[k], lens[k]);
		err = rados_write_op_operate2(my_write_op, io, 
					"hw", NULL, LIBRADOS_OPERATION_NOFLAG);
		if (err)
			cleanup(
			  fprintf(stderr, "cannot write omap to object 'hw'\n"));
		rados_release_write_op(my_write_op);
	}
	tf = time_now();
	delta.tv_sec = tf.tv_sec - t0.tv_sec;
	delta.tv_nsec = tf.tv_nsec - t0.tv_nsec;
	if (tf.tv_nsec < t0.tv_nsec) {
		delta.tv_sec -= 1;
		delta.tv_nsec += 1000000000; /* 1 second in nanosec */
	}
	printf("elapsed time = %lu.%09lu sec\n", delta.tv_sec, delta.tv_nsec);
	return 0;
}
