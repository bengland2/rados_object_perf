# program from Sebastien Han to populate a RADOS omap and read it
# obtained from 
# https://gist.github.com/jd/e679e7c43a0d8e54181b257e8f733c97

import cradox as rados

POOL_NAME = "gnocchi-test"
CONFFILE = ""
USERNAME = ""
OBJECT_NAME = "myomapobject"
ITER = 100

# options['keyring'] = conf.ceph_keyring
# options['key'] = conf.ceph_secret

conn = rados.Rados(conffile=CONFFILE,
                   rados_id=USERNAME)
                   # conf=options)
conn.connect()
ioctx = conn.open_ioctx(POOL_NAME)

print("Writing %d OMAP keys..." % ITER)
for i in range(ITER):
    with rados.WriteOpCtx() as op:
        ioctx.set_omap(op, ("foobar-%d" % i,), (b"",))
        ioctx.operate_write_op(op, OBJECT_NAME,
                               flags=rados.LIBRADOS_OPERATION_SKIPRWLOCKS)
print("DONE")

print("Getting OMAP keys")
with rados.ReadOpCtx() as op:
    omaps, ret = ioctx.get_omap_vals(op, "", "", -1)
    print(len(omaps), ret)
print("DONE")
