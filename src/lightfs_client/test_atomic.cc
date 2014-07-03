
//test rados op atomic
#include "lightfs.h"

void usage()
{
  stringstream ss; 
  ss << "./test_atomic";
     
  cout << ss.str() << endl;
}

int test_atomic()
{
  int r = -1;
  const char *oid = "myobj";
  rados_write_op_t write = rados_create_write_op();
  const char *a_str = "aaaaaaaa";
  const char *b_str = "bbbbbbbb";
  const char *xattr = "mtime";
  
  //create object
  const char *keys[] = {"name"};
  const char *vals[] = {"value"};
  size_t len1 = strlen("value");
  const size_t lens[] = {len1};
  size_t num = 1;
  rados_write_op_omap_set(write, keys, vals, lens, num); 
  r = rados_write_op_operate(write, ioctx, oid, NULL, LIBRADOS_OPERATION_NOFLAG);
  rados_release_write_op(write);
  cout << "successfully create myobj" << endl;

  cout << "to test atomic..." << endl;

  //do some omap write operation
  write = rados_create_write_op();
  const char *key_1 = "key_1";
  const char *val_1 = "val_1";
  const char *key_2 = "key_2";
  const char *val_2 = "val_2";
  const char *mykeys[] = {key_1};
  const char *myvals[] = {val_1};
  const size_t mylen = strlen(val_1);
  const size_t mylens[] = {mylen};
  int rval = -1;
  rados_write_op_omap_set(write, mykeys, myvals, mylens, 1); 
  //rados_write_op_write(write, b_str, strlen(b_str), 2*4194304);
  //do an error operation, pair <key_2, val_2> not exists
  rados_write_op_omap_cmp(write, key_2, LIBRADOS_CMPXATTR_OP_EQ, val_1, strlen(val_1), &rval);
  r = rados_write_op_operate(write, ioctx, oid, NULL, LIBRADOS_OPERATION_NOFLAG);
  cout << "op operate = " << r << endl;
  cout << "omap cmp = " << rval << endl;
  rados_release_write_op(write);
  return r;
}


int main(int argc, char *argv[])
{

  rados_t cluster;
  const char * userid = "admin";
  const char * pool = "atomic";
  int r = 0;
  /*
    1. create cluster
  */
  r = rados_create(&cluster, userid);
  if ( r < 0) {
    cout << "rados_create error , r = " << r << endl;
    return r;
  }
  
  /*
    2. read conf file to cluster
  */
  r = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
  if ( r < 0 ) {
    cout << "conf read error , r = " << r << endl;
    return r;
  }

  /*
    3. connect to cluster
  */
  r = rados_connect(cluster);
  if ( r < 0 ) {
    cout << "rados connect error , r = " << r << endl;
    return r;
  }
  
  /*
    4. lookup or create pool in cluster
  */
  r= rados_pool_create(cluster, pool);
  if ( r < 0 ) {
    cout << "pool create  error , r = " << r << endl;
    return r;
  }

  /*
    5. in cluster: create ioctx , then do read ,write ......
  */
  r = rados_ioctx_create(cluster, pool, &ioctx);
  if ( r < 0 ) {
    cout << "ioctx create  error , r = " << r << endl;
    return r;
  }
  
  //cout << "parepare cluster & ioctx OK !!!" << endl;

  /*
    6. do sth ......
  */
  test_atomic(); 
  /*
    7. shutdown cluster
  */
  rados_ioctx_destroy(ioctx);

  rados_shutdown(cluster);

  return 0;
}

