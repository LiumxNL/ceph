
//test rados op atomic
#include "lightfs.h"
#include <unistd.h>

void usage()
{
  stringstream ss; 
  ss << "./watch_notify";
     
  cout << ss.str() << endl;
}

void watch_callback(uint8_t opcode, uint64_t ver, void *arg)
{
  cout << "watch_callback ";
  cout << "ver = " << ver << " ";
  cout << "*arg = " << *(int *)arg << endl;
}

int test_watch()
{
  int r = -1;
  const char *oid = "myobj";
  uint64_t exp_ver = 1;
  uint64_t handle = 0;
  int arg = 100;
  
  const char *content = "this is myobj";
  r = rados_write(ioctx, oid, content, strlen(content), 0);
  cout << "rados_write = " << r << endl;
  
  r = rados_watch(ioctx, oid, exp_ver, &handle, watch_callback, (void *)&arg );
  cout << "radow_watch = " << r << endl;
  if (r < 0)
    return r;
  cout << "sleep 100 secondes ..." << endl;
  sleep(100);
  return 0;
}


int main(int argc, char *argv[])
{

  rados_t cluster;
  const char * userid = "admin";
  const char * pool = "watch";
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
  test_watch(); 
  /*
    7. shutdown cluster
  */
  rados_ioctx_destroy(ioctx);

  rados_shutdown(cluster);

  return 0;
}

