
//test rados op atomic
#include "lightfs.h"
#include "include/encoding.h"
#include "rados/librados.hpp"
//#include "rados/librados.h"
#include <unistd.h>
#include <map>

using namespace librados;
// watch/notify
//uint64_t max_watch_cookie;
//map<uint64_t, librados::WatchContext *> watchers;

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

int test_notify()
{
  int r = -1;
  const char *oid = "object";
  uint32_t cookie = 101;
  uint64_t ver = 11;
  const char buf[] = "o";//"this is notify";
  int len = sizeof(buf);
  int arg = 10000;
  
  bufferlist bl, inbl, outbl;
  bl.append(buf);
  cout << "bl.c_str = " << bl.c_str() << endl;
  uint32_t prot_ver = 11;
  uint32_t timeout = 8;
  
  ::encode(prot_ver, inbl);
  ::encode(timeout, inbl);
  ::encode(bl, inbl);
  cout << "inbl.c_str = " << inbl.c_str() << "inbl.length = " << inbl.length() << endl;

  //r = rados_notify(ioctx, oid, ver, buf, len);
  //r = rados_exec(ioctx, oid, "lightfs", "lightfs_notify", inbl.c_str(), (1 + strlen(inbl.c_str())), NULL, 0);
  librados::IoCtx ioctx_class;
  ioctx_class.from_rados_ioctx_t(ioctx, ioctx_class);
  string myoid(oid);
  r = ioctx_class.exec(myoid, "lightfs", "lightfs_notify", inbl, outbl);
  cout << "rados_notify = " << r << endl;
  if (r < 0)
    return r;
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
  test_notify(); 
  /*
    7. shutdown cluster
  */
  rados_ioctx_destroy(ioctx);

  rados_shutdown(cluster);

  return 0;
}

