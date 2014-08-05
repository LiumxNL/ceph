#include <iostream>


#include "include/rados/librados.hpp"
#include "cls/lightfs/cls_lightfs_client.h"

using namespace std;
using namespace librados;
using namespace lightfs;

int main(int argc, char *argv[])
{
  int r;
  Rados rados;
  IoCtx ioctx;

  r = rados.conf_read_file("/etc/ceph/ceph.conf");
  if (r < 0) {
    cerr << "cannot open conf" << endl;
    goto err;
  }

  r = rados.connect();
  if (r < 0) {
    cerr << "Cannot connect cluster" << endl;
    goto err;
  }

  r = rados.ioctx_create("lfs", ioctx);
  if (r < 0) {
    cerr << "Cannot open lfs pool" << endl;
    goto err;
  }
  cerr << "OK, open" << endl;
err:
  ioctx.close();
  rados.shutdown();
  return 0;
}
