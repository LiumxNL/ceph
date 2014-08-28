
#include "lightfs.hpp"
#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"

#include <iostream>
#include <stdio.h>

#define TEST_EQUAL(func, rval) \
{ \
  int r = (func);  \
  if (r == (rval)) \
    printf("[PASSED]: %s == %d\n", #func, (rval)); \
  else  \
    printf("[FAILED]: %s != %d, r = %d\n", #func, (rval), r); \
}

#define PART_LINE(str) printf("====================   %s   ====================\n",(str));

using namespace librados;

namespace lightfs {

  int init_ceph_cluster(Rados &rados, IoCtx &ioctx)
  {
    int r;

    r =  rados.init("admin");
    if (r < 0) {
      cerr << "cannot init rados" << endl;
      return r;
    }

    r = rados.conf_read_file("/etc/ceph/ceph.conf");
    if (r < 0) {
      cerr << "cannot open conf" << endl;
      return r;
    }

    r = rados.connect();
    if (r < 0) {
      cerr << "Cannot connect cluster" << endl;
      return r;
    }

    r = rados.ioctx_create("lfs_test", ioctx);
    if (r < 0) {
      cerr << "Cannot open lfs_test pool" << endl;
      return r;
    }
    return 0;
  }

  void print_inode(inode_t &inode)
  {
    cout << "[" << inode.ctime;
    cout << ", " << inode.atime;
    cout << ", " << inode.mtime;
    cout << ", " << inode.size ;
    cout << ", " << inode.max_size ;
    cout << ", " << inode.truncate_size ;
    cout << ", " << oct << inode.mode << dec ;
    cout << ", " << inode.uid ;
    cout << ", " << inode.gid ;
    cout << "]" << endl;
  }

  void print_fh(Fh &fh)
  {
    cout << "[" << fh.ino;
    cout << ", ";
    print_inode(*(fh.inode));
    cout << ", " << fh.pos;
    cout << ", " << fh.mode;
    cout << ", " << fh.flags;
    cout << "]" << endl;
  }
}
