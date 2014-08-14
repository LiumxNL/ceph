
#include "lightfs.hpp"
#include "test.hpp"
#include "cls/lightfs/cls_lightfs_client.h"
#include "include/lightfs_types.hpp"

using namespace std;
using namespace lightfs;
using namespace lightfs::cls_client;

namespace lightfs {

  int test_inogen(IoCtx *ioctx)
  {
    cout << ">>>test_inogen:" << endl;
    int bits = 1; 
    InoGenerator inogen(*ioctx);
    if (inogen.open() < 0) {
      inogen.init_pool(bits);
      inogen.open();
    }

    inodeno_t ino = 0;
    int r = -1;
    int i = 0;
    for (i = 0; i < 4; i++) {
      r = inogen.generate(ino);
      if (r < 0)
	break;
      cout << hex << ino << dec << endl;
    }
    
    return 0;
  }

  void ls_map(std::map<std::string, inodeno_t> &result)
  {
    std::map<string, inodeno_t>::iterator p;
    for (p = result.begin(); p != result.end(); ++p) {
      cout << p->first.substr(2) << endl;
    }
  }

  void ls(IoCtx *ioctx, string &oid)
  {
    cout << "ls " << oid << ":" << endl;
    int r = -1;
    std::map<string, inodeno_t> result;
    r = list_inode(ioctx, oid, "", (uint64_t)-1, &result);
    if (r < 0)
      return;
    ls_map(result);
  }

  void test_fs(IoCtx *ioctx)
  {
    cout << ">>>test_fs:" << endl;
    int bits = 1;
    InoGenerator inogen(*ioctx);
    if (inogen.open() < 0) {
      inogen.init_pool(bits);
      inogen.open();
    }

    Lightfs fs(ioctx, &inogen);
    /*
    test case : 
      	               root=0 
	      /                      \        \
          dir1=1                   dir2=2     dir3=3
          /    \      \             /   \         \
       sub1=11 sub2=12 sub3=13  sub4=14 sub5=15  sub6=16
       /     \
     g1=101 g2=102 
   */
    if (!fs.create_root())
      return;

    PART_LINE("mkdir");
    TEST_EQUAL(fs.mkdir(0, "dir1"), 0);
    TEST_EQUAL(fs.mkdir(0, "dir2"), 0);
    TEST_EQUAL(fs.mkdir(0, "dir3"), 0);

    string root;
    string dir1;
    string dir2;
    string dir3;
    string sub1;
    get_inode_oid(0, root);
    get_inode_oid(1, dir1);
    get_inode_oid(2, dir2);
    get_inode_oid(3, dir3);
    get_inode_oid(0x11, sub1);

    PART_LINE("readdir");
    std::map<std::string, inodeno_t> result;
    TEST_EQUAL(fs.readdir(0, result), 0);
    ls_map(result);

    PART_LINE("rmdir");
    TEST_EQUAL(fs.rmdir(0, "dir3"), 0);
    ls(ioctx, root);
  }

}

int main(int argc, const char *argv[])
{
  int r;
  Rados rados;
  IoCtx ioctx;

  r = init_ceph_cluster(rados, ioctx);
  if (r < 0)
    goto err;

  test_inogen(&ioctx);
  test_fs(&ioctx);

err:
  ioctx.close();
  rados.shutdown();
  return 0;

}

