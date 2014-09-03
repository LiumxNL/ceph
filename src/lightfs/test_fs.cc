
#include "lightfs.hpp"
#include "test.hpp"
#include "cls/lightfs/cls_lightfs_client.h"
#include "include/lightfs_types.hpp"
#include "lightfs_ctx.h"
#include "lightfs_fuse.hpp"

#include <sys/types.h>

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
    cout << "after inogen" << endl;
    if (inogen.open() < 0) {
      inogen.init_pool(bits);
      inogen.open();
    }
   
    cout << "before create Lightfs " << endl;
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

    cout << "before create_root" << endl;
    if (!fs.create_root())
      return;

    PART_LINE("mkdir");
    mode_t mode = S_IFMT & S_IFDIR;
    inode_t inode;
    inode.mode = mode;
    inodeno_t pino = ROOT_INO;

    TEST_EQUAL(fs.mkdir(pino, "$", NULL, inode), 0);
    TEST_EQUAL(fs.mkdir(pino, ".+", NULL, inode), 0);
    TEST_EQUAL(fs.mkdir(pino, "...", NULL, inode), 0);
  
    TEST_EQUAL(fs.mkdir(pino, "dir1", NULL, inode), 0);
    TEST_EQUAL(fs.mkdir(pino, "dir2", NULL, inode), 0);
    TEST_EQUAL(fs.mkdir(pino, "dir3", NULL, inode), 0);
    TEST_EQUAL(fs.mkdir(pino, "avatar", NULL, inode), 0);
    TEST_EQUAL(fs.mkdir(pino, "batman", NULL, inode), 0);
    TEST_EQUAL(fs.mkdir(pino, "transfrom4", NULL, inode), 0);
    TEST_EQUAL(fs.mkdir(pino, "spiderman", NULL, inode), 0);

    string root;
    get_inode_oid(ROOT_INO, root);

    PART_LINE("readdir");
    std::map<std::string, inodeno_t> result;
    TEST_EQUAL(fs.readdir(pino, result), 0);
    ls_map(result);

    PART_LINE("rmdir");
    TEST_EQUAL(fs.rmdir(pino, "dir3"), 0);
    ls(ioctx, root);

    PART_LINE("lookup");
    inodeno_t l_ino = -1;
    TEST_EQUAL(fs.lookup(pino, "avatar", l_ino), 0);
    cout << "avatar.ino = " << hex << l_ino << dec << endl;

    PART_LINE("rename");
    TEST_EQUAL(fs.rename(pino, "avatar", "AVATAR"), 0);
    ls(ioctx, root);

    PART_LINE("open");
    int flags = O_RDWR|O_CREAT;
    inode_t myinode;
    Fh fh;
    fh.inode = &myinode;
    cout << "open ino: " << hex << l_ino << dec << endl;
    TEST_EQUAL(fs.open(l_ino, flags, &fh), 0);
    print_fh(fh);    

    PART_LINE("write");
    off_t off = 100;
    off_t len = (1<<22);
    string data_obj(len, 'x');
    cout << "length = " << data_obj.length() << endl;
    TEST_EQUAL(fs.write(&fh, off, len, data_obj.c_str()), 0);
  }

  

  void test_fuse(IoCtx *ioctx, int argc, char *argv[])
  {
    cout << ">>>fuse_entry:" << endl;
    int bits = 1;
    InoGenerator inogen(*ioctx);
    cout << "after inogen" << endl;
    if (inogen.open() < 0) {
      inogen.init_pool(bits);
      inogen.open();
    }

    cout << "before create Lightfs " << endl;
    Lightfs fs(ioctx, &inogen);

    cout << "before create_root" << endl;
    if (!fs.create_root())
      return;
   
    cout << "before LightfsFuse" << endl;
    LightfsFuse lfuse(&fs);
    lfuse.init(argc, argv); 
  }

}

int main(int argc, char *argv[])
{
  int r;
  LightfsCtx lctx;
  //Rados rados;
  //IoCtx ioctx;

  //r = init_ceph_cluster(rados, ioctx);
  r = lctx.init_ctx();
  if (r < 0)
    goto err;

  //test_inogen(&(lctx._ioctx));
  //test_fs(&(lctx._ioctx));
  test_fuse(&lctx._ioctx, argc, argv);
err:
  lctx.destroy_ctx();
  return 0;

}

