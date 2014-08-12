#include <iostream>


#include "include/rados/librados.hpp"
#include "cls/lightfs/cls_lightfs_client.h"

using namespace std;
using namespace librados;
using namespace lightfs;
using namespace lightfs::cls_client;


#define TEST_EQUAL(func, rval) \
{ \
  int r = (func);  \
  if (r == (rval)) \
    printf("[PASSED]: %s == %d\n", #func, (rval)); \
  else  \
    printf("[FAILED]: %s != %d, r = %d\n", #func, (rval), r); \
}

#define PART_LINE(str) printf("====================   %s   ====================\n",(str));

namespace lightfs {

  int init_cluster(Rados &rados, IoCtx &ioctx)
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


  int init_inogen(IoCtx &ioctx, string &oid, inodeno_t first)
  {
    cout << "init_inogen" << endl;
    int r;
    r = ioctx.stat(oid, NULL, NULL);
    cout << "ioctx.stat , r = " << r << endl;
    if (r == 0)
      return r;  
    ObjectWriteOperation wr_op;
    bufferlist inbl;
    ::encode(first, inbl);
    wr_op.omap_set_header(inbl);
    r = ioctx.operate(oid, &wr_op);
    if (r < 0) {
      cerr << "write inogen init data failed" << endl;
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

  int test_cls_client(IoCtx &ioctx)
  {
    int r;
    string seq_obj("inogen.0");
    string seq2_obj("inogen.2");
    string parent_obj("inode.10");
    string child_obj("inode.100");
    string child2_obj("inode.101");
    inodeno_t pino = 10;
    inodeno_t seq = -1;
    inodeno_t ino1 = 101;
    inodeno_t ino2 = 102;
    inodeno_t ino3 = 103;

    inodeno_t start = 0;
    inodeno_t end = 0xffffffff;
    
    inodeno_t num = -1;
    r = init_inogen(ioctx, seq_obj, start); 
    if (r < 0)
       return r;
    
    // test seq
    PART_LINE("read_seq");
    TEST_EQUAL(read_seq(&ioctx, seq2_obj, seq), 0);
    TEST_EQUAL(read_seq(&ioctx, seq_obj, seq), 0);
    PART_LINE("write_seq");
    TEST_EQUAL(write_seq(&ioctx, seq_obj, seq), 0);
    TEST_EQUAL(write_seq(&ioctx, seq_obj, 0x100), 0);
    //TEST_EQUAL(write_seq(&ioctx, seq2_obj, 0x200), 0); //assert(false)

    // test inode
    PART_LINE("create_inode");
    inode_t parent;
    inode_t child;
    
    TEST_EQUAL(create_inode(&ioctx, child_obj, true, child), 0);
    TEST_EQUAL(create_inode(&ioctx, child_obj, false, child), 0);
    TEST_EQUAL(create_inode(&ioctx, parent_obj, false, parent), 0);
    TEST_EQUAL(create_inode(&ioctx, parent_obj, true, parent), 0);


    PART_LINE("update_inode");
    inode_t update_inode_v;
    double atime_v = 10.001;
    double ctime_v = 10.001;
    double mtime_v = 10.001;
    update_inode_v.atime.set_from_double(atime_v);
    update_inode_v.ctime.set_from_double(ctime_v);
    update_inode_v.mtime.set_from_double(mtime_v);
    update_inode_v.size = 100;
    update_inode_v.max_size = 1000;
    update_inode_v.truncate_size = 1;
    update_inode_v.mode = S_IFDIR;
    update_inode_v.uid = 1;
    update_inode_v.gid = 1;

    TEST_EQUAL(update_inode(&ioctx, parent_obj, ATTR_ALL, update_inode_v), 0);
    //TEST_EQUAL(update_inode(&ioctx, child2_obj, ATTR_ALL, update_inode_v), 0); //assert(0)

    PART_LINE("get_inode");
    inode_t got_inode;
    TEST_EQUAL(get_inode(&ioctx, parent_obj, got_inode), 0);
    cout << "\t  ";
    print_inode(got_inode);
    TEST_EQUAL(get_inode(&ioctx, child2_obj, got_inode), 0);

    PART_LINE("link_inode");
    TEST_EQUAL(link_inode(&ioctx, parent_obj, "child1", ino1), 0);
    TEST_EQUAL(link_inode(&ioctx, parent_obj, "child2", ino2), 0);
    TEST_EQUAL(link_inode(&ioctx, parent_obj, "child3", ino3), 0);
    TEST_EQUAL(link_inode(&ioctx, child2_obj, "child3", ino3), 0);

    PART_LINE("unlink_inode");
    TEST_EQUAL(unlink_inode(&ioctx, parent_obj, "child1", ino3), 0);
    TEST_EQUAL(unlink_inode(&ioctx, parent_obj, "child1", ino1), 0);
    TEST_EQUAL(unlink_inode(&ioctx, child2_obj, "child3", ino3), 0);

    PART_LINE("rename_inode");
    TEST_EQUAL(rename_inode(&ioctx, parent_obj, "child1", "sub1", ino1), 0);
    TEST_EQUAL(rename_inode(&ioctx, parent_obj, "child2", "sub2", ino1), 0);
    TEST_EQUAL(rename_inode(&ioctx, parent_obj, "child2", "sub2", ino2), 0);
    TEST_EQUAL(rename_inode(&ioctx, child2_obj, "child3", "sub3", ino3), 0);

    PART_LINE("remove_inode");
    TEST_EQUAL(remove_inode(&ioctx, child_obj), 0);
    TEST_EQUAL(remove_inode(&ioctx, parent_obj), 0);
    TEST_EQUAL(remove_inode(&ioctx, child2_obj), 0);

    PART_LINE("find_inode");
    inodeno_t myino = -1;
    TEST_EQUAL(find_inode(&ioctx, parent_obj, "child3", myino), 0);
    cout << "\t  child3->ino = " << myino << endl;
    TEST_EQUAL(find_inode(&ioctx, child_obj, "child", myino), 0);
    TEST_EQUAL(find_inode(&ioctx, child2_obj, "child", myino), 0);
    
    PART_LINE("list_inode");
    std::map<std::string, inodeno_t> result;
    TEST_EQUAL(list_inode(&ioctx, parent_obj, "", 255, &result), 0);
    std::map<std::string, inodeno_t>::iterator ptr = result.begin();
    for (; ptr != result.end(); ++ptr) {
      cout << "\t  <" << ptr->first << "," << ptr->second << ">" << endl;
    }  
    TEST_EQUAL(list_inode(&ioctx, child2_obj, "", 255, &result), 0);

  }
}

int main(int argc, char *argv[])
{
  int r;
  Rados rados;
  IoCtx ioctx;
 
  //cout << "test cls client" << endl;  
 
  r = init_cluster(rados, ioctx);
  if (r < 0) 
    goto err;
  
  //cout << "OK, open" << endl;
  test_cls_client(ioctx);

err:
  ioctx.close();
  rados.shutdown();
  return 0;
}

