
#include <iostream>
#include "cls_lightfs_client.h"

#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

using namespace librados;
using namespace std;
using namespace lightfs::cls_client;

#define USER "admin"
#define POOL "lfs_cls"
#define CONF "/etc/ceph/ceph.conf"

enum test_op {
  OP_NULL,
  READ_SEQ,
  WRITE_SEQ,
  CREATE_INODE,
  REMOVE_INODE,
  LINK_INODE,
  UNLINK_INODE,
  RENAME
};

const char *seq_oid = "generator.0";
const char *inode_oid = "inode.100";

long get_ino()
{
  long ret = time(NULL);
  return ret < 0 ? 0 : ret;  
}

void usage()
{
  cout  << "./test_cls	read_seq" << endl;
  cout	<< "		write_seq	<next_seq>" << endl;
  cout	<< "		create_inode 	<oid>	<S_IFDIR|S_IFREG|S_IFLNK>" << endl;
  cout	<< "		remove_inode	<oid>" << endl;
  cout	<< "		link_inode	<oid>	<name>	<ino>" << endl;
  cout	<< "		unlink_inode	<oid>	<name>" << endl;
  cout	<< "		rename		<oid>	<oldname>  <newname>	<ino>" << endl;
}

mode_t get_mode(const char *mode_str)
{
  if (strcmp(mode_str, "S_IFDIR") == 0) 
    return S_IFDIR;
  
  if (strcmp(mode_str, "S_IFREG") == 0) 
    return S_IFREG;

  if (strcmp(mode_str, "S_IFLNK") == 0) 
    return S_IFLNK;
  
  return -1;
}

void parse_arg(enum test_op *op, int argc, const char *argv[])
{
  if (argc < 2) {
    usage();
    return;
  }

  if (strcmp(argv[1], "read_seq") == 0) {
    *op = READ_SEQ;
  } else if (strcmp(argv[1], "write_seq") == 0) {
    *op = WRITE_SEQ;
  } else if (strcmp(argv[1], "create_inode") == 0) {
    *op = CREATE_INODE;
  } else if (strcmp(argv[1], "remove_inode") == 0) {
    *op = REMOVE_INODE;
  } else if (strcmp(argv[1], "link_inode") == 0) {
    *op = LINK_INODE;
  } else if (strcmp(argv[1], "unlink_inode") == 0) {
    *op = UNLINK_INODE;
  } else if (strcmp(argv[1], "rename") == 0) {
    *op = RENAME;
  }
}

int init_client(librados::Rados &rados, librados::IoCtx &ioctx, const char *userid, const char *pool)
{
    int r = -1;
    /*
      1. create cluster
    */
    r = rados.init(userid);
    if ( r < 0) {
      cout << "rados create error , r = " << r << endl;
      return r;
    }

    /*
      2. read conf file to cluster
    */
    r = rados.conf_read_file(CONF);
    if ( r < 0 ) {
      cout << "conf read error , r = " << r << endl;
      return r;
    }

    /*
      3. connect to cluster
    */
    r = rados.connect();
    if ( r < 0 ) {
      cout << "rados connect error , r = " << r << endl;
      return r;
    }

    /*
      4. lookup or create pool in cluster
    */
    r = rados.pool_create(pool);
    if ( r < 0 ) {
      cout << "pool create  error , r = " << r << endl;
      return r;
    }
    /*
      5. in cluster: creat ioctx , then do read ,write ......
    */
    r = rados.ioctx_create(pool, ioctx);
    if ( r < 0 ) {
      cout << "ioctx create  error , r = " << r << endl;
      return r;
    }

    return 0;
}

void destory_client(librados::Rados &rados, librados::IoCtx &ioctx)
{
  ioctx.close();
  rados.shutdown();
}

int main(int argc, const char *argv[])
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  
  if (argc < 2) {
    usage();
    return -1;
  }
  enum test_op op = OP_NULL;
  parse_arg(&op, argc, argv);
  cout << "will init client" << endl;
  int r = -1;
  r = init_client(rados, ioctx, USER, POOL); 
  if (r < 0)
    return r;
  
  cout << "will create object seq oid" << endl;
  string seq_obj(seq_oid);
  //ioctx.create(seq_obj, true);

  librados::ObjectWriteOperation wr_op;
  bufferlist mybl;
  ::encode(0, mybl);
  ::encode(0x1fffffff, mybl);
  wr_op.omap_set_header(mybl);
  ioctx.operate(seq_obj, &wr_op);

  cout << "after create object seq oid" << endl;
  uint64_t ino = -1;
  uint64_t next_ino = -1;
  mode_t mode;
  string name; 
  string oldname;
  string newname;  
  string inode_oid;

  cout << "before switch" << endl; 
  cout << "op = " << op << endl;
  switch (op) {
    case READ_SEQ:
	{
	  cout << "read_seq: ";
 	  read_seq(&ioctx, seq_obj, ino);
	  cout << "ino = " << hex << ino << dec << endl;
	  break;
	}
    case WRITE_SEQ:
	{
	  if (argc < 4 ) {
	    usage();
	    return 0;
	  }
	  cout << "write_seq: " << endl;
	  inode_oid.append(argv[2]);
	  next_ino = strtoul(argv[3], NULL, 16);
  	  write_seq(&ioctx, seq_obj, next_ino);	  
	  break;
	}
    case CREATE_INODE:
	{
	  if (argc < 5) {
	    usage();
	    return 0;
	  }
	  cout << "create_inode: " << endl;
	  inode_oid.append(argv[2]);
	  ino = strtoul(argv[3], NULL, 16);
	  mode = get_mode(argv[4]);
	  if (mode == 0)
	    return -1;
	  create_inode(&ioctx, inode_oid, ino, mode);
	  break;
	}
    case REMOVE_INODE:
	{
	  if (argc < 3) {
	    usage();
	    return 0;
	  }
	  cout << "remove_inode: " << endl;
	  inode_oid.append(argv[2]);
	  remove_inode(&ioctx, inode_oid);
	  break;
	}
    case LINK_INODE:
	{
	  if (argc < 5) {
	    usage();
	    return 0;
	  }
	  cout << "link_inode: " << endl;
	  inode_oid.append(argv[2]);
	  name.append(argv[3]);
	  ino = strtoul(argv[4], NULL, 16);
	  link_inode(&ioctx, inode_oid, name, ino);
	  break;
	}
    case UNLINK_INODE:
	{
	  if (argc < 4) {
	    usage();
	    return 0;
 	  }
	  cout << "unlink_inode: " << endl;
	  inode_oid.append(argv[2]);
	  name.append(argv[3]);
	  unlink_inode(&ioctx, inode_oid, name);
	  break;
	}
    case RENAME:
	{
	  if (argc < 6) {
	    usage();
	    return 0;
	  }
	  cout << "rename: " << endl;
	  inode_oid.append(argv[2]);
	  oldname.append(argv[3]);
	  newname.append(argv[4]);
	  ino = strtoul(argv[5], NULL, 16);
	  rename(&ioctx, inode_oid, oldname, newname, ino);
	  break;
	}
  }
}
