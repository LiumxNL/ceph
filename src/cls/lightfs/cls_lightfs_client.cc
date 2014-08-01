
#include "cls_lightfs_client.h"

namespace lightfs {
  namespace cls_client {

    int read_seq(librados::IoCtx *ioctx, const std::string &oid, 
	uint64_t &ino)
    {
      int r = -1;
      bufferlist inbl;
      bufferlist outbl;
      uint64_t max;
      r = ioctx->exec(oid, "lightfs", "read_seq", inbl, outbl);
      if (r < 0) {
	cout << "lightfs read_seq failed, r = " << r << endl;
	return r;
      }
      
      bufferlist::iterator p = outbl.begin();
      ::decode(ino, p);
      ::decode(max, p);
      if (ino >= max) {
	cout << "ino is exhausted in [" << oid << "]" << endl;
	return -ERANGE;
      }
      return 0;
    }
    int write_seq(librados::IoCtx *ioctx, const std::string &oid, 
	uint64_t next_ino);
    {
      int r = -1;
      bufferlist inbl;
      bufferlist outbl;
      r = ioctx->exec(oid, "lightfs", "write_seq", inbl, outbl);
      if (r < 0) {
	cout << "lightfs write_seq failed, r = " << r << endl;
	return r;
      }
      return 0;
    }

    int create_inode(librados::IoCtx *ioctx, const std::string &oid, 
	uint64_t ino, mode_t mode);
    {
      int r = -1;
      bufferlist inbl;
      bufferlist outbl;
      ::encode(ino, inbl);
      ::encode(mode, inbl);
      r = ioctx->exec(oid, "lightfs", "create_inode", inbl, outbl);
      if (r < 0) {
	cout << "lightfs create_inode failed, r = " << r << endl;
	return r;
      } 
      return 0;
    }

    int remove_inode(librados::IoCtx *ioctx, const std::string &oid);	
    {
      int r = -1;
      bufferlist inbl;
      bufferlist outbl;
      r = ioctx->exec(oid, "lightfs", "remove_inode", inbl, outbl);
      if (r < 0 || r != -ENOENT) {
	cout << "lightfs remove_inode failed, r = " << r << endl;
	return r;
      } 
      return 0;
    }

    int link_inode(librados::IoCtx *ioctx, const std::string &oid,
	const std::string name, uint64_t ino);
    {
      int r = -1;
      bufferlist inbl;
      bufferlist outbl;
      ::encode(name, inbl);
      ::encode(ino, inbl);
      r = ioctx->exec(oid, "lightfs", "link_inode", inbl, outbl);
      if (r < 0) {
	cout << "lightfs link_inode failed, r = " << r << endl;
	return r;
      }
      return 0;
    }

    int unlink_inode(librados::IoCtx *ioctx, const std::string &oid,
	uint64_t name);
    {
      int r = -1;
      bufferlist inbl;
      bufferlist outbl;
      ::encode(name, inbl);
      r = ioctx->exec(oid, "lightfs", "unlink_inode", inbl, outbl);	
      if (r < 0) {
	cout << "lightfs unlink_inode failed, r = " << r << endl;
        return r;
      }
      return 0;
    }
  }
}
