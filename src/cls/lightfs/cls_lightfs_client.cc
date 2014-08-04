#include "cls_lightfs_client.h"

namespace lightfs {
  namespace cls_client {

    int read_seq(librados::IoCtx *ioctx, const std::string &oid, 
        uint64_t &ino)
    {
      int r;

      bufferlist inbl;
      bufferlist outbl;

      r = ioctx->exec(oid, "lightfs", "read_seq", inbl, outbl);
      if (r < 0)
        return r;

      try {
        bufferlist::iterator p = outbl.begin();
        ::decode(ino, p);
      } catch (const buffer::error &err) {
        assert(0);
        return -EIO;
      }

      return 0;
    }

    int write_seq(librados::IoCtx *ioctx, const std::string &oid, 
        uint64_t now)
    {
      bufferlist inbl;
      bufferlist outbl;

      ::encode(now, inbl);
      return ioctx->exec(oid, "lightfs", "write_seq", inbl, outbl);
    }

    int create_inode(librados::IoCtx *ioctx, const std::string &oid, 
        bool excl, const inode_t &inode)
    {
      bufferlist inbl;
      bufferlist outbl;

      ::encode(excl, inbl);
      inode.encode(inbl);
      return ioctx->exec(oid, "lightfs", "create_inode", inbl, outbl);
    }

    int remove_inode(librados::IoCtx *ioctx, const std::string &oid)
    {
      bufferlist inbl;
      bufferlist outbl;

      return ioctx->exec(oid, "lightfs", "remove_inode", inbl, outbl);
    }

    int link_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &name, inodeno_t ino)
    {
      bufferlist inbl;
      bufferlist outbl;
      
      ::encode(name, inbl);
      ::encode(ino, inbl);
      return ioctx->exec(oid, "lightfs", "link_inode", inbl, outbl);
    }

    int unlink_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &name, inodeno_t ino)
    {
      bufferlist inbl;
      bufferlist outbl;
      
      ::encode(name, inbl);
      ::encode(ino, inbl);
      return ioctx->exec(oid, "lightfs", "unlink_inode", inbl, outbl);
    }

    int rename_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &oldname, const std::string &newname, inodeno_t ino)
    {
      bufferlist inbl;
      bufferlist outbl;
      
      ::encode(oldname, inbl);
      ::encode(newname, inbl);
      ::encode(ino, inbl);
      return ioctx->exec(oid, "lightfs", "rename_inode", inbl, outbl);
    }
  }
}
