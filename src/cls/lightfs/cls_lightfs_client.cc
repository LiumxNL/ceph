#include "cls_lightfs_client.h"

namespace lightfs {
  namespace cls_client {

    int create_seq(librados::IoCtx *ioctx, const std::string &oid,
        uint64_t init_seq)
    {
      bufferlist inbl;
      bufferlist outbl;

      ::encode(init_seq, inbl);
      return ioctx->exec(oid, "lightfs", "create_seq", inbl, outbl);
    }

    int read_seq(librados::IoCtx *ioctx, const std::string &oid, 
        uint64_t &seq)
    {
      int r;

      bufferlist inbl;
      bufferlist outbl;

      r = ioctx->exec(oid, "lightfs", "read_seq", inbl, outbl);
      if (r < 0)
        return r;

      try {
        bufferlist::iterator p = outbl.begin();
        ::decode(seq, p);
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

    int get_inode(librados::IoCtx *ioctx, const std::string &oid, 
        inode_t &inode, int used_attr)
    {
      int r;

      bufferlist inbl;
      bufferlist outbl;

      ::encode(used_attr, inbl);
      r = ioctx->exec(oid, "lightfs", "get_inode", inbl, outbl);
      if (r < 0)
        return r;

      try {
        bufferlist::iterator p = outbl.begin();
        inode.decode(used_attr, p);
      } catch (const buffer::error &err) {
        assert(0);
        return -EIO;
      }

      return 0;
    }

    int update_inode(librados::IoCtx *ioctx, const std::string &oid, 
        int used_attr, const inode_t &inode)
    {
      bufferlist inbl;
      bufferlist outbl;

      ::encode(used_attr, inbl);
      inode.encode(used_attr, inbl);
      return ioctx->exec(oid, "lightfs", "update_inode", inbl, outbl);
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

    int check_link_inode(librados::IoCtx *ioctx, const std::string &oid,
        inodeno_t ino)
    {
      bufferlist inbl;
      bufferlist outbl;

      ::encode(ino, inbl);
      return ioctx->exec(oid, "lightfs", "check_link_inode", inbl, outbl);
    }
  }
}
