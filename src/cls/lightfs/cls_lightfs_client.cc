#include "cls_lightfs_client.h"

namespace lightfs {
  namespace cls_client {

    void create_seq(librados::ObjectWriteOperation *rados_op,
        uint64_t init_seq)
    {
      bufferlist in;
      ::encode(init_seq, in);
      rados_op->exec("lightfs", "create_seq", in);
    }

    int create_seq(librados::IoCtx *ioctx, const std::string &oid,
        uint64_t init_seq)
    {
      librados::ObjectWriteOperation op;
      create_seq(&op, init_seq);
      return ioctx->operate(oid, &op);
    }

    void read_seq_start(librados::ObjectReadOperation *rados_op)
    {
      bufferlist empty;
      rados_op->exec("lightfs", "read_seq", empty);
    }

    int read_seq_end(bufferlist *outbl, uint64_t &seq)
    {
      try {
        bufferlist::iterator p = outbl->begin();
        ::decode(seq, p);
      } catch (const buffer::error &err) {
        return -EBADMSG;
      }
      return 0;
    }

    int read_seq(librados::IoCtx *ioctx, const std::string &oid,
        uint64_t &seq)
    {
      int r;
      bufferlist out;
      librados::ObjectReadOperation op;
      read_seq_start(&op);
      r = ioctx->operate(oid, &op, &out);
      if (r < 0)
        return r;
      return read_seq_end(&out, seq);
    }

    void write_seq(librados::ObjectWriteOperation *rados_op,
        uint64_t now)
    {
      bufferlist in;
      ::encode(now, in);
      rados_op->exec("lightfs", "write_seq", in);
    }

    int write_seq(librados::IoCtx *ioctx, const std::string &oid,
        uint64_t now)
    {
      librados::ObjectWriteOperation op;
      write_seq(&op, now);
      return ioctx->operate(oid, &op);
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

    int find_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &name, inodeno_t &ino)
    {
      int r;

      bufferlist inbl;
      bufferlist outbl;

      ::encode(name, inbl);
      r = ioctx->exec(oid, "lightfs", "find_inode", inbl, outbl);
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

    int list_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &start_after, uint64_t max_return,
        std::map<std::string, inodeno_t> *result)
    {
      int r;

      bufferlist inbl;
      bufferlist outbl;

      ::encode(start_after, inbl);
      ::encode(max_return, inbl);
      r = ioctx->exec(oid, "lightfs", "list_inode", inbl, outbl);
      if (r < 0)
        return r;

      try {
        bufferlist::iterator p = outbl.begin();
        std::map<std::string, inodeno_t> res;
        ::decode(res, p);
        std::map<std::string, inodeno_t>::iterator ptr = res.begin();
    	for (; ptr != res.end(); ++ptr) {
	  //N.name -> name  "N.name".substr(2) = "name"
	  (*result)[ptr->first.substr(2)] = ptr->second;
  	}
      } catch (const buffer::error &err) {
        assert(0);
        return -EIO;
      }

      return 0;
    }
  }
}
