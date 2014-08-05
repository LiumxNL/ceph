#ifndef __CLS_LIGHTFS_CLIENT_HEADER__
#define __CLS_LIGHTFS_CLIENT_HEADER__

#include <string>

#include <sys/types.h>

#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"

namespace lightfs {
  namespace cls_client {

    int create_seq(librados::IoCtx *ioctx, const std::string &oid,
        uint64_t max);
    int read_seq(librados::IoCtx *ioctx, const std::string &oid, 
        uint64_t &seq);
    int write_seq(librados::IoCtx *ioctx, const std::string &oid, 
        uint64_t now);

    int create_inode(librados::IoCtx *ioctx, const std::string &oid, 
        bool excl, const inode_t &inode);
    int remove_inode(librados::IoCtx *ioctx, const std::string &oid);

    int get_inode(librados::IoCtx *ioctx, const std::string &oid,
        inode_t &inode, int used_attr = ATTR_ALL);
    int update_inode(librados::IoCtx *ioctx, const std::string &oid,
        int used_attr, const inode_t &inode);

    int link_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &name, inodeno_t ino);
    int unlink_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &name, inodeno_t ino);
    int rename_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &oldname, const std::string &newname, inodeno_t ino);
  }
}

#endif
