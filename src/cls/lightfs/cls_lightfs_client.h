#ifndef __CLS_LIGHTFS_CLIENT_HEADER__
#define __CLS_LIGHTFS_CLIENT_HEADER__

#include <string>

#include <sys/types.h>

#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"

namespace lightfs {
  namespace cls_client {

    void create_seq(librados::ObjectWriteOperation *rados_op,
        uint64_t init_seq);
    int create_seq(librados::IoCtx *ioctx, const std::string &oid,
        uint64_t init_seq);
    void read_seq_start(librados::ObjectReadOperation *rados_op);
    int read_seq_end(bufferlist *outbl, uint64_t &seq);
    int read_seq(librados::IoCtx *ioctx, const std::string &oid, 
        uint64_t &seq);
    void write_seq(librados::ObjectWriteOperation *rados_op,
        uint64_t now);
    int write_seq(librados::IoCtx *ioctx, const std::string &oid, 
        uint64_t now);

    int create_inode(librados::IoCtx *ioctx, const std::string &oid, 
        bool excl, const inode_t &inode);
    int remove_inode(librados::IoCtx *ioctx, const std::string &oid);

    int get_inode(librados::IoCtx *ioctx, const std::string &oid,
        inode_t &inode, int used_attr = ATTR_ALL);
    void update_inode(librados::ObjectWriteOperation *rados_op,
        int used_attr, const inode_t &inode);
    int update_inode(librados::IoCtx *ioctx, const std::string &oid,
        int used_attr, const inode_t &inode);

    int link_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &name, inodeno_t ino);
    int unlink_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &name, inodeno_t ino);
    int rename_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &oldname, const std::string &newname, inodeno_t ino);
    int check_link_inode(librados::IoCtx *ioctx, const std::string &oid,
        inodeno_t ino);

    int find_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &name, inodeno_t &ino);
    int list_inode(librados::IoCtx *ioctx, const std::string &oid,
        const std::string &start_after, uint64_t max_return,
        std::map<std::string, inodeno_t> *result);
  }
}

#endif
