

#include "include/rados/librados.hpp"
#include "include/types.h"
//#include "include/lightfs_types.hpp"

#include <string>

#include <sys/types.h>
#include <errno.h>

namespace lightfs {
  namespace cls_client {

    int read_seq(librados::IoCtx *ioctx, const std::string &oid, 
	uint64_t &ino);
    int write_seq(librados::IoCtx *ioctx, const std::string &oid, uint64_t next_ino);

    int create_inode(librados::IoCtx *ioctx, const std::string &oid, 
	uint64_t ino, mode_t mode);
    int remove_inode(librados::IoCtx *ioctx, const std::string &oid);	

    int link_inode(librados::IoCtx *ioctx, const std::string &oid,
	const std::string &name, uint64_t ino);
    int unlink_inode(librados::IoCtx *ioctx, const std::string &oid,
	const std::string &name);

    int rename(librados::IoCtx *ioctx, const std::string &oid,
	const std::string &oldname, const std::string &newname, uint64_t ino);
  }
}
