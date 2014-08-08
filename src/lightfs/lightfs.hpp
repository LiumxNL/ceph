#ifndef __LIGHTFS_HEADER__
#define __LIGHTFS_HEADER__

#include <string>

#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"

namespace lightfs
{
  class InoGenerator
  {
  private:
    librados::IoCtx _ioctx;
    int _bits;
    static void get_oid(int index, string &oid);
  public:
    InoGenerator(const librados::IoCtx& ioctx);
    ~InoGenerator();
    static int init_pool(librados::IoCtx& ioctx, int bits);
    int generate(inodeno_t &ino);
  };

  class DirectoryManager
  {
  private:
    librados::IoCtx _ioctx;
  public:
    DirectoryManager(const librados::IoCtx &ioctx);
    static int init_pool(librados::IoCtx &ioctx);
    int create(inodeno_t pino, bool excl, inode_t &inode);
    int remove(inodeno_t pino, const std::string &name, inodeno_t ino);
    int lookup(inodeno_t pino, const std::string &name, inodeno_t &ino);
    int lookup(inodeno_t pino, inodeno_t ino);
  };
};

#endif

