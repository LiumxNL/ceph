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
    void get_oid(int index, string &oid);
  public:
    InoGenerator(const librados::IoCtx& ioctx);
    ~InoGenerator();
    int init_pool(int bits);
    int generate(inodeno_t &ino);
  };
};

#endif

