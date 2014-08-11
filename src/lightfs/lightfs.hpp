#ifndef __LIGHTFS_HEADER__
#define __LIGHTFS_HEADER__

#include <string>

#include "common/Mutex.h"

#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"

namespace lightfs
{
  class InoGenerator
  {
  private:
    Mutex _mutex;
    librados::IoCtx _ioctx;
    int _bits;
    static void get_oid(int index, string &oid);
  public:
    InoGenerator(const librados::IoCtx &ioctx);
    ~InoGenerator();
    int init_pool(int bits);
    int open();
    void close();
    int generate(inodeno_t &ino);
  };
};

#endif

