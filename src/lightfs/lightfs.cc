#include <stdlib.h>
#include <string.h>
#include <string>

#include "lightfs.hpp"
#include "cls/lightfs/cls_lightfs_client.h"

using namespace std;
using namespace librados;

namespace lightfs
{
  InoGenerator::InoGenerator(const IoCtx &ioctx)
    :_bits(0)
  {
    _ioctx.dup(ioctx);
  }

  InoGenerator::~InoGenerator()
  {
    _ioctx.close();
  }

  void InoGenerator::get_oid(int index, string &oid)
  {
    if (index < 0) {
      oid = "inogen";
    } else {
      char buf[20];
      snprintf(buf, 20, "inogen.%04X", index);
      oid = buf;
    }
  }

  int InoGenerator::init_pool(int bits)
  {
    if (bits < 1 || bits > 4)
      return -EINVAL;

    int r;
    for (int i = 0; i < (1 << bits); ++i) {
      string oid;
      get_oid(i, oid);

      r = cls_client::create_seq(&_ioctx, oid);
      if (r < 0)
        return r;
    }

    string oid;
    get_oid(-1, oid);

    r = _ioctx.create(oid, false);
    if (r < 0)
      return r;

    bufferlist data;
    ::encode(bits, data);
    r = _ioctx.omap_set_header(oid, data);
    if (r < 0)
      return r;

    return 0;
  }

  int InoGenerator::generate(inodeno_t &ino)
  {
    int r;

    if (_bits == 0) {
      string oid;
      get_oid(-1, oid);

      bufferlist data;
      r = _ioctx.omap_get_header(oid, &data);
      if (r < 0)
        return r;

      try {
        bufferlist::iterator p = data.begin();
        ::decode(_bits, p);
      } catch (const buffer::error &err) {
        return -EIO;
      }
    }

    uint64_t max = (1 << (64 - _bits));
    while (true) {
      int index = rand() & ((1 << _bits) - 1);
      string oid;
      get_oid(index, oid);

      r = cls_client::read_seq(&_ioctx, oid, ino);
      if (r < 0)
        return r;

      if (ino == max)
        continue;

      r = cls_client::write_seq(&_ioctx, oid, ino);
      if (r < 0) {
        if (r == -ERANGE || r == -EAGAIN)
          continue;
        return r;
      }

      ino |= index << (64 - _bits);
      return 0;
    }
  }
};

