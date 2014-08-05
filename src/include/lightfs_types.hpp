#ifndef __LIGHTFS_TYPES_HPP__
#define __LIGHTFS_TYPES_HPP__

#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>

#include "include/types.h"
#include "include/encoding.h"
#include "include/int_types.h"
#include "include/utime.h"

namespace lightfs
{
  enum
  {
    ATTR_NONE = 0,
    ATTR_MODE = 1,
    ATTR_UID = 2,
    ATTR_GID = 4,
    ATTR_MTIME = 8,
    ATTR_ATIME = 16,
    ATTR_CTIME = 32,
    ATTR_SIZE = 64,
    ATTR_ALL = ATTR_MODE | ATTR_UID | ATTR_GID |
               ATTR_MTIME | ATTR_ATIME | ATTR_CTIME |
               ATTR_SIZE
  };

  typedef uint64_t inodeno_t;
  
  struct inode_t {
    utime_t ctime;
    utime_t atime;
    utime_t mtime;

    uint64_t size;
    uint64_t max_size;
    uint32_t truncate_size;

    mode_t mode;

    int uid;
    int gid;

    inode_t() :
      ctime(0),
      atime(0),
      mtime(0),
      size(0),
      max_size(0),
      truncate_size(0),
      mode(0),
      uid(0),
      gid(0)
    {}

    void encode(int used_attr, bufferlist &bl) const
    {
      ENCODE_START(1, 1, bl);

      if (used_attr & ATTR_CTIME)
        ::encode(ctime, bl);
      if (used_attr & ATTR_ATIME)
        ::encode(atime, bl);
      if (used_attr & ATTR_MTIME)
        ::encode(mtime, bl);

      if (used_attr & ATTR_SIZE) {
        ::encode(size, bl);
        ::encode(max_size, bl);
        ::encode(truncate_size, bl);
      }

      if (used_attr & ATTR_MODE)
        ::encode(mode, bl);

      if (used_attr & ATTR_UID)
        ::encode(uid, bl);
      if (used_attr & ATTR_GID)
        ::encode(gid, bl);

      ENCODE_FINISH(bl);
    }

    void encode(bufferlist &bl) const
    {
      encode(ATTR_ALL, bl);
    }

    void decode(int used_attr, bufferlist::iterator &p)
    {
      DECODE_START(1, p);

      if (used_attr & ATTR_CTIME)
        ::decode(ctime, p);
      if (used_attr & ATTR_ATIME)
        ::decode(atime, p);
      if (used_attr & ATTR_MTIME)
        ::decode(mtime, p);

      if (used_attr & ATTR_SIZE) {
        ::decode(size, p);
        ::decode(max_size, p);
        ::decode(truncate_size, p);
      }

      if (used_attr & ATTR_MODE)
        ::decode(mode, p);

      if (used_attr & ATTR_UID)
        ::decode(uid, p);
      if (used_attr & ATTR_GID)
        ::decode(gid, p);

      DECODE_FINISH(p);
    }

    void decode(bufferlist::iterator &p)
    {
      decode(ATTR_ALL, p);
    }
  };
};

#endif
