#ifndef __LIGHTFS_TYPES_HPP__
#define __LIGHTFS_TYPES_HPP__

#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>

#include "include/types.h"
#include "include/encoding.h"
#include "include/int_types.h"
#include "include/utime.h"

#define BITS 64
#define OFF_SET 8
#define INO_CHAR_LEN (BITS >> 2) // in hexdecimal string eg. 1010->A , one char equals to 4 bits
#define GEN_UNIT 8  //unit64_t , 64bits -> 8bytes
#define GEN_CONTENT_LEN 16 // [uint64_t, uint64_t] , 2 long unsigned ,  8 * 2 = 16

#define INO_PREFIX "inode"
#define DATA_PREFIX "data"
#define GEN_PREFIX "generator"
#define LOG_PREFIX "log"
#define DOT "."

#define INO_PREFIX_LEN 5
#define DATA_PREFIX_LEN 4
#define GEN_PREFIX_LEN 9 //strlen("generator") 
#define LOG_PREFIX_LEN 3
#define DOT_LEN 1
#define TERMINATE 1 // strlen('\0') = 1

//eg. "inode.xxxx"
#define INO_LEN ((INO_PREFIX_LEN) + (DOT_LEN) + (INO_CHAR_LEN) + (TERMINATE))
//eg. "data.xxxx.yyyy"
#define DATA_LEN ((DATA_PREFIX_LEN) + (DOT_LEN) + (INO_CHAR_LEN) + (DOT_LEN) + (OFF_SET)+ (TERMINATE))
//eg. "generator.xxxx"
#define GEN_LEN ((GEN_PREFIX_LEN) + (DOT_LEN) + (INO_CHAR_LEN)+ (TERMINATE))
//eg. "log.xxxx"
#define LOG_LEN ((LOG_PREFIX_LEN) + (DOT_LEN) + (INO_CHAR_LEN)+ (TERMINATE))

#define ROOT_INO 0

enum lightfs_obj_type {
  LIGHTFS_OBJ_INO,
  LIGHTFS_OBJ_DATA,
  LIGHTFS_OBJ_GEN,
  LIGHTFS_OBJ_LOG,
};

struct oid_t;
struct lightfs_inode_t;


struct oid_t {
private:
  enum lightfs_obj_type type;
  uint32_t offset;
  union {
    char ino_id[INO_LEN];
    char data_id[DATA_LEN];
    char gen_id[GEN_LEN];
    char log_id[LOG_LEN];
  };
public:
  char *id; //point to current type id in {ino_id, data_id, gen_id, log_id}
  oid_t(enum lightfs_obj_type itype, uint64_t num, uint32_t ioffset = 0) : type(itype), offset(ioffset) {
    int t = 0;
    char format1[32];
    char format2[32];
    char *format = NULL;
    switch (type) {
      case LIGHTFS_OBJ_INO: 
	{
	  //snprintf(format, sizeof(format), "inode.%%0%dlu", INO_CHAR_LEN);
	  snprintf(format1, sizeof(format1), "%s.%%llx", INO_PREFIX);
	  snprintf(format2, sizeof(format2), "%s.%%0%dllx", INO_PREFIX, INO_CHAR_LEN);
          format = num > 0 ? format2 : format1;	  
	  t = snprintf(ino_id, sizeof(ino_id), format, num);
 	  ino_id[t]='\0';
  	  id = ino_id;
	  //printf("t=%d, id=%s\n",t,id);
 	  break; 
 	}
      case LIGHTFS_OBJ_DATA:
	{
	  //snprintf(format, sizeof(format), "data.%%0%dlu.%%08lu", INO_CHAR_LEN);
	  snprintf(format2, sizeof(format2), "%s.%%0%dllx.%%0%dllx", DATA_PREFIX, INO_CHAR_LEN, OFF_SET);
	  t = snprintf(data_id, sizeof(data_id), format2, num, offset);
 	  data_id[t]='\0';
  	  id = data_id;
	  //printf("t=%d, id=%s\n",t,id);
	  break;
	}
      case LIGHTFS_OBJ_GEN:
	{
	  //snprintf(format, sizeof(format), "generator.%%0%dlu", INO_CHAR_LEN);
	  snprintf(format1, sizeof(format1), "%s.%%llx", GEN_PREFIX);
	  t = snprintf(gen_id, sizeof(gen_id), format1, num);
 	  gen_id[t]='\0';
  	  id = gen_id;
	  //printf("t=%d, id=%s\n",t,id);
	  break;
	}
      case LIGHTFS_OBJ_LOG:
	{
	  //snprintf(format, sizeof(format), "log.%%0%dlu", INO_CHAR_LEN);
	  snprintf(format1, sizeof(format1), "%s.%%llx", LOG_PREFIX);
	  t = snprintf(log_id, sizeof(log_id), format1, num);
 	  log_id[t]='\0';
  	  id = log_id;
	  //printf("t=%d, id=%s\n",t,id);
	  break;
	}
      default:
	{
	  id = NULL;
	  //printf("invalid oid type\n");
	}
    }
  }
  ~oid_t() {}
  enum lightfs_obj_type get_obj_type() { return type; }
};

struct lightfs_inode_t {

  _inodeno_t ino;
  
  utime_t ctime; // change time (status time): change .size, attr, mode, and so on, usually write file or new file in dir
  utime_t atime; // access time: open file, read file or execute file , usually read file or readdir
  utime_t mtime; // modify time: file content is modified (changed), usually write file or new file in dir

  uint64_t size;
  uint64_t max_size;
  uint32_t truncate_size;
  
  //uint32_t mode;
  mode_t mode;

  int uid;
  int gid;
  
  lightfs_inode_t(_inodeno_t inum, uint32_t imode = 0) : ino(inum), ctime(), atime(), mtime(),
    size(0), max_size(0), truncate_size(0), uid(-1), gid(-1) 
  {
    switch (imode) {
      case S_IFLNK: 
      case S_IFDIR:
      case S_IFREG:
    	mode = 0500 | imode; //file owner can read and execute
 	break;
      default:
	mode = 0;
    }
  }
  ~lightfs_inode_t() {}
  
  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir() const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file() const { return (mode & S_IFMT) == S_IFREG; }

  void encode_inode(bufferlist &bl)
  {
    ::encode(ino, bl);
    ::encode(ctime, bl);
    ::encode(atime, bl);
    ::encode(mtime, bl);
    ::encode(size, bl);
    ::encode(max_size, bl);
    ::encode(truncate_size, bl);
    ::encode(mode, bl);
  }

  void decode_inode(bufferlist::iterator &p)
  {
    ::decode(ino, p);
    ::decode(ctime, p);
    ::decode(atime, p);
    ::decode(mtime, p);
    ::decode(size, p);
    ::decode(max_size, p);
    ::decode(truncate_size, p);
    ::decode(mode, p);
  }
 
};

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
