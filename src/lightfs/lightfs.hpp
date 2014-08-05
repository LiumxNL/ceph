

/*
  lightfs: a ceph light weight file system, non POSIX compat
*/

#include "include/rados/librados.hpp"
#include "include/rados/librados.h"
#include "include/types.h"
#include "include/int_types.h"
//#include "include/xlist.h"
#include "include/buffer.h"
#include "common/Clock.h"

#include "include/lightfs_types.hpp"


#include <iostream>
#include <sstream>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fuse/fuse_lowlevel.h>

#include <set>
#include <vector>
#include <deque>
#include <utility>

#ifndef LIGHTFS_HPP
#define LIGHTFS_HPP

#define FUSE_USE_VERSION 24

#define LIGHTFS_CONF "/etc/ceph/ceph.conf"
#define LIGHTFS_POOL "lfs"

enum operation {
  LIGHTFS_NOACTION,
  LIGHTFS_MKDIR,
  LIGHTFS_READDIR,
  LIGHTFS_LOOKUP,
  LIGHTFS_RMDIR,
  LIGHTFS_RM,
  LIGHTFS_RENAME,
};

namespace lightfs {
  
  class LightfsCtx;
  class Lightfs;
  
  class InoGen;
  
  class LightfsDentry;
  class LightfsDir;
  class LightfsFile;
  
  class Inode;
  class InodeCache;
  
  class LightfsLog;
  
  class FuseHandle;
  
  
  class LightfsCtx {
  private:
    const char *userid;
    const char *pool;
    const char *conf;
  public:
    librados::Rados rados; //related to a ceph cluster
    librados::IoCtx ioctx; //related to a pool
    
    LightfsCtx(const char *user, const char *this_pool = LIGHTFS_POOL, const char *myconf = LIGHTFS_CONF) 
      : userid(user), pool(this_pool), conf(myconf) {}
    ~LightfsCtx() {}
  
    int ctx_init(); 
    void ctx_destroy();
  
  };
  
  class LightfsInode {
  public:
    ino_t ino;
    Inode* parent;
    map<string, LightfsInode*> subdirs;
    //Metadata
    int mtime,ctime,atime;
    int uid,gid;
    int mode;
    //
    int type;
  };
  
  class Lightfs {
  public:
    lightfs_inode_t root;
  
  public:
    map<ino_t, Inode *> caches;
  
    LightfsCtx *lctx;
    set<lightfs_inode_t *> inode_cache;
    map<LightfsDentry *, set<LightfsDentry *> > dentry_root;
    //to do :others like superblock
    Lightfs(LightfsCtx *ctx) : root(ROOT_INO, S_IFDIR), lctx(ctx) {}
    ~Lightfs() {}
     
    int create_lightfs();
    int stat_inode(lightfs_inode_t *inode);
  
    /* inode ops */
    int mkdir(_inodeno_t pino, const char *name, mode_t mode);

    int readdir(_inodeno_t myino, size_t count, std::map<std::string, bufferlist> &out_vals);
    int lookup(_inodeno_t pino, const char *name, _inodeno_t *ino);
    int do_rmdir(_inodeno_t myino);
    int rmdir(_inodeno_t pino, const char *name);
    int rmdir_posix(lightfs_inode_t *parent, const char *name);
    int rename(); 
  
    /* file ops */
    int open();
    int write();
    int read();
    int truncate();
  
  
    /*fuse lowlevel ops API*/
    int ll_mkdir();
    int ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
  
  };
  
  class InoGen {
  private:
    _inodeno_t ino; //uint64_t
    /*
           high                      low
            00 00 0000 0000 ... 0000
             | \+-----------------+/
            \/           ||
          gen_bits (BITS - gen_bits)
    */
    uint32_t gen_bits; // eg. gen_bits = 8, 2^gen_bits = 256, range = [00 ... FF]
  public:
    LightfsCtx *lctx; //ino must in a LightfsCtx
    
    InoGen(LightfsCtx *ctx, uint32_t bits = 2) : gen_bits(bits), lctx(ctx) {}
    ~InoGen() {}
  
    void encode_gen(uint64_t &inum, uint64_t &max, bufferlist &bl);
    void decode_gen(uint64_t &inum, uint64_t &max, bufferlist &bl);
    int init_gen();
    int init_gen_omap();
    _inodeno_t get_ino() { return ino; }
    int do_generate_ino(string oid);
    int do_generate_ino_omap(string oid, bool is_root);
    _inodeno_t generate_ino();
    int destroy_gen();
  
    friend class Lightfs; //InoGen shares member variables {ino, gen_bits, lctx ...} with Lightfs
  };
  
  class LightfsDentry {
  public:
    string dname;
    lightfs_inode_t *inode;
    LightfsDentry *parent;
    set<LightfsDentry *> subs; //dentry cache  
  };
  
  class LightfsDir {
  public:
    lightfs_inode_t *inode;
    set<LightfsDir *> subdirs;
  };
  
  class FuseHandle {
  public:
    Lightfs *fs;
    struct fuse_chan *chan;
    struct fuse_session *session;
    char *mount_point;
    int fd;
    
    FuseHandle(Lightfs *ifs, int ifd) : fs(ifs), chan(NULL), session(NULL), fd(ifd) {}
    ~FuseHandle() {}
   
    int init(int argc, const char *argv[]);
    int loop();
    void finalize(); 
    
  };

  class Log {
/*
  private:
    // pair <pino, ino>
    std::deque <std::pair<uint64_t, uint64_t> *> log_queue;
    uint32_t log_bits; 
  public:
    LightfsCtx *lctx;

    Log(LightfsCtx *ctx, uint32_t bits) : log_queue<std::pair<uint64_t, uint64_t> *>(0), log_bits(bits), lctx(ctx) {}
    ~Log() {}
    int init_log();
    int loop()
    {
      std::pair<uint64_t, uint64_t> *entry = NULL;
      while ( !log_queue.empty() ) {
	entry = log_queue.front();
	log_queue.pop_front(); 
	printf("<%016lx, %016lx\n>", entry->first, entry->second);
	free(entry);
        entry = NULL;
      }

    }
    int process()
    {
    }   
    int add_log(uint64_t pino, uint64_t ino)
    {
      std::pair<uint64_t, uint64_t> *entry = new std::pair<uint64_t, uint64_t>(pino, ino);
      log_queue.push_back(entry);
      printf("size = %d, max_size = %d\n", log_queue.size(), log_queue.max_size());
    }
*/
  };
}
#endif
