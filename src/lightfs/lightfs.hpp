#ifndef __LIGHTFS_HEADER__
#define __LIGHTFS_HEADER__

#include <string>
#include <map>
#include <fuse/fuse_lowlevel.h>

#include "common/Mutex.h"

#include "include/xlist.h"

#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"
#include "include/stat.h"

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

  class CRoot;

  class CInode
  {
  protected:
    xlist<CInode *>::item _lru;
    int _ref;
    string _name;
    CRoot *_root;
    CInode *_parent;
    inodeno_t _ino;
    std::map<std::string, CInode *> _subdirs;
    CInode();
  public:
    ~CInode();

    void get();
    void put();

    int add(const std::string &name, inodeno_t ino, CInode **result);
    int remove(const std::string &name);
    int rename(const std::string &oldname, const std::string &newname);
    const std::map<std::string, CInode *>& subdirs();

    inodeno_t ino() { return _ino; }
    CInode *parent() { return _parent; }
    const std::string & name() { return _name; }

  public:
    inode_t inode;
  };

  class CRoot: public CInode
  {
    friend class CInode;
  private:
    xlist<CInode *> _lru_list;
    std::map<inodeno_t, CInode *> _cache;
  public:
    CRoot();
    ~CRoot();

    void trim(int count);
  };

  void get_inode_oid(inodeno_t ino, std::string &oid);
  void get_file_oid(inodeno_t ino, off_t off, std::string &oid);

  struct Fh {
    inodeno_t ino;
    inode_t *inode;
    
    off_t pos;
    int mode;
    
    int flags;
    
    Fh() : ino(-1), inode(NULL), pos(0), mode(0), flags(0) {}
  };
  
  class Lightfs
  {
  private:
    bool has_root;
    long unsigned order; //object size = 1 << order;
  public:
    std::map<inodeno_t, CInode *> _inode_cache;
    librados::IoCtx *_ioctx;
    InoGenerator *_ino_gen;
    
    Lightfs(librados::IoCtx *ctx, InoGenerator *ino_gen, const long unsigned myorder = 22) :
      has_root(false), order(myorder), _inode_cache(), _ioctx(ctx), _ino_gen(ino_gen)
    {}
    ~Lightfs() {}
  
    /* helper */
    void file_to_objects(off_t file_off, off_t file_len, std::map<off_t, std::pair<off_t, off_t> > &objects);
    void objects_to_file(std::map<off_t, std::pair<off_t, off_t> > objects, off_t &off, off_t &len);
    void fill_stat(struct stat *attr, inodeno_t ino, inode_t inode);

    bool create_root();
    
    /* inode ops */
    int do_mkdir(inodeno_t pino, const char *name, inodeno_t myino, inode_t &inode);
    int mkdir(inodeno_t pino, const char *name, inodeno_t *ino, inode_t &inode);
    int readdir(inodeno_t ino, std::map<std::string, inodeno_t> &result);
    int rmdir(inodeno_t pino, const char *name);
    int lookup(inodeno_t pino, const char *name, inodeno_t &ino);
    int rename(inodeno_t pino, const char *oldname, const char *newname);
    int getattr(inodeno_t myino, inode_t &inode);
    
    /* file ops */
    int open(inodeno_t myino, int flags, Fh *fh);
    int lseek(Fh *fh, off_t off, int whence);
    int read(Fh *fh, off_t off, off_t len, bufferlist *bl);
    int write(Fh *fh, off_t off, off_t len, const char *data);
    int truncate();

    /* fuse lowlevel ops */
    int ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, 
		struct stat *attr);
    int ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
		struct fuse_file_info *fi);
    int ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
    int ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name,
		struct stat *attr);
    int ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name, 
		fuse_ino_t newparent, const char *newname);
    int ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi);
    int ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, 
		struct fuse_file_info *fi);
    int ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, 
 		struct fuse_file_info *fi);
  };
};

#endif

