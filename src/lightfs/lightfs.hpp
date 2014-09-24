#ifndef __LIGHTFS_HEADER__
#define __LIGHTFS_HEADER__

#include <string>
#include <map>
#include <fuse/fuse_lowlevel.h>
#include <libgen.h>

#include "common/Mutex.h"

#include "include/xlist.h"

#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"
#include "include/stat.h"
#include "osdc/ObjectCacher.h"
#include "osdc/WritebackHandler.h"

#define ROOT_INO 1
#define ROOT_PARENT 2

#define MAX_NAME_LEN 256
#define VALID_NAME(x) ((1 + strlen((x))) <= MAX_NAME_LEN) ? 1 : 0

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

    CInode * get(inodeno_t ino);
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

    ObjectCacher::ObjectSet oset;
    off_t new_size;
    
    Fh() : ino(-1), inode(NULL), pos(0), mode(0), flags(0)
          , oset(this, 0, 0)
          , new_size(0)
    {}
  };

  struct dir_buffer {
    inodeno_t ino;
    std::map<std::string, inodeno_t> buffer;
    dir_buffer() : ino(0), buffer() {}
    ~dir_buffer() {}
  };

  class LightfsWriteback: public WritebackHandler
  {
  private:
    librados::IoCtx _ioctx;
    Mutex &_mutex;
    ceph_tid_t _tid;
  public:
    LightfsWriteback(librados::IoCtx &ioctx, Mutex &mutex);
    virtual void read(const object_t& oid, const object_locator_t& oloc,
		    uint64_t off, uint64_t len, snapid_t snapid,
		    bufferlist *pbl, uint64_t trunc_size,  __u32 trunc_seq,
		    Context *onfinish);
    virtual bool may_copy_on_write(const object_t& oid, uint64_t read_off, uint64_t read_len, snapid_t snapid);
    virtual ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
			   uint64_t off, uint64_t len, const SnapContext& snapc,
			   const bufferlist &bl, utime_t mtime,
			   uint64_t trunc_size, __u32 trunc_seq,
			   Context *oncommit);
  };

  class Lightfs
  {
  private:
    LightfsWriteback *writeback_handler;
    ObjectCacher *objectcacher;
    Mutex mutex;
    bool has_root;
    long unsigned order; //object size = 1 << order;
  public:
    std::map<inodeno_t, CInode *> _inode_cache;
    librados::IoCtx *_ioctx;
    InoGenerator *_ino_gen;
    
    Lightfs(librados::IoCtx *ctx, InoGenerator *ino_gen, const long unsigned myorder = 22);
    ~Lightfs();
  
    /* helper */
    void file_to_objects(off_t file_off, off_t file_len, std::map<off_t, std::pair<off_t, off_t> > &objects);
    void objects_to_file(std::map<off_t, std::pair<off_t, off_t> > objects, off_t &off, off_t &len);
    void fill_stat(struct stat *st, inodeno_t ino, inode_t inode, 
		dev_t rdev = 0, dev_t dev = 0, nlink_t l = 1);
    utime_t lightfs_now();
    int path_walk(const char *pathname, char *out_parent, bool out_exit);
    int write_symlink(inodeno_t ino, string target, inode_t inode);
    int read_symlink(inodeno_t ino, string &target);

    bool create_root();
    
    /* inode ops */
    int do_mkdir(inodeno_t pino, const char *name, inodeno_t myino, inode_t &inode);
    int mkdir(inodeno_t pino, const char *name, inodeno_t *ino, inode_t &inode);
    int rmdir(inodeno_t pino, const char *name);
    int symlink(const char *link, inodeno_t pino, const char *name,
		inodeno_t ino, inode_t inode);
    int readlink(inodeno_t ino, string &linkname);
    int lookup(inodeno_t pino, const char *name, inodeno_t &ino);
    int rename(inodeno_t pino, const char *oldname, const char *newname);
    int setattr(inodeno_t ino, inode_t &inode, int mask);
    int getattr(inodeno_t myino, inode_t &inode);
    
    /* file ops */
    int mknod(inodeno_t pino, const char *name, mode_t mode, inodeno_t &ino, inode_t &inode);
    int create(inodeno_t pino, const char *name, int mode, int flags,
		 inodeno_t &ino, Fh *fh, int uid = -1, int gid = -1);
    int unlink(inodeno_t pino, const char *name);
    int open(inodeno_t myino, int flags, Fh *fh);
    int release(Fh *fh);
    int lseek(Fh *fh, off_t off, int whence);
    int read(Fh *fh, off_t off, off_t len, bufferlist *bl);
    int write(Fh *fh, off_t off, off_t len, const char *data);
    int truncate();
    int opendir(inodeno_t ino, dir_buffer *d_buffer);
    int readdir(inodeno_t ino, std::map<std::string, inodeno_t> &result);
    int releasedir(dir_buffer *d_buffer);

    /* fuse lowlevel ops : inode ops */
    int ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, 
		struct stat *attr);
    int ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name);
    int ll_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
		const char *name, struct stat *attr);
    int ll_readlink(fuse_req_t req, fuse_ino_t ino, string &linkname);
    int ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name,
		struct stat *attr);
    int ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name, 
		fuse_ino_t newparent, const char *newname);
    int ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
		int to_set, Fh *fh);
    int ll_getattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr);

    /* fuse lowlevel ops : file ops */
    int ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode,
		dev_t rdev, struct stat *attr);
    int ll_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode,
		struct stat *attr, int flags, Fh **fhp);
    int ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name);
    int ll_open(fuse_req_t req, fuse_ino_t ino, int flags, Fh **fhp);
    int ll_release(fuse_req_t req, Fh *fh);
    int ll_read(fuse_req_t req, size_t size, off_t off, Fh *fh, bufferlist *bl);
    int ll_write(fuse_req_t req, off_t off, size_t size, const char *buf, Fh *fh);
    int ll_opendir(fuse_req_t req, fuse_ino_t ino, dir_buffer **d_buffer);
    int ll_readdir(fuse_req_t req, fuse_ino_t ino, off_t off, off_t size,
		off_t *fill_size, char *buf, dir_buffer *d_buffer);
    int ll_releasedir(fuse_req_t req, dir_buffer *d_buffer);
  };
};

#endif

