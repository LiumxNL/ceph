#include <stdlib.h>
#include <string.h>
#include <string>

#include "lightfs.hpp"
#include "cls/lightfs/cls_lightfs_client.h"

using namespace librados;
using namespace std;

namespace lightfs
{
  InoGenerator::InoGenerator(const IoCtx &ioctx)
    :_mutex("InoGenerator")
    ,_bits(0)
  {
    _ioctx.dup(ioctx);
  }

  InoGenerator::~InoGenerator()
  {
    close();
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

    Mutex::Locker lock(_mutex);

    int r;
    for (int i = 0; i < (1 << bits); ++i) {
      string oid;
      get_oid(i, oid);

      r = cls_client::create_seq(&_ioctx, oid, i ? 0 : (ROOT_PARENT+1));
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

  int InoGenerator::open()
  {
    if (_bits)
      return 0;

    Mutex::Locker lock(_mutex);

    int r;

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

    return 0;
  }

  void InoGenerator::close()
  {
    _bits = 0;
  }

  int InoGenerator::generate(inodeno_t &ino)
  {
    int r;

    if (_bits == 0)
      return -EINVAL;

    Mutex::Locker lock(_mutex);

    uint64_t max = (1 << (64 - _bits));
    while (true) {
      long unsigned index = rand() & ((1 << _bits) - 1);
      string oid;
      get_oid(index, oid);

      r = cls_client::read_seq(&_ioctx, oid, ino);
      if (r < 0)
        return r;

      if (ino == max)
        continue;

      r = cls_client::write_seq(&_ioctx, oid, ino);
      if (r < 0) {
        if (r == -ERANGE || r == -ENOEXEC)
          continue;
        return r;
      }

      ino |= index << (64 - _bits);
      return 0;
    }
  }

  CInode::CInode()
    :_lru(this)
    ,_ref(1)
  {}

  CInode::~CInode()
  {
    assert(_root);
    assert(_parent == NULL);

    if (_root != this) {
      assert(_ref == 0);
      _root->_cache.erase(_ino);
    }
  }

  void CInode::get()
  {
    if (_ref++ == 0) {
      assert(_parent);
      _root->_lru_list.remove(&_lru);
    }
  }

  void CInode::put()
  {
    if (--_ref == 0) {
      if (_parent)
        _root->_lru_list.push_back(&_lru);
      else
        delete this;
    }
  }

  int CInode::add(const std::string &name, inodeno_t ino, CInode **result)
  {
    if (_subdirs.count(name))
      return -EEXIST;

    if (_root->_cache.count(ino))
      return -EEXIST;

    CInode *sub = new CInode();
    sub->_root = _root;
    sub->_parent = this;
    sub->_ino = ino;
    sub->_name = name;
    _subdirs[name] = sub;
    _root->_cache[ino] = sub;
    get();

    if (result)
      *result = sub;
    else
      sub->put();

    return 0;
  }

  int CInode::remove(const std::string &name)
  {
    map<string, CInode*>::iterator pos = _subdirs.find(name);
    if (pos == _subdirs.end())
      return -ENOENT;

    CInode *inode = pos->second;

    inode->get();

    assert(inode->_name == name);
    inode->_parent = NULL;
    _subdirs.erase(pos);
    put();

    inode->put();

    return 0;
  }

  int CInode::rename(const std::string &oldname, const std::string &newname)
  {
    map<string, CInode*>::iterator pos = _subdirs.find(oldname);
    if (pos == _subdirs.end())
      return -ENOENT;
    if (_subdirs.count(newname))
      return -EEXIST;

    CInode *inode = pos->second;
    assert(inode->_name == oldname);
    _subdirs.erase(pos);
    _subdirs[newname] = inode;
    inode->_name = newname;

    return 0;
  }

  const map<string, CInode *>& CInode::subdirs()
  {
    return _subdirs;
  }

  CRoot::CRoot()
  {
    _root = this;
    _parent = NULL;
    _ino = ROOT_INO;
    _cache[ROOT_INO] = this;
  }

  CRoot::~CRoot()
  {
    assert(_cache.size() == 1);
  }

  CInode * CRoot::get(inodeno_t ino)
  {
    std::map<inodeno_t, CInode *>::iterator pos = _cache.find(ino);
    if (pos == _cache.end())
      return NULL;

    CInode *result = pos->second;
    result->get();
    return result;
  }

  void CRoot::trim(int count)
  {
    while (!_lru_list.empty() && count) {
      CInode *inode = _lru_list.front();
      inode->get();
      int r = inode->parent()->remove(inode->name());
      assert(r == 0);
      inode->put();

      --count;
    }
  }

  void get_inode_oid(inodeno_t ino, std::string &oid)
  {
    char oid_str[32] = {'\0'};
    //ino starts with 1 
    if (ino < 1) { 
      return;
    }
    snprintf(oid_str, sizeof(oid_str), "inode.%016lX", ino);
    oid = oid_str;
  }

  void get_file_oid(inodeno_t ino, off_t index, std::string &oid)
  {
    char oid_str[32] = {'\0'};
    if (ino == 0)
      return;
    snprintf(oid_str, sizeof(oid_str), "data.%016lX.%08lX", ino, index);
    oid = oid_str;    
  }

  void release_comp_callback(librados::completion_t cb, void *arg)
  {
    librados::AioCompletion *comp = static_cast<librados::AioCompletion *>(cb);
    cout << "will release comp if it exits" << std::endl;
    if (comp && comp->pc)
      comp->release();
  }

  /* class LightfsWriteback */

  void context_cb(rados_completion_t c, void *arg)
  {
    Context *con = reinterpret_cast<Context *>(arg);
    con->complete(rados_aio_get_return_value(c));
  }

  class C_Request : public Context {
  public:
    C_Request(Context *c, Mutex *l)
      : m_ctx(c), m_lock(l) {}
    virtual ~C_Request() {}
    virtual void finish(int r) {
      assert(!m_lock->is_locked());
      Mutex::Locker l(*m_lock);
      m_ctx->complete(r);
    }
  private:
    Context *m_ctx;
    Mutex *m_lock;
  };

  LightfsWriteback::LightfsWriteback(librados::IoCtx &ioctx, Mutex &mutex)
    : _ioctx(ioctx)
    , _mutex(mutex)
    , _tid(0)
  {
  }

  void LightfsWriteback::read(const object_t& oid, const object_locator_t& oloc,
		              uint64_t off, uint64_t len, snapid_t snapid,
                              bufferlist *pbl, uint64_t trunc_size,  __u32 trunc_seq,
                              Context *onfinish)
  {
    Context *req = new C_Request(onfinish, &_mutex);
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(req, context_cb, NULL);
    librados::ObjectReadOperation op;
    op.read(off, len, pbl, NULL);
    int r = _ioctx.aio_operate(oid.name, rados_completion, &op,
					 0, NULL);
    rados_completion->release();
    assert(r >= 0);
  }

  bool LightfsWriteback::may_copy_on_write(const object_t& oid, uint64_t read_off, uint64_t read_len, snapid_t snapid)
  {
    return false;
  }

  ceph_tid_t LightfsWriteback::write(const object_t& oid, const object_locator_t& oloc,
                                     uint64_t off, uint64_t len, const SnapContext& snapc,
                                     const bufferlist &bl, utime_t mtime,
                                     uint64_t trunc_size, __u32 trunc_seq,
                                     Context *oncommit)
  {
    Context *req = new C_Request(oncommit, &_mutex);
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(req, NULL, context_cb);
    librados::ObjectWriteOperation op;
    op.write(off, bl);
    std::vector<snap_t> snaps;
    int r = _ioctx.aio_operate(oid.name, rados_completion, &op,
					 0, snaps);
    rados_completion->release();

    return ++_tid;
  }

  /* class Lightfs */

  Lightfs::Lightfs(librados::IoCtx *ctx, InoGenerator *ino_gen, const long unsigned myorder)
    : mutex("lightfs_client")
    , has_root(false), order(myorder), _inode_cache(), _ioctx(ctx), _ino_gen(ino_gen)
  {
    CephContext *cct = (CephContext *)ctx->cct();
    writeback_handler = new LightfsWriteback(*ctx, mutex);
    objectcacher = new ObjectCacher(cct, "lightfs", *writeback_handler, mutex, NULL, NULL,
                                    cct->_conf->client_oc_size,
                                    cct->_conf->client_oc_max_objects,
                                    cct->_conf->client_oc_max_dirty,
                                    cct->_conf->client_oc_target_dirty,
                                    cct->_conf->client_oc_max_dirty_age,
                                    true);
  }

  Lightfs::~Lightfs()
  {
    delete writeback_handler;
    delete objectcacher;
  }

  /* helper */
  void Lightfs::file_to_objects(off_t file_off, off_t file_len, std::map<off_t, std::pair<off_t, off_t> > &objects)
  {
    off_t first = file_off; // point to first object 
    off_t end_pos = file_off + file_len; //end pos in file
    off_t last = end_pos - 1; // last object
    
    // first object init
    off_t first_index = first >> order; // first / object_size
    off_t first_off = first & ((1 << order) - 1); // first % object_size
    off_t first_len = 0;
    //last object init
    off_t last_index = last >> order;
    off_t last_off = last & ((1 << order) - 1);
    off_t last_len = 0;

    //first object
    if (first_index == last_index) {
      first_len = file_len;  
      if (first_len == 0)
        return;
      objects[first_index] = std::make_pair(first_off, first_len);
      return;
    }

    // if more than one object
    first_len = (1 << order) - first_off;
    
    off_t index = first_index;
    off_t off = first_off;
    off_t len = 0;
    
    //middle objects
    while (++index < last_index) {
      off = 0;
      len = (1 << order);
      objects[index] = std::make_pair(off, len);
    }
    
    //last object
    last_len = end_pos - (last_index + 1) * (1 << order);
    if (last_len == 0)
      return;
    last_off = 0;
    objects[last_index] = std::make_pair(last_off, last_len);
  }

  void Lightfs::objects_to_file(std::map<off_t, std::pair<off_t, off_t> > objects, off_t &off, off_t &len)
  {
    off = 0;
    len = 0;
    std::map<off_t, std::pair<off_t, off_t> >::iterator p = objects.begin();
    off = p->second.first;
    for (p = objects.begin(); p != objects.end(); ++p) {
      len += p->second.second;
    }
  }
  
  void Lightfs::fill_stat(struct stat *st, inodeno_t ino, inode_t inode,
		 dev_t rdev, dev_t dev, nlink_t l)
  {
    /*
      dev_t : unsigned long int
      nlink_t: unsigned long int
    */
    st->st_ino = ino;
    st->st_dev = dev; //default: no device
    st->st_mode = inode.mode;
    st->st_rdev = rdev; //default: no rdevice
    st->st_nlink = l;  //default: we don't support hard link currently
    st->st_uid = inode.uid;
    st->st_gid = inode.gid;
    st->st_size = inode.size;

    if (inode.mode & S_IFMT == S_IFDIR)
      st->st_blocks = 1;
    else 
      st->st_blocks = (inode.size + 511) >> 9; // 1 byte also needs 512B, so (1+511) >> 9 == 1

    st->st_blksize = 4096; // I/O size is the same as page size

    if (inode.ctime.sec() > inode.mtime.sec()) {
      stat_set_ctime_sec(st, inode.ctime.sec());
      stat_set_ctime_nsec(st, inode.ctime.nsec());
    } else {
      stat_set_ctime_sec(st, inode.mtime.sec());
      stat_set_ctime_nsec(st, inode.mtime.nsec());
    }

    stat_set_atime_sec(st, inode.atime.sec());
    stat_set_atime_nsec(st, inode.atime.nsec());
    stat_set_mtime_sec(st, inode.mtime.sec());
    stat_set_mtime_nsec(st, inode.mtime.nsec());
  }

  utime_t Lightfs::lightfs_now()
  {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    utime_t t(&tv);
    return t;
  }

  int Lightfs::path_walk(const char *pathname, char *out_parent, bool out_exit)
  {
    return 0;
  }

  int Lightfs::write_symlink(inodeno_t ino, string target, inode_t inode)
  {
    int r = -1;
    string oid;
    get_inode_oid(ino, oid);

    bufferlist valbl, inbl;
    valbl.append(target);
    inode.encode(ATTR_ALL, inbl); 

    string key("LINK");
    librados::ObjectWriteOperation wr;
    std::map<string, bufferlist> inmap;
    inmap[key] = valbl;
    wr.omap_set(inmap);
    wr.omap_set_header(inbl);
    r = _ioctx->operate(oid, &wr);
    cout << "write_symlink: operate = " << r << std::endl;
    return r;
  }

  int Lightfs::read_symlink(inodeno_t ino, string &target)
  {
    int r = -1;
    int rval = -1;
    bufferlist bl;
    string oid;
    get_inode_oid(ino, oid);
    
    std::set<string> keys;
    keys.insert("LINK");
    std::map<string, bufferlist> outmap;

    librados::ObjectReadOperation rd;
    rd.omap_get_vals_by_keys(keys, &outmap, &rval);
    r = _ioctx->operate(oid, &rd, &bl);
    cout << "read_symlink: operate = " << r << std::endl;
    if (r < 0)
      return r;
    std::map<string, bufferlist>::iterator p = outmap.begin();
    target = p->second.c_str();
    return 0;
  }


  /*lightfs member*/
  bool Lightfs::create_root()
  {
    cout << "create_root: has_root = " << has_root << std::endl;
    if (has_root)
      return true;
    int r = -1;
    inode_t inode;
    std::string oid;
    get_inode_oid(ROOT_INO, oid);
  
    struct timeval tv;
    gettimeofday(&tv, NULL);

    inode.ctime.set_from_timeval(&tv);
    inode.atime.set_from_timeval(&tv);
    inode.mtime.set_from_timeval(&tv);

    char buf[20];
    int t = snprintf(buf, sizeof(buf), "I.%08lX", (long unsigned)-1);
    inode.size = sizeof(inode) + sizeof("N.") + sizeof("N..") 
                + 2 * sizeof(inodeno_t) + 2 * t + sizeof(".") + sizeof("..");


    inode.mode = S_IFDIR;

    r = cls_client::create_inode(_ioctx, oid, true, inode);
    cout << "create_root: create_inode, r = " << r << std::endl;
    if (r < 0 && r != -EEXIST) 
      return false;
    r = cls_client::link_inode(_ioctx, oid, "..", ROOT_PARENT);	
    cout << "create_root: link_inode <.., ino> , r = " << r << std::endl;
    if (r < 0 && r != -EEXIST) 
      return false;
    r = cls_client::link_inode(_ioctx, oid, ".", ROOT_INO);
    cout << "create_root: link_inode <., ino>, r = " << r << std::endl;
    if (r < 0 && r != -EEXIST) 
      return false;
    has_root = true;

    return true;  
  }

  /* inode ops */  

  int Lightfs::do_mkdir(inodeno_t pino, const char *name, inodeno_t myino, inode_t &inode)
  {
    int r = -1;
    string oid;
    string poid;
    get_inode_oid(myino, oid);
    get_inode_oid(pino, poid);

    // 1. create child inode 
    r = cls_client::create_inode(_ioctx, oid, true, inode);
    if (r < 0)
        return r;
    
    // 2. add <N.name, myino> & <I.myino, name> entry to parent, and add "..", "." to child dir 
    r = cls_client::link_inode(_ioctx, poid, name, myino);
    if (r < 0)
      goto clean;
    r = cls_client::link_inode(_ioctx, oid, "..", pino); 
    if (r < 0)
      goto clean;
    r = cls_client::link_inode(_ioctx, oid, ".", myino); 
    if (r < 0)
      goto clean;

  clean:
    // 3. if 2 failed, remove child inode
    if (r < 0) {
      cls_client::remove_inode(_ioctx, oid);
      return r;
    } 
    // if 2 or 3 is interrupted(network disconnnected), log will clean(recycle) child inode

    return 0;
  }

  int Lightfs::mkdir(inodeno_t pino, const char *name, inodeno_t *out_ino, inode_t &inode)
  {
    int r = -1;
    if (!_ino_gen)
      return -EINVAL;
    inodeno_t ino = -1;
    _ino_gen->generate(ino);
    if (out_ino)
      *out_ino = ino;

    //cout << "<" << name << ", " << hex << ino << dec << ">" << std::endl;
    r = do_mkdir(pino, name, ino, inode);
    //cout << "do_mkdir = " << r << std::endl;
    if (r < 0)
      return r;
    return 0;
  }


  int Lightfs::rmdir(inodeno_t pino, const char *name)
  {
    int r = -1;
    string poid;
    get_inode_oid(pino, poid);

    inodeno_t ino = -1;
    // 1. lookup name's ino
    r = cls_client::find_inode(_ioctx, poid, name, ino);
    if (r < 0)
      return r;

    // 2. unlink <N.name, ino> & <I.ino, name> in parent
    // log backend will remove sub inode and its subs
    return cls_client::unlink_inode(_ioctx, poid, name, ino);
  }

  int Lightfs::symlink(const char *link, inodeno_t pino, const char *name,
                inodeno_t ino, inode_t inode)
  {
    int r = -1; 
    string oid, poid, target(link);
    get_inode_oid(ino, oid);
    get_inode_oid(pino, poid);
    
    r = write_symlink(ino, target, inode); 
    if (r < 0)
      return r;
    r = cls_client::link_inode(_ioctx, poid, name, ino);
    if (r < 0) {
      cls_client::remove_inode(_ioctx, oid);
      return r;
    }
    return 0;
  }

  int Lightfs::readlink(inodeno_t ino, string &linkname)
  {
    return read_symlink(ino, linkname); 
  }

  int Lightfs::lookup(inodeno_t pino, const char *name, inodeno_t &ino)
  {
    string poid;
    get_inode_oid(pino, poid);
    
    int r = -1;
    r = cls_client::find_inode(_ioctx, poid, name, ino);
    //cout << "lookup : r = " << r << std::endl;
    return r;
  }

  int Lightfs::rename(inodeno_t pino, const char *oldname, const char *newname)
  {
    int r = -1;
    string poid;
    get_inode_oid(pino, poid);
    inodeno_t ino = -1;
    
    r = cls_client::find_inode(_ioctx, poid, oldname, ino);    
    if (r < 0)
      return r;

    return cls_client::rename_inode(_ioctx, poid, oldname, newname, ino);
  }

  int Lightfs::setattr(inodeno_t ino, inode_t &inode, int mask)
  {
    string oid;
    get_inode_oid(ino, oid);
    return cls_client::update_inode(_ioctx, oid, mask, inode);
  }

  int Lightfs::getattr(inodeno_t myino, inode_t &inode)
  {
    string oid;
    get_inode_oid(myino, oid);    

    return cls_client::get_inode(_ioctx, oid, inode);
  }

  /* file ops */

  int Lightfs::mknod(inodeno_t pino, const char *name, mode_t mode, inodeno_t &ino, inode_t &inode)
  {
    int r = -1;
    string poid;
    get_inode_oid(pino, poid);
    string myname(name);

    r = cls_client::find_inode(_ioctx, poid, myname, ino);
    if (r == 0) {
      cout << name << " already exist, so return" << std::endl;
      return -EEXIST;
    }

    r = _ino_gen->generate(ino);
    if (r < 0) {
      cout << "generate ino failed, r = " << r << std::endl;
      return r;
    }

    string oid;
    get_inode_oid(ino, oid);

    struct timeval tv;
    gettimeofday(&tv, NULL);

    inode.ctime.set_from_timeval(&tv);
    inode.atime.set_from_timeval(&tv);
    inode.mtime.set_from_timeval(&tv);
    inode.mode = mode | S_IFREG;

    r = cls_client::create_inode(_ioctx, oid, true, inode);
    if (r < 0)
      return r;

    return cls_client::link_inode(_ioctx, poid, myname, ino);
    
  }

  int Lightfs::create(inodeno_t pino, const char *name, int mode, int flags, 
		inodeno_t &ino, Fh *fh, int uid, int gid)
  {
    cout << "create:" << name << " pino = " << pino << std::endl;
    int r = -1;
    string poid, oid;
    
    _ino_gen->generate(ino); 
    cout << "generate ino = " << hex << ino << dec << std::endl;
    get_inode_oid(ino, oid);
    get_inode_oid(pino, poid);

    inode_t *inodep = new inode_t();
    if (inodep == NULL)
      return -ENOMEM;
    inodep->ctime = lightfs_now();
    inodep->mode = mode;
    inodep->uid = uid;
    inodep->gid = gid;

    r = cls_client::create_inode(_ioctx, oid, true, *inodep);
    if (r < 0) 
      goto err;

    r = cls_client::link_inode(_ioctx, poid, name, ino);
    if (r < 0)
      goto err;

    fh->ino = ino;
    fh->inode = inodep;
    fh->pos = 0;
    fh->mode = mode;
    fh->flags = flags;
    fh->oset.ino = ino;
    fh->oset.poolid = _ioctx->get_id();
    return 0;

  err:
    delete inodep;
    inodep = NULL;
    return r;
  }

  int Lightfs::unlink(inodeno_t pino, const char *name)
  {
    //to fix: transaction: ops on {poid, oid, file_oid} 3 objects
    int r = -1;
    inodeno_t ino;
    inode_t inode;
    r = lookup(pino, name, ino);
    cout << "lookup "<< name << ": r = " << r << std::endl;
    if (r < 0)
      return r;
    r = getattr(ino, inode);
    cout << "getattr " << ino << ": r = " << r << std::endl;
    if (r < 0)
      return r;

    string poid, oid, foid;
    get_inode_oid(pino, poid);
    get_inode_oid(ino, oid);
    //reomve entry in parent inode object
    r = cls_client::unlink_inode(_ioctx, poid, name, ino);
    if (r < 0 && r != -ENOENT)
      return r;

    off_t len = inode.size;
    off_t count = (len >> order) + 1;
    off_t i = 0;
    //remove file object data.<ino>.<off>
    for (i = 0; i < count; i++) {
      get_file_oid(ino, i, foid);
      cout << foid << std::endl;
      librados::AioCompletion *comp = librados::Rados::aio_create_completion();
      r = _ioctx->aio_remove(foid, comp);
      comp->release();
      cout << "remove: r = " << r << std::endl;
      if (r < 0 && r != -ENOENT)
        return r;
    }
   
    cout << "inode = " << oid << std::endl;
    //remove file inode object 
    return cls_client::remove_inode(_ioctx, oid); 
  }

  int Lightfs::open(inodeno_t myino, int flags, Fh *fh)
  { 
    //if has cache, read from cache?
 
    //else read from osd
    cout << "open:" << std::endl;
    if (fh == NULL)
      return -1;
    string oid;
    get_inode_oid(myino, oid);
    inode_t *inodep = new inode_t();  
      
    int r = -1;
    r = cls_client::get_inode(_ioctx, oid, *inodep);
    if (r < 0) {
      delete inodep;
      inodep = NULL;
      return r;
    }
    
    fh->ino = myino;
    fh->inode = inodep;
    fh->pos = 0;
    fh->mode = inodep->mode;
    fh->flags = flags;
    fh->oset.ino = myino;
    fh->oset.poolid = _ioctx->get_id();
 
    return 0;
  }

  int Lightfs::release(Fh *fh)
  {
    if (fh == NULL)
      return -1;
    if (fh->inode == NULL)
      return -1;
    delete fh->inode;
    {
      //This code is just a temporary solution. This should be place at 'fsync'.
      Mutex::Locker lock(mutex);

      Mutex flock("Lightfs::_release flock");
      Cond cond;
      bool done = false;
      Context *onfinish = new C_SafeCond(&flock, &cond, &done);

      objectcacher->flush_set(&fh->oset, onfinish);

      mutex.Unlock();
      flock.Lock();
      while (!done)
        cond.Wait(flock);
      flock.Unlock();
      mutex.Lock();

      objectcacher->release_set(&fh->oset);
    }
    fh->inode = NULL;
    delete fh;
    fh = NULL;
    return 0;
  }

  int Lightfs::lseek(Fh *fh, off_t off, int whence)
  {
    int r = -1;
    inode_t *inode = fh->inode;
    switch (whence) {
    case SEEK_SET:
      fh->pos = off;
      break;
    case SEEK_CUR:
      fh->pos += off;
      break;
    case SEEK_END:
      r = getattr(fh->ino, *inode);
      if (r < 0)
        return r;
      fh->pos = inode->size + off;
      break;
    }
    return 0;
  }

  int Lightfs::read(Fh *fh, off_t off, off_t len, bufferlist *bl)
  {
    cout << "Lightfs:: read off = " << off << ", len = " << len << std::endl;
    int r = -1;
    if (fh == NULL) {
      cout << "file hanlder is NULL..." << std::endl;
      return -1;
    }

    ceph_file_layout layout;
    memset(&layout, 0, sizeof(layout));
    layout.fl_object_size = 4 << 20;
    layout.fl_stripe_unit = 4 << 20;
    layout.fl_stripe_count = 1;
    layout.fl_pg_pool = _ioctx->get_id();

    Mutex::Locker lock(mutex);

    int rvalue = 0;
    Mutex flock("Lightfs::_read flock");
    Cond cond;
    bool done = false;
    Context *onfinish = new C_SafeCond(&flock, &cond, &done, &rvalue);

    r = objectcacher->file_read(&fh->oset, &layout, 0,
                                off, len,
                                bl,
                                0,
                                onfinish);
    if (r == 0) {
      mutex.Unlock();
      flock.Lock();
      while (!done)
        cond.Wait(flock);
      flock.Unlock();
      mutex.Lock();
      r = rvalue;
    } else {
      delete onfinish;
    }

    return r;
  }

  int Lightfs::write(Fh *fh, off_t off, off_t len, const char *data)
  {
    cout << "Lightfs::write off = " << off << ", len = " << len << std::endl;
    int r = -1;

    //to fix: off+len > max_file_size
    if (fh == NULL) {
      cout << "file hanlder is NULL..." << std::endl;
      return -1;
    }

    Mutex::Locker lock(mutex);

    ceph_file_layout layout;
    memset(&layout, 0, sizeof(layout));
    layout.fl_object_size = 4 << 20;
    layout.fl_stripe_unit = 4 << 20;
    layout.fl_stripe_count = 1;
    layout.fl_pg_pool = _ioctx->get_id();

    bufferptr bp;
    if (len > 0) bp = buffer::copy(data, len);
    bufferlist bl;
    bl.push_back(bp);

    SnapContext snap;

    r = objectcacher->file_write(&fh->oset, &layout, snap,
			         off, len, bl, ceph_clock_now((CephContext *)_ioctx->cct()), 0,
			         mutex);
    if (r < 0)
      return r;

    r = len;
    
    //to fix: transaction->metadata update
    cout << "will update metadata" << std::endl;
    inode_t inode;
    r = getattr(fh->ino, inode);
    inode.mtime = lightfs_now();
    inode.ctime = inode.mtime;
    off_t end_pos = off + len;
    inode.size = inode.size > end_pos ? inode.size : end_pos;
    int used_attr = 0;
    used_attr |= ATTR_MTIME | ATTR_SIZE;
    string ioid;
    get_inode_oid(fh->ino, ioid);
    //to fix: sync io -> async io
    r = cls_client::update_inode(_ioctx, ioid, used_attr, inode);  
    cout << "update_file_inode = " << r << std::endl;

    return len;
  }

  int Lightfs::opendir(inodeno_t ino, dir_buffer *d_buffer)
  {
    d_buffer->ino = ino;
    return 0;
  }

  int Lightfs::readdir(inodeno_t ino, std::map<std::string, inodeno_t> &result)
  {
    int r = -1;
    long unsigned max_return = (uint64_t)-1;

    string oid;
    get_inode_oid(ino, oid);
    return cls_client::list_inode(_ioctx, oid, "", max_return, &result);
  }

  int Lightfs::releasedir(dir_buffer *d_buffer)
  {
    if (d_buffer != NULL) {
      cout << "d_buffer != NULL " << std::endl;
      delete d_buffer;
      d_buffer = NULL;
      return 0;
    }
    return -1;
  }

  /* fuse lowlevel ops : inode ops */

  int Lightfs::ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode,  
                struct stat *attr)
  {
    int r = -1;
    const struct fuse_ctx *fctx = fuse_req_ctx(req);

    inodeno_t ino = -1;
    inode_t inode;
    struct timeval tv;
    gettimeofday(&tv, NULL);

    inode.ctime.set_from_timeval(&tv); 
    inode.atime.set_from_timeval(&tv); 
    inode.mtime.set_from_timeval(&tv); 

    /*
	dir size:
	struct inode_t
	<N.. ino>  & <I.ino .>
	<N... ino> & <I.ino ..>
    */
    char buf[20];
    int t = snprintf(buf, sizeof(buf), "I.%08lX", (long unsigned)-1);
    inode.size = sizeof(inode) + sizeof("N.") + sizeof("N..") 
		+ 2 * sizeof(inodeno_t) + 2 * t + sizeof(".") + sizeof("..");

    inode.mode = mode | S_IFDIR;
    inode.uid = fctx->uid;
    inode.gid = fctx->gid;    
    
    r = mkdir(parent, name, &ino, inode); 
    if (r < 0)
      return r;
    
    fill_stat(attr, ino, inode);
    return 0;
  }

  int Lightfs::ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
  {
    return rmdir(parent, name);
  }

  int Lightfs::ll_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                const char *name, struct stat *attr)
  {
    int r = -1;
    inodeno_t ino;
    inode_t inode;
    
    _ino_gen->generate(ino);
    inode.mode |= S_IFLNK | 0644;
    inode.ctime = lightfs_now();

    r = symlink(link, parent, name, ino, inode);
    if (r < 0)
      return r;

    fill_stat(attr, ino, inode);
    return 0;
  }

  int Lightfs::ll_readlink(fuse_req_t req, fuse_ino_t ino, string &linkname)
  {
    return readlink(ino, linkname); 
  }

  int Lightfs::ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name,
                struct stat *attr)
  {
    cout << "ll_lookup: parent = " << parent << std::endl;
    int r = -1;
    inodeno_t ino = -1;

    if (parent != ROOT_PARENT) {
      r = lookup(parent, name, ino);
      cout << "lookup = " << r << std::endl;
      if (r < 0)
        return r;
    } else {
      ino = ROOT_INO;
    }
    
    inode_t inode;
    r = getattr(ino, inode);
    if (r < 0)
      return r;
    
    fill_stat(attr, ino, inode);
    return 0;
  }

  int Lightfs::ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                fuse_ino_t newparent, const char *newname)
  {
    //to fix: rename parent/name to newparent/newname
    //non POSIX compatable
    return rename(parent, name, newname);
  }

  // set attr to inode
  int Lightfs::ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                int to_set, Fh *fh)
  {
    int r = -1;
    int mask = 0;
    inode_t inode;

    //get inode info from osd
    r = getattr(ino, inode);
    if (r < 0)
      return r;

    if (to_set & FUSE_SET_ATTR_MODE) {
      mask |= ATTR_MODE;
      inode.mode = attr->st_mode;
    }
    if (to_set & FUSE_SET_ATTR_UID) {
      mask |= ATTR_UID;
      inode.uid = attr->st_uid;
    }
    if (to_set & FUSE_SET_ATTR_GID) {
      mask |= ATTR_GID;
      inode.gid = attr->st_gid;
    }
    if (to_set & FUSE_SET_ATTR_SIZE) {
      mask |= ATTR_SIZE;
      inode.size = attr->st_size;
    }
    if (to_set & FUSE_SET_ATTR_ATIME) {
      mask |= ATTR_ATIME;
      inode.atime = utime_t(stat_get_atime_sec(attr), stat_get_atime_nsec(attr));
    }
    if (to_set & FUSE_SET_ATTR_MTIME) {
      mask |= ATTR_MTIME;
      inode.mtime = utime_t(stat_get_mtime_sec(attr), stat_get_mtime_nsec(attr));
    }

    mask |= ATTR_CTIME;
    inode.ctime = lightfs_now();

    r = setattr(ino, inode, mask);    
    if (r < 0)
      return r;
    fill_stat(attr, ino, inode);
    return 0;
  }

  int Lightfs::ll_getattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr)
  {
    int r = -1;
    inode_t inode;
    r = getattr(ino, inode);
    cout << "ll_getattr: getattr = " << r << std::endl;
    if (r < 0)
      return r;
    fill_stat(attr, ino, inode);
    return 0;
  }

  /* fuse lowlevel ops : file ops */ 
  int Lightfs::ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, 
                dev_t rdev, struct stat *attr)
  {
    int r = -1;
    inode_t inode;
    inodeno_t ino = 0;

    r = mknod(parent, name, mode, ino, inode);

    if (r < 0) {
      cout << "ll_mknod: mknod failed, r = " << r << std::endl;
      return r;
    }
   
    fill_stat(attr, ino, inode, rdev); 
    return 0; 
  }

  int Lightfs::ll_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode,
                struct stat *attr, int flags, Fh **fhp)
  {
    int r = -1;
    inode_t inode;
    inodeno_t ino;
    if (fhp == NULL)
      return -1;
    
    if (flags & O_CREAT == 0) {
      cout << "ll_create: flags is not O_CREAT, failed" << std::endl;
      return -EBADF;
    }    

    r = lookup(parent, name, ino);
    if (r == 0) {
      if (flags & O_EXCL)
	return -EEXIST;
      else 
	return -1;
    } else if (r < 0) {
      if (r != -ENOENT) {
	cout << "ll_create failed, r = " << r << std::endl;
        return r;
      } else {
	//so create file
        *fhp = new Fh();
	if (*fhp == NULL) 
	  return -ENOMEM;

        const struct fuse_ctx *fctx = fuse_req_ctx(req);
        int uid = fctx->uid;
  	int gid = fctx->gid;
        
 	r = create(parent, name, mode, flags, ino, *fhp, uid, gid);
	if (r < 0) {
	  if (fhp && *fhp) {
	    delete *fhp;
	    *fhp == NULL;
	  }
	  return r;
        }
        fill_stat(attr, ino, *((*fhp)->inode));
      }
    }
    return 0;
  }

  int Lightfs::ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
  {
    return unlink(parent, name);
  }

  int Lightfs::ll_open(fuse_req_t req, fuse_ino_t ino, int flags, Fh **fhp)
  {
    int r = -1;
    cout << "ll_open: fhp == NULL:" << (fhp == NULL) << std::endl;
    if (fhp == NULL) {
      cout << "no file handler pointer..." << std::endl;
      return -1;
    }
    cout << "new fh" << std::endl;
    *fhp = new Fh();
    if (*fhp == NULL) {
      cout << "new file handler failed..." << std::endl;
      return -ENOMEM;
    }
    return open(ino, flags, *fhp);
  }

  int Lightfs::ll_release(fuse_req_t req, Fh *fh)
  {
    return release(fh);
  }

  int Lightfs::ll_read(fuse_req_t req, size_t size, off_t off, Fh *fh, bufferlist *bl)
  {
    return read(fh, off, size, bl); 
  }


  int Lightfs::ll_write(fuse_req_t req, off_t off, size_t size, const char *buf, Fh *fh)
  {
    return write(fh, off, size, buf);         
  }


  
  int Lightfs::ll_opendir(fuse_req_t req, fuse_ino_t ino, dir_buffer **d_buf)
  {
    if (d_buf == NULL)
      return -1;
    *d_buf = new dir_buffer();
    if (*d_buf == NULL)
      return -1;
    return opendir(ino, *d_buf);
  }

  int Lightfs::ll_readdir(fuse_req_t req, fuse_ino_t ino, off_t off, off_t size,
		off_t *f_size, char *buf, dir_buffer *d_buffer)
  {
    cout << "ll_readdir" << std::endl;

    int r = -1;
    inode_t inode;
    struct dirent ent;
    struct stat st;
    memset(&ent, 0, sizeof(ent));
    memset(&ent, 0, sizeof(st));

    //to fix: buffer update
    //get all dir entries: <name, ino>
    cout << "before readdir" << std::endl;
    if (d_buffer->buffer.size() == 0) {
      r = readdir(ino, d_buffer->buffer);
      if (r < 0)
	return r;
    }
    
    *f_size = 0;
    off_t fill_size = 0;
    off_t ent_size = 0;
    off_t remind_size = size;
    off_t next_off_size = 0;
    off_t next_off = off;
    off_t cur_off = off;
    off_t pos = off;
    off_t entry_count = d_buffer->buffer.size();
    char *ptr = buf;   
    const char *entry = NULL;

    inodeno_t ent_ino = 0;

    std::map<std::string, inodeno_t>::iterator p = d_buffer->buffer.begin();
    std::map<std::string, inodeno_t>::iterator end = d_buffer->buffer.end();
    cout << "ll_readdir: off = " << off << ", entry_count = " << entry_count << std::endl;
    //seek to off
    while(pos--)
      ++p;

    //traverse map<string, inodeno_t> at off
    for (; p != end; ++p) {
      entry = p->first.c_str();
      //get the dir entry size
      ent_size = fuse_add_direntry(req, NULL, 0, entry, NULL, 0);
      //cout << "ent_size = " << ent_size << std::endl;
      next_off_size += ent_size;
      if (next_off_size > remind_size)
        break;
      next_off++;
      if (next_off > entry_count)
        break;
      //printf("%s, remind_size = %ld, fill_size = %ld,  cur_off = %ld, next_off = %ld, ptr = %p\n", 
      //  entry, remind_size, fill_size, cur_off, next_off, ptr);
   
      ent_ino = p->second;
      cout << "<" << entry << ", " << hex << ent_ino << dec << ">" << std::endl;
      if (ent_ino == ROOT_PARENT) {
	inode.mode = S_IFDIR;	
      } else { 
        r = getattr(ent_ino, inode);
        //cout << "getattr = " << r << std::endl; 
        if (r < 0)
          return r; 
      }
      fill_stat(&st, ent_ino, inode);

      fuse_add_direntry(req, ptr, remind_size, entry, &st, next_off);
      fill_size += ent_size;
      remind_size -= ent_size;
      ptr += ent_size;
      cur_off = next_off;
    }
    
    *f_size = fill_size;
    return 0;
  }

  int Lightfs::ll_releasedir(fuse_req_t req, dir_buffer *d_buffer)
  {
    return releasedir(d_buffer);
  }
};
