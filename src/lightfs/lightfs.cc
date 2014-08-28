#include <stdlib.h>
#include <string.h>
#include <string>

#include "lightfs.hpp"
#include "cls/lightfs/cls_lightfs_client.h"

using namespace librados;

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

      r = cls_client::create_seq(&_ioctx, oid, i ? 0 : 1);
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
    assert(this == _root || _ref == 0);
    assert(_parent == NULL);

    _root->_cache.erase(_ino);
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
    if (ino == 0) { 
      oid = "inode.0";
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

  /* class Lightfs */

  /* helper */
  void Lightfs::file_to_objects(off_t file_off, off_t file_len, std::map<off_t, std::pair<off_t, off_t> > &objects)
  {
    off_t first = file_off;
    off_t last = file_off + file_len;
    
    // first object init
    off_t first_index = first >> order;
    off_t first_off = first & ((1 << order) - 1);
    off_t first_len = 0;
    //last object init
    off_t last_index = last >> order;
    off_t last_off = last & ((1 << order) - 1);
    off_t last_len = 0;

    //first object
    if (first_index == last_index) {
      first_len = last_off - first_off;  
      if (first_len == 0)
        return;
      objects[first_index] = std::make_pair(first_off, first_len);
      return;
    }

    first_len = (1 << order) - first_off;
    objects[first_index] = std::make_pair(first_off, first_len);
    
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
    last_len = last_off - off;
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
  
  void Lightfs::fill_stat(struct stat *st, inodeno_t ino, inode_t inode)
  {
    st->st_ino = ino;
    st->st_dev = 0; //no device
    st->st_mode = inode.mode;
    st->st_rdev = 0; //no rdevice
    st->st_nlink = 1;  // we don't support hard link currently
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

  /*lightfs member*/
  bool Lightfs::create_root()
  {
    if (has_root)
      return true;
    int r = -1;
    inode_t inode;
    std::string oid("inode.0");

    r = cls_client::create_inode(_ioctx, oid, true, inode);
    if (r < 0 && r != -EEXIST) {
      return false;
    }
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

    // 1. generate ino and create child inode 
    r = cls_client::create_inode(_ioctx, oid, true, inode);
    if (r < 0)
        return r;
    
    // 2. add <N.name, myino> & <I.myino, name> entry to parent  
    r = cls_client::link_inode(_ioctx, poid, name, myino);
    // 3. if 2 failed, remove child inode
    if (r < 0) {
      r = cls_client::remove_inode(_ioctx, oid);
      if (r < 0)
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

    //cout << "<" << name << ", " << hex << ino << dec << ">" << endl;
    r = do_mkdir(pino, name, ino, inode);
    //cout << "do_mkdir = " << r << endl;
    if (r < 0)
      return r;
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

  int Lightfs::lookup(inodeno_t pino, const char *name, inodeno_t &ino)
  {
    string poid;
    get_inode_oid(pino, poid);
    
    int r = -1;
    r = cls_client::find_inode(_ioctx, poid, name, ino);
    //cout << "lookup : r = " << r << endl;
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

  int Lightfs::getattr(inodeno_t myino, inode_t &inode)
  {
    string oid;
    get_inode_oid(myino, oid);    

    return cls_client::get_inode(_ioctx, oid, inode);
  }

  /* file ops */

  int Lightfs::open(inodeno_t myino, int flags, Fh *fh)
  { 
    //if has cache, read from cache?
 
    //else read from osd
    if (!fh)
      return -1;
    string oid;
    get_inode_oid(myino, oid);
    inode_t inode;  
      
    int r = -1;
    r = cls_client::get_inode(_ioctx, oid, inode);
    if (r < 0)
      return r;
    
    fh->ino = myino;
    *(fh->inode) = inode;
    fh->pos = 0;
    fh->mode = inode.mode;
    fh->flags = flags;
 
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
    }
    return 0;
  }

  int Lightfs::read(Fh *fh, off_t off, off_t len, bufferlist *bl)
  {
    return 0;
  }

  int Lightfs::write(Fh *fh, off_t off, off_t len, const char *data)
  {
    int r = -1;
    lseek(fh, off, SEEK_SET);
    
    std::map<off_t, std::pair<off_t, off_t> > objects;
    file_to_objects(off, len, objects);

    off_t f_off = 0;
    off_t f_len = 0;
    off_t o_off = 0;
    off_t o_len = 0;
    //sync io

    //async io
    std::map<off_t, std::pair<off_t, off_t> >::iterator p = objects.begin();
    for (p = objects.begin(); p != objects.end(); ++p) {
      //cout << "<" << p->first << ", <" << p->second.first << "," << p->second.second << "> >" << endl;
      bufferlist inbl;
      o_off = p->second.first;
      o_len = p->second.second;
      f_len = o_len;
      
      inbl.append(data+f_off, f_len);
      string myoid;
      get_file_oid(fh->ino, p->first, myoid);
      //cout << myoid << endl;
      //cout << "f_off = " << f_off << ", f_len = " << f_len << endl;
      //AioCompletion *comp = librados::Rados::aio_create_completion();
      //r = _ioctx->aio_write(myoid, comp, inbl, o_len, o_off);    
      //librados::ObjectWriteOperation wr_op;
      //wr_op.write
      r = _ioctx->write(myoid, inbl, o_len, o_off);
      //cout << "r = " << r << endl;
      f_off += f_len;
    }
    return 0;
  }

  /*fuse lowlevel ops*/

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

    inode.mode = mode | S_IFDIR;
    inode.uid = fctx->uid;
    inode.gid = fctx->gid;    
    
    r = mkdir(parent, name, &ino, inode); 
    if (r < 0)
      return r;
    
    fill_stat(attr, ino, inode);
    return 0;
  }

  int Lightfs::ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name,
                struct stat *attr)
  {
    int r = -1;
    inodeno_t ino = -1;

    r = lookup(parent, name, ino);
    if (r < 0)
      return r;
    
    inode_t inode;
    r = getattr(ino, inode);
    if (r < 0)
      return r;
    
    fill_stat(attr, ino, inode);
    return 0;
  }


};

