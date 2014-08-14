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
    oid.clear();
    char oid_str[32] = {'\0'};
    if (ino == 0) { 
      oid = "inode.0";
      return;
    }

    snprintf(oid_str, sizeof(oid_str), "inode.%08lX", ino);
    oid = oid_str;
  }

  /* class Lightfs */
  bool Lightfs::create_root()
  {
    if (has_root)
      return true;
    int r = -1;
    inode_t inode;
    std::string oid("inode.0");

    r = cls_client::create_inode(_ioctx, oid, true, inode);
    if (r == 0 || r == -EEXIST) {
      has_root = true;
      return true;
    }
    return false;
  }

  int Lightfs::do_mkdir(inodeno_t pino, const char *name, inodeno_t myino)
  {
    inode_t inode;
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

  int Lightfs::mkdir(inodeno_t pino, const char *name)
  {
    if (!_ino_gen)
      return -EINVAL;
    inodeno_t ino = -1;
    _ino_gen->generate(ino);
    return do_mkdir(pino, name, ino);
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
    
    return cls_client::find_inode(_ioctx, poid, name, ino);
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

};

