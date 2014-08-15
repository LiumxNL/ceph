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
};

