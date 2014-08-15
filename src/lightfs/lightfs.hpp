#ifndef __LIGHTFS_HEADER__
#define __LIGHTFS_HEADER__

#include <string>
#include <map>

#include "common/Mutex.h"

#include "include/xlist.h"

#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"

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
};

#endif

