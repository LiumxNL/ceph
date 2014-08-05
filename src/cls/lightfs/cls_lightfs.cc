#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#include "osd/ReplicatedPG.h"
#include "objclass/objclass.h"
#include "include/buffer.h"
#include "include/rados.h"
#include "include/encoding.h"
#include "include/utime.h"

#include "include/lightfs_types.hpp"

using namespace std;

namespace lightfs
{
  static int read_seq(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
  {
    CLS_LOG(20, "lightfs read_seq");
    int r;

    bufferlist bl;
    r = cls_cxx_map_read_header(hctx, &bl);
    if (r < 0)
      return r;

    uint64_t now;
    uint64_t max;
    try {
      bufferlist::iterator p = bl.begin();
      ::decode(now, p);
      ::decode(max, p);
    } catch (const buffer::error &err) {
      assert(false);
      return -ERANGE;
    }

    bufferlist res;
    ::encode(now, res);
    out->claim(res);

    return 0;
  }

  static int write_seq(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
  {
    CLS_LOG(20, "lightfs write_seq");
    int r;

    uint64_t hope;
    try {
      bufferlist::iterator p = in->begin();
      ::decode(hope, p);
    } catch (const buffer::error &err) {
      return -EINVAL;
    }

    bufferlist data;
    r = cls_cxx_map_read_header(hctx, &data);
    if (r < 0)
      return r;
  
    uint64_t now, max;
    try {
      bufferlist::iterator p = data.begin();
      ::decode(now, p);
      ::decode(max, p);
    } catch (const buffer::error &err) {
      assert(false);
      return -ERANGE;
    }

    if (hope != now)
      return -EAGAIN;

    if (now == max)
      return -ERANGE;

    ++now;
    bufferlist newdata;
    ::encode(now, newdata);
    ::encode(max, newdata);
    r = cls_cxx_map_write_header(hctx, &newdata);
    if (r < 0)
      return r;

    return 0;
  }

  static int create_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
  {
    CLS_LOG(20, "lightfs create_inode");

    int r;

    bool excl;
    inode_t inode;
    try {
      bufferlist::iterator p = in->begin();
      ::decode(excl, p);
      inode.decode(p);
    } catch (const buffer::error &err) {
      return -EINVAL;
    }

    r = cls_cxx_create(hctx, excl);
    if (r < 0)
      return r;

    bufferlist data;
    inode.encode(data);
    r = cls_cxx_map_write_header(hctx, &data);
    if (r < 0)
      return r;

    return 0;
  }

  static int get_subinode_count(cls_method_context_t hctx, uint64_t &result)
  {
    int r;

    bufferlist data;
    r = cls_cxx_map_get_val(hctx, "C", &data);
    if (r < 0) {
      if (r == -ENOENT)
        return 0;
      else
        return r;
    }

    try {
      bufferlist::iterator p = data.begin();
      ::decode(result, p);
    } catch (const buffer::error &err) {
      assert(0);
      return -EIO;
    }

    return 0;
  }

  static int set_subinode_count(cls_method_context_t hctx, uint64_t count)
  {
    int r;

    bufferlist data;
    ::encode(count, data);
    r = cls_cxx_map_set_val(hctx, "C", &data);
    if (r < 0)
      return r;

    return 0;
  }

  static int remove_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
  {
    CLS_LOG(20, "lightfs remove_inode");

    int r;

    uint64_t count;
    r = get_subinode_count(hctx, count);
    if (r < 0)
      return r;

    if (count)
      return -ENOTEMPTY;

    r = cls_cxx_remove(hctx);
    if (r < 0)
      return r;

    return 0;
  }

  static int get_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
  {
    CLS_LOG(20, "lightfs remove_inode");

    int r;

    int used_attr;
    try {
      bufferlist::iterator p = in->begin();
      ::decode(used_attr, p);
    } catch (const buffer::error &err) {
      return -EINVAL;
    }

    bufferlist data;
    r = cls_cxx_map_read_header(hctx, &data);
    if (r < 0)
      return r;

    inode_t inode;
    try {
      bufferlist::iterator p = data.begin();
      inode.decode(p);
    } catch (const buffer::error &err) {
      assert(0);
      return -EIO;
    }

    bufferlist res;
    inode.encode(used_attr, res);
    out->claim(res);

    return 0;
  }

  static int update_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
  {
    CLS_LOG(20, "lightfs remove_inode");

    int r;

    bufferlist data;
    r = cls_cxx_map_read_header(hctx, &data);
    if (r < 0)
      return r;

    inode_t inode;
    try {
      bufferlist::iterator p = data.begin();
      inode.decode(p);
    } catch (const buffer::error &err) {
      assert(0);
      return -EIO;
    }

    int used_attr;
    try {
      bufferlist::iterator p = in->begin();
      ::decode(used_attr, p);
      inode.decode(used_attr, p);
    } catch (const buffer::error &err) {
      return -EINVAL;
    }

    bufferlist newdata;
    inode.encode(newdata);
    r = cls_cxx_map_write_header(hctx, &newdata);
    if (r < 0)
      return r;

    return 0;
  }

  static inline void get_name_key(const string& name, string &result)
  {
    result = "N." + name;
  }

  static inline void get_inodeno_key(inodeno_t ino, string &result)
  {
    char buf[20];
    snprintf(buf, 20, "I.%08lX", ino);
    result = buf;
  }

  static int is_inode_exist(cls_method_context_t hctx, const string &name, inodeno_t hope_ino)
  {
    int r;

    string key;
    get_name_key(name, key);
    bufferlist data;
    r = cls_cxx_map_get_val(hctx, key, &data);
    if (r < 0)
      return r;

    inodeno_t ino;
    try {
      bufferlist::iterator p = data.begin();
      ::decode(ino, p);
    } catch (const buffer::error &err) {
      assert(0);
      return -EIO;
    }
    if (ino != hope_ino)
      return -EINVAL;

    return 0;
  }

  static int is_inode_exist(cls_method_context_t hctx, inodeno_t ino)
  {
    string key;
    get_inodeno_key(ino, key);

    bufferlist data;
    return cls_cxx_map_get_val(hctx, key, &data);
  }

  static int is_inode_not_exist(cls_method_context_t hctx, const string &name, inodeno_t hope_ino)
  {
    int r;

    r = is_inode_exist(hctx, name, hope_ino);
    if (r == -ENOENT)
      return 0;
    else if (r == 0)
      return -EEXIST;
    else
      return r;
  }

  static int do_link(cls_method_context_t hctx, const string& name, inodeno_t ino)
  {
    bufferlist namedata;
    ::encode(name, namedata);

    bufferlist inodata;
    ::encode(ino, inodata);

    string key;
    map<string, bufferlist> mapdata;

    get_name_key(name, key);
    mapdata[key] = inodata;

    get_inodeno_key(ino, key);
    mapdata[key] = namedata;
    
    return cls_cxx_map_set_vals(hctx, &mapdata); 
  }

  static int do_unlink(cls_method_context_t hctx, const string& name, inodeno_t ino)
  {
    set<string> setdata;
    string key;

    get_name_key(name, key);
    setdata.insert(key);

    get_inodeno_key(ino, key);
    setdata.insert(key);
    
    return cls_cxx_map_remove_keys(hctx, &setdata);
  }

  static int link_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
  {
    CLS_LOG(20, "lightfs link_inode");

    int r;

    string name;
    inodeno_t ino;
    try {
      bufferlist::iterator p = in->begin();
      ::decode(name, p);
      ::decode(ino, p);
    } catch (const buffer::error &err) {
      return -EINVAL;
    }

    r = is_inode_not_exist(hctx, name, ino);
    if (r < 0)
      return r;

    r = do_link(hctx, name, ino);
    if (r < 0)
      return r;

    uint64_t count;
    r = get_subinode_count(hctx, count);
    if (r < 0)
      return r;

    ++count;
    r = set_subinode_count(hctx, count);
    if (r < 0)
      return r;

    return 0;
  }

  static int unlink_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
  {
    CLS_LOG(20, "lightfs unlink_inode");

    int r;

    string name;
    inodeno_t ino;
    try {
      bufferlist::iterator p = in->begin();
      ::decode(name, p);
      ::decode(ino, p);
    } catch (const buffer::error &err) {
      return -EINVAL;
    }

    r = is_inode_exist(hctx, name, ino);
    if (r < 0)
      return r;

    r = do_unlink(hctx, name, ino);
    if (r < 0)
      return 0;

    uint64_t count;
    r = get_subinode_count(hctx, count);
    if (r < 0)
      return r;

    --count;
    r = set_subinode_count(hctx, count);
    if (r < 0)
      return r;

    return 0;
  }

  static int rename_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
  {
    CLS_LOG(20, "lightfs rename_inode");

    int r;

    string oldname;
    string newname;
    inodeno_t ino;
    try {
      bufferlist::iterator p = in->begin();
      ::decode(oldname, p);
      ::decode(newname, p);
      ::decode(ino, p);
    } catch (const buffer::error &err) {
      return -EINVAL;
    }

    r = is_inode_exist(hctx, oldname, ino);
    if (r < 0)
      return r;

    r = is_inode_not_exist(hctx, newname, ino);
    if (r < 0)
      return r;

    r = do_unlink(hctx, oldname, ino);
    if (r < 0)
      return r;

    r = do_link(hctx, newname, ino);
    if (r < 0)
      return r;

    return 0;
  }
}

CLS_VER(2,0);
CLS_NAME(lightfs);

cls_handle_t h_class;

cls_method_handle_t h_read_seq;
cls_method_handle_t h_write_seq;

cls_method_handle_t h_create_inode;
cls_method_handle_t h_remove_inode;
cls_method_handle_t h_get_inode;
cls_method_handle_t h_update_inode;
cls_method_handle_t h_link_inode;
cls_method_handle_t h_unlink_inode;
cls_method_handle_t h_rename_inode;

void __cls_init()
{
  CLS_LOG(20, "loading cls_lightfs!");
  
  cls_register("lightfs", &h_class);

  cls_register_cxx_method(h_class, "read_seq", 
	CLS_METHOD_RD, lightfs::read_seq, &h_read_seq);
  cls_register_cxx_method(h_class, "write_seq", 
	CLS_METHOD_RD | CLS_METHOD_WR, lightfs::write_seq, &h_write_seq);

  cls_register_cxx_method(h_class, "create_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, lightfs::create_inode, &h_create_inode);
  cls_register_cxx_method(h_class, "remove_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, lightfs::remove_inode, &h_remove_inode);

  cls_register_cxx_method(h_class, "get_inode",
	CLS_METHOD_RD, lightfs::get_inode, &h_get_inode);
  cls_register_cxx_method(h_class, "update_inode",
	CLS_METHOD_RD | CLS_METHOD_WR, lightfs::update_inode, &h_update_inode);

  cls_register_cxx_method(h_class, "link_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, lightfs::link_inode, &h_link_inode);
  cls_register_cxx_method(h_class, "unlink_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, lightfs::unlink_inode, &h_unlink_inode);
  cls_register_cxx_method(h_class, "rename_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, lightfs::rename_inode, &h_rename_inode);

  return;
}
