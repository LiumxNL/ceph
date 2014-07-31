
/*
  tools for implement lightfs in transaction
  mkdir
  rm
  rename
  list
  lookup
  read
  write
*/
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#include "osd/ReplicatedPG.h"
#include "objclass/objclass.h"
#include "include/types.h"
#include "include/buffer.h"
#include "include/rados.h"
#include "include/encoding.h"
#include "include/utime.h"

#include "include/lightfs_types.hpp"

#define INO_BITS ((INO_CHAR_LEN) + 1) // llx 16 chars + 1 char '\0'
#define INODE_STR_LEN 6//strlen(".inode") == 6

CLS_VER(2,0);
CLS_NAME(lightfs);

cls_handle_t h_class;

cls_method_handle_t h_genino;
cls_method_handle_t h_read_seq_omap;
cls_method_handle_t h_write_seq_omap;

cls_method_handle_t h_create_lightfs;
cls_method_handle_t h_stat_inode;

cls_method_handle_t h_lightfs_mkdir;
cls_method_handle_t h_create_inode;
cls_method_handle_t h_remove_inode;
cls_method_handle_t h_add_entry;
cls_method_handle_t h_remove_entry;

cls_method_handle_t h_lightfs_notify;

typedef long long unsigned inode_t;


int genino(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = -1;
  bufferlist bl;
  r = cls_cxx_read(hctx, 0, GEN_CONTENT_LEN, &bl);
  if (r < 0) {
    CLS_ERR("error read ino!!!");
    return r;
  }  
  bl.copy(0, GEN_UNIT, *out);
  CLS_LOG(10, "out.length = %d", out->length());
  uint64_t num = 0, max = 0;
  bufferlist::iterator p = bl.begin();
  assert(!p.end());
  ::decode(num, p); 
  ::decode(max, p);
  CLS_LOG(10, "gen ino = %lx, max = %lx", num, max);
  num++;
  if (num > max) {
    CLS_LOG(1, "genino: ino is out of range!!!");
    return -ERANGE;
  }
  bufferlist new_bl;
  ::encode(num, new_bl);
  //r = cls_cxx_write(hctx, 0, GEN_UNIT, &new_bl); 
  CLS_LOG(10, "gen write r = %d", r);
  if (r < 0) {
    CLS_LOG(10, "error write ino+1!!!");
    return r;
  }
  return 0;
}

int read_seq_omap(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "read_seq_omap");
  int r = -1;
  bufferlist bl;
  r = cls_cxx_map_read_header(hctx, out);
  if (r < 0) {
    CLS_ERR("error omap read seq !!!");
    return r;
  }  
  return 0;
}

int write_seq_omap(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "write_seq_omap");
  int r = -1;
  uint64_t next = 0;
  bufferlist::iterator p = in->begin();
  assert(!p.end());
  ::decode(next, p); 
  CLS_LOG(10, "gen next seq = %lx", next);
  

  bufferlist mybl;
  r = cls_cxx_map_read_header(hctx, &mybl);
  if (r < 0) {
    CLS_ERR("error omap read seq !!!");
    return r;
  }  
  
  uint64_t mynum = 0, mymax = 0;
  bufferlist::iterator myp = mybl.begin();
  assert(!myp.end());
  ::decode(mynum, myp);
  ::decode(mymax, myp);
  
  if (next > mymax) {
    CLS_LOG(1, "next seq is out of range!!!");
    return -ERANGE;
  }
  if (next <= mynum) {
    CLS_LOG(1, "next=%lx <= %lx : next seq is old!!!", next, mynum);
    return -EAGAIN;
  }

  bufferlist new_bl;
  ::encode(next, new_bl);
  ::encode(mymax, new_bl);
  //r = cls_cxx_write(hctx, 0, GEN_UNIT, &new_bl); 
  r = cls_cxx_map_write_header(hctx, &new_bl);
  CLS_LOG(10, "write next seq r = %d", r);
  if (r < 0) {
    CLS_LOG(10, "error omap write next seq!!!");
    return r;
  }
  return 0;
}

int genino_omap(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = -1;
  bufferlist bl;
  r = cls_cxx_map_read_header(hctx, out);
  if (r < 0) {
    CLS_ERR("error omap read ino !!!");
    return r;
  }  

  /* saddly ,outbl is igored when returned by pg, so we can't get ino*/
  //bl.copy(0, GEN_UNIT, *out);
  out->copy(0, GEN_CONTENT_LEN, *in);
  CLS_LOG(10, "out.length = %d", out->length());
  uint64_t num = 0, max = 0;
  bufferlist::iterator p = out->begin();
  assert(!p.end());
  ::decode(num, p); 
  ::decode(max, p);
  CLS_LOG(10, "gen ino = %lx, max = %lx", num, max);
  num++;
  if (num > max) {
    CLS_LOG(1, "genino: ino is out of range!!!");
    return -ERANGE;
  }
  bufferlist new_bl;
  ::encode(num, new_bl);
  ::encode(max, new_bl);
  //r = cls_cxx_write(hctx, 0, GEN_UNIT, &new_bl); 
  r = cls_cxx_map_write_header(hctx, &new_bl);
  CLS_LOG(10, "gen write r = %d", r);
  if (r < 0) {
    CLS_LOG(10, "error omap write ino+1!!!");
    return r;
  }
  return 0;
}

//create inode object
int create_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "lightfs create_inode");
  int r = -1;
  if ( 0 == cls_cxx_stat(hctx, NULL, NULL) ) {
    CLS_LOG(10, "inode already exisits");
    return 0;
  }

  if (in == NULL)
    return -EINVAL;

  _inodeno_t ino = 0;
  mode_t mode = 0;
  
  bufferlist::iterator p = in->begin();
  ::decode(ino, p);
  ::decode(mode, p);
  
  if (ino == 0 || (mode != S_IFLNK && mode != S_IFDIR && mode != S_IFREG))
    return -EINVAL;
  
  lightfs_inode_t inode(ino, mode);
  bufferlist bl;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  inode.ctime.set_from_timeval(&tv);
  inode.encode_inode(bl);
  r = cls_cxx_map_write_header(hctx, &bl);
  if (r < 0) 
    CLS_LOG(10, "create inode failed");
  return 0;
}

int create_lightfs(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "lightfs create_lightfs");
  int r = -1;
  if (0 == cls_cxx_stat(hctx, NULL, NULL)) 
    return 0;
  //r = cls_cxx_create(hctx, true); 
  lightfs_inode_t inode(ROOT_INO, S_IFDIR);
  bufferlist bl;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  inode.ctime.set_from_timeval(&tv);
  inode.encode_inode(bl);
  r = cls_cxx_map_write_header(hctx, &bl);
  if (r < 0) 
    CLS_LOG(10, "create lightfs failed");
  return r;
}


int stat_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "lightfs stat_inode");
  int r = -1;
  r = cls_cxx_stat(hctx, NULL, NULL);
  if (r < 0)
    return r;
  r = cls_cxx_map_read_header(hctx, out);
  if (r < 0)
    CLS_LOG(10, "stat inode failed");
  return r;
}

int remove_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "lightfs remove_inode");
  if ( 0 > cls_cxx_stat(hctx, NULL, NULL) ) {
    CLS_LOG(10, "object does not exists");
    return -ENOENT;
  }
  
  int r = cls_cxx_remove(hctx);
  if (r < 0) {
    CLS_LOG(10, "remove inode failed , r = %d", r);
    return r;
  }
  return 0;
}

static inode_t get_last_ino()
{
  return 100000000 + (random() % 255);
}

static inode_t get_next_ino()
{
  inode_t last_ino = get_last_ino();
  return ++last_ino;
}

static int get_val(cls_method_context_t hctx, const string &key, string &out)
{
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading omap key %s: %d", key.c_str(), r);
    }
    return r;
  }

  try {
    out.clear();
    bl.copy(0, bl.length(), out);
  } catch (const buffer::error &err) {
    CLS_ERR("error decoding %s", key.c_str());
    return -EIO;
  }

  return 0;
}

int lightfs_mkdir(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string dir_name;
  string dir_inode;
  
  CLS_LOG(10,"func: lightfs_mkdir");
///*
  /*
    1. parent ino object not exists
    hctx ---> parent ino object
    return -ENOENT
  */
  if(cls_cxx_stat(hctx, NULL, NULL) < 0) {
    CLS_LOG(10, "parent object not exists");
    return -ENOENT;
  }

  CLS_LOG(10, "parent object exists");
  try {
    //bufferlist::iterator iter = in->begin();
    CLS_LOG(10, "in.len = %d , in=%s", in->length(), in->c_str());
    in->copy(0, in->length(), dir_name);
    //::decode(dir_name, iter);
    CLS_LOG(10, "dir_name = %s", dir_name.c_str());
  } catch (const buffer::error &err) {
    CLS_LOG(10,"ERR= %s", err.what());
    return -EINVAL;
  }

  int r = get_val(hctx, dir_name, dir_inode);
  CLS_LOG(10, "get dir inode = %s", dir_inode.c_str());
  /*
    2. dir_name not exists in parent ino object
  */
  if (r < 0) {
    CLS_LOG(10, "dir_name '%s' not exists, so create it, r = %d", dir_name.c_str(), r);
    long long unsigned ino = get_next_ino();
    char inode_hex[INO_BITS] = {'\0'};
    char inode_dec[INO_BITS] = {'\0'};
    int len_hex = snprintf(inode_hex, sizeof(inode_hex), "%llx", ino);
    int len_dec = snprintf(inode_dec, sizeof(inode_dec), "%llu", ino);
    assert(len_hex < INO_BITS);
    assert(len_dec < INO_BITS);
    CLS_LOG(10, "inode_hex = %s , inode_dec = %s, strlen(inode_hex) = %lu", inode_hex, inode_dec, strlen(inode_hex));
    bufferlist bl;
    bl.append(inode_hex, len_hex);
    //set ino of dir 
    cls_cxx_map_set_val(hctx, dir_name, &bl);
    out->clear();
    out->append(inode_hex, len_hex);
    CLS_LOG(10, "out.len = %d , out = %s", out->length(), out->c_str());
  } else {
  /*
    3. dir_name exists in parent ino object
  */
    CLS_LOG(10, "dir_name exists, so just return");
    out->clear();
//    out->append('\0');
    CLS_LOG(10, "out.len = %d , out = %s", out->length(), out->c_str());
  }
//*/
  return 0; 
}


int add_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string name;
  string inode;
  _inodeno_t ino = 0;
  
  CLS_LOG(10,"lightfs add_entry");
///*
  /*
    1. parent ino object not exists
    hctx ---> parent ino object
    return -ENOENT
  */
  if(cls_cxx_stat(hctx, NULL, NULL) < 0) {
    CLS_LOG(10, "parent object not exists");
    return -ENOENT;
  }

  CLS_LOG(10, "parent object exists");
  bufferlist::iterator p = in->begin();
  assert(!p.end());
  ::decode(name, p);
  ::decode(ino, p);

  assert(ino != 0);


  int r = get_val(hctx, name, inode);
  CLS_LOG(10, "get dir inode = %s", inode.c_str());
  /*
    2. dir_name not exists in parent ino object
  */
  if (r < 0) {
    char inode_hex[INO_BITS] = {'\0'};
    char format[32];
    snprintf(format, sizeof(format), "%%0%dllx", INO_CHAR_LEN);
    CLS_LOG(10, "format : %s", format);
    int len_hex = snprintf(inode_hex, sizeof(inode_hex), format, ino);
    assert(len_hex < INO_BITS);
    CLS_LOG(10, "inode_hex = %s , strlen(inode_hex) = %lu, len_hex = %d", inode_hex, strlen(inode_hex), len_hex);
    bufferlist bl;
    bl.append(inode_hex, len_hex);
    //set ino of dir 
    cls_cxx_map_set_val(hctx, name, &bl);
  } else {
  /*
    3. dir_name exists in parent ino object
  */
    CLS_LOG(10, "dir_name exists, so just return");
    return -EEXIST;
  }
  return 0; 
  
}

int remove_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "lightfs remove_entry");
  int r = -1;
  string name;
  string inode; 
  bufferlist::iterator p = in->begin();
  ::decode(name, p);
  r = get_val(hctx, name, inode);
  if (r < 0)
    return 0;

  r = cls_cxx_map_remove_key(hctx, name);
  return r;
}

int do_notify(cls_method_context_t hctx, bufferlist *inbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  OSDOp& op = ops[0];

  uint64_t cookie = 20;
  uint32_t ver = 11;
  uint8_t flag = 1;//watch the notify?
  op.op.op = CEPH_OSD_OP_NOTIFY;
  op.op.watch.cookie = cookie;
  op.op.watch.ver = ver;
  op.op.watch.flag = flag;
  //op.indata.append(*inbl);
  op.indata = *inbl;

  CLS_LOG(10, "do_notify: --> do_osd_ops ...");
  int r = -1;
  r = (*pctx)->pg->do_osd_ops(*pctx, ops);
  CLS_LOG(10, "do_notify: -> r = %d", r);
  if (r < 0) {
    CLS_LOG(10, "do_osd_ops = %d, failed", r);
    return r;
  }
  return 0;
}

int lightfs_notify(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "lightfs_notify");
///*
  if (in) {
    uint32_t ver;
    uint32_t timeout;
    bufferlist bl;
    bufferlist::iterator bp = in->begin();
    CLS_LOG(10, "in->length = %d", in->length());
    try {
      ::decode(ver, bp);
      ::decode(timeout, bp);
      ::decode(bl, bp);
    } catch (const buffer::error &e) {
      timeout = 0;
    } 
    CLS_LOG(10, "ver = %u, timeout = %u, buf = %s", ver, timeout, bl.c_str());
  }
//*/
  int r = -1;
  r = do_notify(hctx, in);
  CLS_LOG(10, "lightfs_notify: call cls_notify = %d ", r);
  return r;  
}

void __cls_init()
{
  CLS_LOG(0, "loading cls_lightfs!");
  
  cls_register("lightfs", &h_class);
  cls_register_cxx_method(h_class, "genino", 
	CLS_METHOD_RD | CLS_METHOD_WR, genino, &h_genino);
  cls_register_cxx_method(h_class, "read_seq_omap", 
	CLS_METHOD_RD, read_seq_omap, &h_read_seq_omap);
  cls_register_cxx_method(h_class, "write_seq_omap", 
	CLS_METHOD_RD | CLS_METHOD_WR, write_seq_omap, &h_write_seq_omap);

  cls_register_cxx_method(h_class, "create_lightfs", 
	CLS_METHOD_RD | CLS_METHOD_WR, create_lightfs, &h_create_lightfs);
  cls_register_cxx_method(h_class, "create_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, create_inode, &h_create_inode);
  cls_register_cxx_method(h_class, "stat_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, stat_inode, &h_stat_inode);
  cls_register_cxx_method(h_class, "remove_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, remove_inode, &h_remove_inode);

  cls_register_cxx_method(h_class, "add_entry", 
	CLS_METHOD_RD | CLS_METHOD_WR, add_entry, &h_add_entry);
  cls_register_cxx_method(h_class, "remove_entry", 
	CLS_METHOD_RD | CLS_METHOD_WR, remove_entry, &h_remove_entry);
/*
  cls_register_cxx_method(h_class, "lightfs_mkdir", 
	CLS_METHOD_RD | CLS_METHOD_WR, lightfs_mkdir, &h_lightfs_mkdir);
  cls_register_cxx_method(h_class, "create_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, create_inode, &h_create_inode);
  cls_register_cxx_method(h_class, "remove_inode", 
	CLS_METHOD_RD | CLS_METHOD_WR, remove_inode, &h_remove_inode);
  cls_register_cxx_method(h_class, "add_entry", 
	CLS_METHOD_RD | CLS_METHOD_WR, add_entry, &h_add_entry);
  cls_register_cxx_method(h_class, "lightfs_notify", 
	CLS_METHOD_RD | CLS_METHOD_WR, lightfs_notify, &h_lightfs_notify);
*/
  return;
}
