
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

#define INO_BITS 65 // llx 64bits + 1 bit '\0'
#define INODE_STR_LEN 6//strlen(".inode") == 6

CLS_VER(2,0);
CLS_NAME(lightfs);

cls_handle_t h_class;
cls_method_handle_t h_lightfs_mkdir;
cls_method_handle_t h_create_inode;
cls_method_handle_t h_remove_inode;
cls_method_handle_t h_add_entry;
cls_method_handle_t h_lightfs_notify;

typedef long long unsigned inode_t;

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

//create inode object
int create_inode(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "lightfs create_inode");
  if ( 0 == cls_cxx_stat(hctx, NULL, NULL) ) {
    CLS_LOG(10, "object already exisits");
    return -EEXIST;
  }
  
  int r = cls_cxx_create(hctx, CEPH_OSD_OP_FLAG_EXCL);
  if (r < 0) {
    CLS_LOG(10, "create object failed , r = %d", r);
    return r;
  }

  return 0;
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
    CLS_LOG(10, "remove object failed , r = %d", r);
    return r;
  }
  return 0;
}

int add_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string dir_name;
  string dir_inode;
  
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
    CLS_LOG(10, "inode_hex = %s , inode_dec = %s, strlen(inode_hex) = %lu, len_hex = %d", inode_hex, inode_dec, strlen(inode_hex), len_hex);
    bufferlist bl;
    bl.append(inode_hex, len_hex + 1);
    //set ino of dir 
    cls_cxx_map_set_val(hctx, dir_name, &bl);
  } else {
  /*
    3. dir_name exists in parent ino object
  */
    CLS_LOG(10, "dir_name exists, so just return");
//    out->append('\0');
  }
//*/
  return 0; 
  
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

  return;
}
