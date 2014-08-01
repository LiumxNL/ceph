
/*
  lightfs file system
*/

#include "lightfs.hpp"
#include "cls/lightfs/cls_lightfs_client.h"
#include <assert.h>
#include <stdlib.h>
#include <time.h>

#define MAX_RETURN 65535

namespace lightfs {
  
  /* LightfsCtx member functions*/
  
  int LightfsCtx::ctx_init() 
  {
    int r = -1;
    /*
      1. create cluster
    */
    r = rados.init(userid);
    if ( r < 0) {
      cout << "rados create error , r = " << r << endl;
      return r;
    }
  
    /*
      2. read conf file to cluster
    */
    r = rados.conf_read_file(conf);
    if ( r < 0 ) {
      cout << "conf read error , r = " << r << endl;
      return r;
    }
  
    /*
      3. connect to cluster
    */
    r = rados.connect();
    if ( r < 0 ) {
      cout << "rados connect error , r = " << r << endl;
      return r;
    }
  
    /*
      4. lookup or create pool in cluster
    */
    r = rados.pool_create(pool);
    if ( r < 0 ) {
      cout << "pool create  error , r = " << r << endl;
      return r;
    }
    /*
      5. in cluster: creat ioctx , then do read ,write ......
    */
    r = rados.ioctx_create(pool, ioctx);
    if ( r < 0 ) {
      cout << "ioctx create  error , r = " << r << endl;
      return r;
    }
    return 0;    
  }
  
  void LightfsCtx::ctx_destroy()
  {
    /*
      7. shutdown cluster
    */
    ioctx.close();
    rados.shutdown();
  }
  
  
  /* Ligthfs member functions */
  
  int Lightfs::create_lightfs()
  {
    int r = -1;
    bufferlist inbl;
    bufferlist outbl;
    //inbl.append(root_obj.id);
    InoGen gen(lctx);
    gen.init_gen_omap();
  
    oid_t myobj(LIGHTFS_OBJ_INO, root.ino);
    string myoid(myobj.id);
  
    string gen_root(GEN_PREFIX".0");
    cout << "gen_root: " << gen_root.c_str() << endl;
    r = gen.do_generate_ino_omap(gen_root, true);
    if (r < 0) {
      cout << "do generate ino map on root_inode  failed, r = " << r << endl;
      return r;
    }
  
    r = lctx->ioctx.exec(myoid, "lightfs", "create_lightfs", inbl, outbl);   
    if (r < 0) {
      cout << "create lightfs exec failed, r = " << r << endl;
      return r;
    }
    return 0;
  }
  
  
  int Lightfs::stat_inode(lightfs_inode_t *inode)
  {
    
    bufferlist inbl;
    bufferlist outbl;
    oid_t myobj(LIGHTFS_OBJ_INO, inode->ino);
    string myoid(myobj.id);
    int r = -1;
    r = lctx->ioctx.exec(myoid, "lightfs", "stat_inode", inbl, outbl);   
    if (r < 0) {
      cout << "stat inode exec failed, r = " << r << endl;
      return r;
    }
    bufferlist::iterator p = outbl.begin();
    inode->decode_inode(p);
    return 0;
  }
  
  /* lightfs inode ops */
  
  void lightfs_print_inode(lightfs_inode_t &inode, char *name)
  {
      cout << name <<": [" << dec << inode.ino << ", " << inode.ctime << ", " << oct << inode.mode << "]" << dec << endl;
  }
  
  
  
  int Lightfs::mkdir(_inodeno_t pino, const char *name, mode_t mode)
  {
    //todo: log mkdir 
    
    cout << "Lightfs::mkdir" << endl;
    int r = -1;
    InoGen gen(lctx);
    gen.init_gen_omap();
    _inodeno_t ino = gen.generate_ino();
    cout <<"ino = " << hex << ino << dec << endl;
    
    oid_t ioid(LIGHTFS_OBJ_INO, ino);
    cout << "i: " << ioid.id << endl; 
   
    oid_t poid(LIGHTFS_OBJ_INO, pino);
    cout << "parent: " << poid.id << endl;
  
    string ioid_obj(ioid.id);
    string poid_obj(poid.id);
    string name_str(name);
    //1. create an inode
/*
    bufferlist inbl;
    bufferlist outbl;
    ::encode(ino, inbl);
    ::encode(mode, inbl);
    r = lctx->ioctx.exec(ioid_obj, "lightfs", "create_inode", inbl, outbl);
    cout << "lightfs create_inode r = " << r << endl;
*/
    lightfs::cls_client::create_inode(lctx, ioid_obj, ino, mode)
  
    //2. add inode to parent childlist
/*
    bufferlist new_inbl;
    bufferlist new_outbl;
    ::encode(name, new_inbl);
    ::encode(ino, new_inbl);
    r = lctx->ioctx.exec(poid_obj, "lightfs", "link_inode", new_inbl, new_outbl);
    cout << "lightfs exec add_entry r = " << r << endl;
*/
    lightfs::cls_client::link_inode(lctx, poid_obj, name_str, ino);
  
    //3. delete inode if it already existed in parent childlist
    if (r == -EEXIST) {
/*
      bufferlist in;
      bufferlist out;
      r = lctx->ioctx.exec(ioid_obj, "lightfs", "remove_inode", in, out);
      if (r < 0 && r != -ENOENT) {
        cout << "mkdir remove inode failed , r = " << r << endl;
        return r;
      }
*/
      lightfs::cls_client::remove_inode(lctx, ioid_obj);
    }
    
    // 4. write to cache ...
  
    return 0;
  }
  
  int Lightfs::readdir(_inodeno_t myino, size_t size, std::<std::string, bufferlist> &out_vals)
  {
    int r = -1;
    int rval = -1;
    librados::ObjectReadOperation rd_op;
  
    // readdir from client cache ?
  
  
  
    // readdir from pool
    string start_after("");
    string filter_prefix("N.");
    uint64_t max_return = size;
    //std::map<std::string, bufferlist> out_vals;
    rd_op.omap_get_vals(start_after, filter_prefix, max_return, &out_vals, &rval);
  
    oid_t toid(LIGHTFS_OBJ_INO, myino);
    string myoid(toid.id);
    bufferlist bl;
    r = lctx->ioctx.operate(myoid, &rd_op, &bl);
    if (r < 0) {
      cout << "lightfs readdir operate omap_get_vals error, r = " << r << endl;
      return r;
    }
     
    // print all subs
    std::map<std::string, bufferlist>::iterator p = out_vals.begin();
    for (; p != out_vals.end(); ++p) {
      bufferlist blist;
      blist.append(p->second);
      blist.append('\0');
      cout << "<" <<p->first << ", " << blist.c_str() << ">" << endl;
    } 
    return 0; 
    
  }
  
  int Lightfs::lookup(_inodeno_t pino, const char *name, _inodeno_t *ino)
  {
    cout << "lightfs lookup" << endl;
    int r = -1;
    int rval = -1;
    librados::ObjectReadOperation rd_op;
    std::set<std::string> key;
    std::map<std::string, bufferlist> out_vals;
    
    std::string myname("N."); 
    myname.append(name);
    key.insert(myname); 
    const std::set<std::string> keys(key);
  
    rd_op.omap_get_vals_by_keys(keys, &out_vals, &rval);
  
    oid_t toid(LIGHTFS_OBJ_INO, pino);
    string myoid(toid.id);
    bufferlist bl;
    
    r = lctx->ioctx.operate(myoid, &rd_op, &bl);
    
    if (r < 0) {
      cout << "lightfs lookup operate omap_get_keys_by_vals error, r = " << r << endl;
      return r;
    }
    
    //show result
    std::map<std::string, bufferlist>::iterator p = out_vals.begin();
    bufferlist blist;
    blist.append(p->second);
    blist.append('\0');
    *ino = strtoul(bl.c_str(), NULL, 16);
    cout << "<" <<p->first << ", " << hex << ino  << dec << ">" << endl;
  
    // todo : fill target dirent and dentry cache
    return 0;
  }
  
  
  int Lightfs::do_rmdir(_inodeno_t myino)
  {
    //cout << "lightfs do_rmdir" << endl;
    int r = -1;
    int rval = -1;
  
    const string start_after("");
    uint64_t max_return = MAX_RETURN;
    std::map<std::string, bufferlist> out_vals;
    librados::ObjectReadOperation rd_op;
  
    rd_op.omap_get_vals(start_after, max_return, &out_vals, &rval);
    
    oid_t toid(LIGHTFS_OBJ_INO, myino);
    string myoid(toid.id);
    bufferlist bl;
  
    r = lctx->ioctx.operate(myoid, &rd_op, &bl);
    if (r < 0) {
      cout << "do_rmdir operate error , r = " << r << endl;
      return r;
    }
    
    _inodeno_t ino = 0;
    std::map<std::string, bufferlist>::iterator p = out_vals.begin();
    for (; p != out_vals.end(); ++p) {
      bufferlist mybl; 
      mybl.append(p->second);
      mybl.append('\0');
      ino = strtoul(mybl.c_str(), NULL, 16);  
      printf("%016lx ", ino);
      do_rmdir(ino);
    }
    cout << endl;
    
    //after children removed, removing myself now
/*
    bufferlist inbl;
    bufferlist outbl;
    r = lctx->ioctx.exec(myoid, "lightfs", "remove_inode", inbl, outbl);
    if (r < 0) {
      cout << "lightfs do_rmdir ioctx.exec failed, r = " << r << endl;
      cout << endl;
    }
*/
    lightfs::cls_client::remove_inode(lctx, myoid);
    return 0;
  }
  
  // non POSIX
  int Lightfs::rmdir(_inodeno_t pino, const char *name)
  {
    //todo : log 
    cout << "lightfs rmdir" << endl;
    int r = -1; 
    int rval = -1;
    oid_t myparent(LIGHTFS_OBJ_INO, pino);
    string parent_obj(parent->id);
    r = lightfs::cls_client::unlink_inode(lctx, parent_obj, name);
    
    return r ; 
  }
  
  //POSIX rmdir
  int Lightfs::rmdir_posix(lightfs_inode_t *parent, const char *name)
  {
    return 0;
  }
  
  int Lightfs::ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, 
  			off_t off, struct fuse_file_info *fi)
  {
    int r = -1;
    int ret = 0;
    char *buf = new char[size];
    std::map<std::string, bufferlist> out_vals;
    r = readdir(ino, 100, out_vals);
    if (r < 0) {
      cout << "readdir failed" << endl;
      ret = r;
      goto out;
    }
  
  out:
    delete [] buf;
    return ret;
  }
  
  /* InoGen member functions  */
  
  void InoGen::encode_gen(uint64_t &inum, uint64_t &max, bufferlist &bl)
  {
    ::encode(inum, bl);
    ::encode(max, bl); 
  }
  
  void InoGen::decode_gen(uint64_t &inum, uint64_t &max, bufferlist &bl)
  {
    bufferlist::iterator p = bl.begin();
    ::decode(inum, p);
    ::decode(max, p);
  }
  
  
  int InoGen::init_gen()
  {
    cout << "InoGen::init_gen" << endl;
    //oid = "generator.0"
    oid_t gen(LIGHTFS_OBJ_GEN, 0);
    string myoid(gen.id);
    cout << "myoid = " << myoid.c_str() << endl;
    librados::ObjectReadOperation read;
    bufferlist bl;
    uint64_t size = 0;
    int rval = -1;
    int r = -1;
    read.stat(&size, NULL, &rval);
    r = lctx->ioctx.operate(myoid, &read, &bl);
    //if generator.0 not exists, means generator not init
    if (r == -ENOENT) {
      cout << "object " << myoid.c_str() << " not exists" << endl;
      long unsigned i = 0;
      long unsigned max = (1 << gen_bits);
      long unsigned up_bound = 0;
      char number[32];
      long unsigned num = 0;
      long unsigned shift = BITS - gen_bits;
      printf("gen_bits = %u, max = %lu\n", gen_bits,  max);
      //object generator.[0, FF]
      for (i = 0; i < max; i++)
      {
        oid_t gen_oid(LIGHTFS_OBJ_GEN, i);
        /*
  	 high                      low
  	  00 | 00 0000 0000 ... 0000
             |   \+-----------------+/
  	  \/             |
  	gen_bits      BITS - gen_bits
        */
        num = i << shift;
        if (i+1 < max)
          up_bound = ((i + 1) << shift) - 1;
        else
  	up_bound = (long unsigned) (-1);
        snprintf(number, sizeof(number), "%lu", num);
  
        bufferlist inbl;
        encode_gen(num, up_bound, inbl);
  
        librados::ObjectWriteOperation op;
        op.write(0, inbl);
        string toid(gen_oid.id);
        r = lctx->ioctx.operate(toid, &op);
        printf("oid = %s, write operate, r = %d\n", gen_oid.id, r);
        if (r < 0) 
  	return r;
      }
      printf("\n");
    }
    return 0;
  }
  
  int InoGen::init_gen_omap()
  {
    //cout << "InoGen::init_gen_omap" << endl;
    //oid = "generator.0"
    oid_t gen(LIGHTFS_OBJ_GEN, 0);
    string myoid(gen.id);
    //cout << "myoid = " << myoid.c_str() << endl;
    librados::ObjectReadOperation read;
    bufferlist bl;
    uint64_t size = 0;
    int rval = -1;
    int r = -1;
    read.stat(&size, NULL, &rval);
    r = lctx->ioctx.operate(myoid, &read, &bl);
    //if generator.0 not exists, means generator not init
    if (r == -ENOENT) {
      cout << "object " << myoid.c_str() << " not exists" << endl;
      long unsigned i = 0;
      long unsigned max = (1 << gen_bits);
      long unsigned up_bound = 0;
      int len = 0;
      char number[32];
      long unsigned num = 0;
      long unsigned shift = BITS - gen_bits;
      printf("gen_bits = %u, max = %lu\n", gen_bits,  max);
      //object generator.[0, FF]
      for (i = 0; i < max; i++)
      {
        oid_t gen_oid(LIGHTFS_OBJ_GEN, i);
        /*
  	 high                      low
  	  00 | 00 0000 0000 ... 0000
             |   \+-----------------+/
  	  \/             |
  	gen_bits      BITS - gen_bits
        */
        num = i << shift;
        if (i+1 < max)
          up_bound = ((i + 1) << shift) - 1;
        else
  	up_bound = (long unsigned) (-1);
        snprintf(number, sizeof(number), "%lu", num);
  
        bufferlist inbl;
        encode_gen(num, up_bound, inbl);
  
        string toid(gen_oid.id);
        r = lctx->ioctx.omap_set_header(toid, inbl);
        printf("oid = %s, set header, r = %d\n", gen_oid.id, r);
        if (r < 0) 
  	return r;
      }
      printf("\n");
    }
    return 0;
  }
  
  int InoGen::do_generate_ino(string myoid)
  {
    cout << "oid = " << myoid.c_str() << endl;
    bufferlist outbl;
    bufferlist new_bl;
    bufferlist inbl;
    int rval = -1;
    int r = -1;
    
    lctx->ioctx.exec(myoid, "lightfs", "lightfs_genino", inbl, outbl);
    cout << "out.length =" << outbl.length() << endl;
    uint64_t num = 0;
    bufferlist::iterator p = outbl.begin();
    assert(!p.end());
    ::decode(num, p);
    cout << "outbl.num = " << num << endl;
    //ino = atol(str);
  /*  
    librados::ObjectReadOperation rd_op;
    rd_op.read(0, GEN_CONTENT_LEN, &outbl, &rval);
    r = lctx->ioctx.operate(myoid, &rd_op, &new_bl);  
    if (r < 0) {
      cout << "ioctx operate failed , r = " << r << endl;
      return r;
    }
  
    if (rval < 0) {
      cout << "read gen content failed, rval = " << rval << endl;
      return rval;
    }
  
  
    uint64_t num = 0, max = 0;
    decode_gen(num, max, outbl);
    printf("num = %lx , max = %lx\n", num, max);
  
    ino = num;
    num++;
    
    librados::ObjectWriteOperation wr_op;
    bufferlist inbl;
    encode_gen(num, max, inbl);
    wr_op.write(0, inbl);
    r = lctx->ioctx.operate(myoid, &wr_op);
    if (r < 0) {
      cout << "ioctx operate failed, r = " << r << endl;
      return r;
    }
  */
    return 0;
  }
  
  int InoGen::do_generate_ino_omap(string myoid, bool is_root)
  {
    //cout << "oid = " << myoid.c_str() << endl;
    bufferlist outbl;
    bufferlist inbl;
    bufferlist new_inbl;
    bufferlist new_outbl;
    int rval = -1;
    int r = -1;
    
    r = lctx->ioctx.exec(myoid, "lightfs", "read_seq", inbl, outbl);
    if (r < 0) {
      cout << "readino_omap failed , r = " << r << endl;
      return r;
    }
    //lctx->ioctx.omap_get_header(myoid, &outbl);
    uint64_t num = 0, max = 0;
    decode_gen(num, max, outbl);
    //cout << "outbl.num = " << num << " max = " << max << endl;
    if (is_root) {
      //root ino already got
      if (num > 0)
        return 0;
    }
    if (num == max) {
      cout << "ino is exthausted in this range" << endl;
      return -ERANGE;
    }
    ino = num;
    num++;
    encode_gen(num, max, new_inbl); 
    r = lctx->ioctx.exec(myoid, "lightfs", "write_seq", new_inbl, new_outbl);
    if (r < 0) {
      cout << "writeino_omap failed , r = " << r << endl;
      return r;
    }
    //lctx->ioctx.omap_set_header(myoid, inbl);
    return 0;
  }
  
  
  _inodeno_t InoGen::generate_ino()
  {
    srandom(time(NULL));    
    uint64_t bound = 1 << gen_bits;
    uint64_t no = random() % bound;
    
    oid_t rand_oid(LIGHTFS_OBJ_GEN, no);
    string myoid(rand_oid.id);
    
    //do_generate_ino(myoid);  
    do_generate_ino_omap(myoid, false);  
  
    return ino;
  }
  
  
  /* Log member functions */
  //int Log::add_log(uint64_t pino, uint64_t ino)
  //{
    
  //}
  }
