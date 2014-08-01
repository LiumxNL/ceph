//ceph-lfs test client

#include "lightfs.hpp"

using namespace lightfs;

void usage()
{
  //ostringstream oss;
  stringstream oss;
  oss << "./lfs      mkdir 	<parent_ino> 	<dir_name>\n" 
      << "           readdir 	<ino> 		<dir_name>\n" 
      << "           lookup 	<parent_ino> 	<dir_name>\n" 
      << "           rmdir 	<parent_ino> 	<dir_name>\n"
      << "           rm 	<ino>		<name>\n"
      << "           rm 	<parent>	<old_name>	<new_name>\n";
       
  cout << oss.str() << endl;
  
}

void print_inode(lightfs_inode_t &inode, char *name)
{
    cout << name <<": [" << dec << inode.ino << ", " << inode.ctime << ", " << hex << inode.mode << "]" << dec << endl;
}

int main(int argc, char *argv[])
{
  enum operation action = LIGHTFS_NOACTION;
  if (argc < 4) {
    usage();
    exit(-1);
  }
  _inodeno_t ino = 0;
  _inodeno_t pino = 0;
  char *name = NULL;
  char *new_name = NULL;
  if (strcmp(argv[1], "mkdir") == 0) {
    pino = strtoul(argv[2], NULL, 16);
    name = argv[3];
    action = LIGHTFS_MKDIR;
  } else if (strcmp(argv[1], "readdir") == 0) {
    ino = strtoul(argv[2], NULL, 16);
    name = argv[3];
    action = LIGHTFS_READDIR;
  } else if (strcmp(argv[1], "lookup") == 0) {
    pino = strtoul(argv[2], NULL, 16);
    name = argv[3];
    action = LIGHTFS_LOOKUP;
  } else if (strcmp(argv[1], "rmdir") == 0) {
    pino = strtoul(argv[2], NULL, 16);
    name = argv[3];
    action = LIGHTFS_RMDIR;
  } else if (strcmp(argv[1], "rm") == 0) {
    ino = strtoul(argv[2], NULL, 16);
    action = LIGHTFS_RM;
  } else if (strcmp(argv[1], "rename") == 0) {
    if (argc < 5) {
      usage();
      exit(-1);
    }
    pino = strtoul(argv[2], NULL, 16);
    name = argv[3];
    new_name = argv[4];
    action = LIGHTFS_RENAME;
  } else {
    usage();
    exit(-1);
  }
 
  const char *userid="admin";
  const char *pool = "lfs"; 
  LightfsCtx lctx(userid, pool);
  lctx.ctx_init();

  //{
    cout << "test Lightfs class" << endl;
    int r = -1;
    /* test Lightfs */
    Lightfs lfs(&lctx);    
    lfs.create_lightfs();
    //lightfs_inode_t inode3(0, S_IFREG);
    //print_inode(inode3, "inode3");
    //r = lfs.stat_inode(&inode3);
    //cout << "stat inode r = " << r << endl;
    //print_inode(inode3, "inode3");
  //}
  /*
    6. do sth ......
  */
   
  switch(action) {
  case LIGHTFS_MKDIR:
	{
	  cout << "mkdir" << endl;
	  cout << "ino = " << hex << ino << dec << " name = " << name << endl;
	  //pino= 0x8000000000000000;
  	  lightfs_inode_t parent(pino, S_IFDIR);
	  r = lfs.mkdir(&parent, name, S_IFDIR);
	  break;
	}

  case LIGHTFS_READDIR:
	{
	  cout << "readdir" << endl;
	  cout << "ino = " << hex << ino << dec << endl;
  	  //ino = 0x8000000000000000;
  	  lightfs_inode_t myself(ino, S_IFDIR);
	  r = lfs.readdir(&myself, NULL, 0);
	  break;
	}
  case LIGHTFS_LOOKUP:
	{
	  cout << "lookup" << endl;
	  cout << "pino = " << hex << pino << dec << endl;
  	  lightfs_inode_t parent(pino, S_IFDIR);
	  lightfs_inode_t target(-1, S_IFDIR);
	  r = lfs.lookup(&parent, name, &target);
	  break;
	}
  case LIGHTFS_RMDIR:
	{
	  cout << "rmdir" << endl;
	  cout << "pino = " << hex << pino << dec << endl;
 	  cout << "dir = " << name << endl;
  	  lightfs_inode_t parent(pino, S_IFDIR);
	  r = lfs.rmdir(&parent, name);
	  break;
	}
  case LIGHTFS_RM:
	{
	  cout << "rm" << endl;
	  cout << "ino = " << hex << ino << dec << endl;
 	  //char oid[INO_BITS+INODE_SUFFIX_LEN];
 	  //make_oid(oid, sizeof(oid), ino);
	  //r = remove_object(oid);
	  break;
	}
  case LIGHTFS_RENAME:
	{
	  cout << "rename" << endl;
	  cout << "ino = " << hex << ino << dec << endl;
	  //r = rename(ino, name, new_name);
	  break;
	}
  default:
	{
	  cout << "unknown action" << endl;
	  cout << "test atomic" << endl;
	  
	}

  }
  
  /* teset oid_t */
/*
  oid_t myoid1(LIGHTFS_OBJ_INO, -1);
  oid_t myoid2(LIGHTFS_OBJ_DATA, -2, -1);
  oid_t myoid3(LIGHTFS_OBJ_GEN, -3);
*/
  //oid_t myoid4((enum lightfs_obj_type)4, -4);
/*
  oid_t myoid4(LIGHTFS_OBJ_LOG, -4);
  printf("myoid4.id=%s\n",myoid4.id);
*/
  //printf("%d, %d, %d, %d\n",myoid1.get_obj_type(), myoid2.get_obj_type(), myoid3.get_obj_type(), myoid4.get_obj_type());
  
  {
  /* test InoGen */

/*
  InoGen inogen1(&lctx,4);
  inogen1.init_gen_omap();
	
  inogen1.generate_ino();
  printf("ino = %lx\n", inogen1.get_ino());
*/
  }

 
  {
/*
    cout << "test Lightfs class" << endl;
    int r = -1;
    Lightfs lfs(&lctx);    
    lfs.create_lightfs();
    lightfs_inode_t inode3(0, S_IFREG);
    print_inode(inode3, "inode3");
    r = lfs.stat_inode(&inode3);
    cout << "stat inode r = " << r << endl;
    print_inode(inode3, "inode3");
*/
  }

//  string oid_0("generator.0");
//  inogen1.do_generate_ino_omap(oid_0);
/* 
  int a = 256;
  char *ptr = (char *)&a;
  printf("p[0]:%x, p[1]:%x, p[2]:%x\n", ptr[0], ptr[1], ptr[2]);

  int b = 256;
  bufferlist bl;
  encode_raw(b, bl);
  bufferlist::iterator p = bl.begin();
  int c = 0;
  decode_raw(c, p);
  printf("c=%d\n",c);

  uint64_t n1 = 255, n2=1024, n11=0, n22=0;
  bufferlist ibl;
  inogen1.encode_gen(n1, n2, ibl);
  inogen1.decode_gen(n11, n22, ibl);
  printf("n1=%d, n2=%d\n", n11, n22);

  _inodeno_t myino = inogen1.generate_ino();
  printf("myino = %lx\n", myino);
*/
  /*test cls*/
  {
    int r = -1;
    int rval = -1;
    bufferlist inbl;
    bufferlist outbl;
    char *obj = NULL;
    if (argc < 5)
      return 0;
    obj = argv[4];
    cout << "argv[4] = " << obj << endl;
    string toid(obj);
    librados::ObjectOperation op;
    //inbl.append("from client");
    r = lctx.ioctx.exec(toid, "hello", "record_hello", inbl, outbl);
    cout << "r = " << r << endl;
    if (r < 0) {
      cout << "cls exec failed, r = " << r << endl;
      return r;
    }   
    //cout << "outbl = " << outbl.c_str(); 
    cout << endl;
  }
  lctx.ctx_destroy();

  return 0;
}

