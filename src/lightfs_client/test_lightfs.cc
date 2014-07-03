//ceph-lfs test client

#include "lightfs.h"

void usage()
{
  //ostringstream oss;
  stringstream oss;
  oss << "./ceph-lfs mkdir 	<parent_ino> 	<dir_name>\n" 
      << "           readdir 	<ino> 		<dir_name>\n" 
      << "           lookup 	<parent_ino> 	<dir_name>\n" 
      << "           rmdir 	<parent_ino> 	<dir_name>\n"
      << "           rm 	<ino>		<name>\n"
      << "           rm 	<parent>	<old_name>	<new_name>\n";
       
  cout << oss.str() << endl;
  
}

int main(int argc, char *argv[])
{
  enum operation action = LIGHTFS_NOACTION;
  if (argc < 4) {
    usage();
    exit(-1);
  }
  inode_num_t ino = 0;
  char *name = NULL;
  char *new_name = NULL;
  if (strcmp(argv[1], "mkdir") == 0) {
    ino = atoll(argv[2]);
    name = argv[3];
    action = LIGHTFS_MKDIR;
  } else if (strcmp(argv[1], "readdir") == 0) {
    ino = atoll(argv[2]);
    name = argv[3];
    action = LIGHTFS_READDIR;
  } else if (strcmp(argv[1], "lookup") == 0) {
    ino = atoll(argv[2]);
    name = argv[3];
    action = LIGHTFS_LOOKUP;
  } else if (strcmp(argv[1], "rmdir") == 0) {
    ino = atoll(argv[2]);
    name = argv[3];
    action = LIGHTFS_RMDIR;
  } else if (strcmp(argv[1], "rm") == 0) {
    ino = atoll(argv[2]);
    action = LIGHTFS_RM;
  } else if (strcmp(argv[1], "rename") == 0) {
    if (argc < 5) {
      usage();
      exit(-1);
    }
    ino = atoll(argv[2]);
    name = argv[3];
    new_name = argv[4];
    action = LIGHTFS_RENAME;
  } else {
    usage();
    exit(-1);
  }

  rados_t cluster;
  const char * userid = "admin";
  const char * pool = "lfs";
  int r = 0;
  /*
    1. create cluster
  */
  r = rados_create(&cluster, userid);
  if ( r < 0) {
    cout << "rados_create error , r = " << r << endl;
    return r;
  }
  
  /*
    2. read conf file to cluster
  */
  r = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
  if ( r < 0 ) {
    cout << "conf read error , r = " << r << endl;
    return r;
  }

  /*
    3. connect to cluster
  */
  r = rados_connect(cluster);
  if ( r < 0 ) {
    cout << "rados connect error , r = " << r << endl;
    return r;
  }
  
  /*
    4. lookup or create pool in cluster
  */
  r= rados_pool_create(cluster, pool);
  if ( r < 0 ) {
    cout << "pool create  error , r = " << r << endl;
    return r;
  }

  /*
    5. in cluster: creat ioctx , then do read ,write ......
  */
  r = rados_ioctx_create(cluster, pool, &ioctx);
  if ( r < 0 ) {
    cout << "ioctx create  error , r = " << r << endl;
    return r;
  }
  
  //cout << "parepare cluster & ioctx OK !!!" << endl;

  /*
    6. do sth ......
  */
  init_lightfs();
   
  switch(action) {
  case LIGHTFS_MKDIR:
	{
	  r = mkdir(ino, name);
	  break;
	}

  case LIGHTFS_READDIR:
	{
	  r = readdir(ino, name);
	  break;
	}
  case LIGHTFS_LOOKUP:
	{
	  r = lookup(ino, name);
	  break;
	}
  case LIGHTFS_RMDIR:
	{
	  r = rmdir(ino, name);
	  break;
	}
  case LIGHTFS_RM:
	{
 	  char oid[INO_BITS+INODE_SUFFIX_LEN];
 	  make_oid(oid, sizeof(oid), ino);
	  r = remove_object(oid);
	  break;
	}
  case LIGHTFS_RENAME:
	{
	  r = rename(ino, name, new_name);
	  break;
	}
  default:
	{
	  cout << "unknown action" << endl;
	  cout << "test atomic" << endl;
	  
	}

  }
  
  /*
    7. shutdown cluster
  */
  rados_ioctx_destroy(ioctx);

  rados_shutdown(cluster);

  return 0;
}

