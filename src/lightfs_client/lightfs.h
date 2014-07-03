//light file system client
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

#include <iostream>
#include <string>
#include <sstream>
#include <deque>

#include "include/rados/librados.h"
#include "include/types.h"
#include "include/buffer.h"


#define INO_BITS 65 /* 64 + 1 for string transfer to "%llx" */
#define INODE_SUFFIX_LEN 6 /* strlen(".inode") == 6, so char oid[INO_BITS+INODE_SUFFIX_LEN] */
#define LIGHTFS_ROOT_INO 1
#define LIGHTFS_ROOT_INO_STR "1"
#define LIGHTFS_ROOT_NAME "/"
#define NULL_STR "NULL"
#define READ_MAX 65535

enum operation {
  LIGHTFS_NOACTION,
  LIGHTFS_MKDIR,
  LIGHTFS_READDIR,
  LIGHTFS_LOOKUP,
  LIGHTFS_RMDIR,
  LIGHTFS_RM,
  LIGHTFS_RENAME,
};

//global var, for all funcitons
extern rados_ioctx_t ioctx;

typedef long long unsigned inode_num_t;

inode_num_t get_random_ino();
int make_oid(char *oid, size_t len, inode_num_t ino);
char *ino_c_str(char *ino_str, size_t len, inode_num_t ino);

int create_object(rados_write_op_t write, const char *oid, const char *parent_ino_str, const char *my_ino_str);
int remove_object(const char *oid);
int init_lightfs();
int mkdir(inode_num_t parent_ino, const char *dir_name);
int readdir(inode_num_t ino, const char *dir_name);
int exist_in_parent(inode_num_t parent_ino, const char *dir_name);
int do_lookup(inode_num_t parent_ino, const char *dir_name, char *my_ino, size_t my_len);
int lookup(inode_num_t parent_ino, const char *dir_name);
int get_subs(rados_omap_iter_t &itr, const char *oid, size_t count, deque<char *> &sub_inos, deque<char *> &sub_names);
int do_rmdir(char *ino_str, const char *dir_name);
int rmdir(inode_num_t parent_ino, const char *dir_name);
int rename(inode_num_t parent, const char *old_name, const char *new_name);

