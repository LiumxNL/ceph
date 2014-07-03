//light file system client 
#include "lightfs.h"

rados_ioctx_t ioctx;

inode_num_t get_random_ino()
{
  srandom(time(NULL));
  inode_num_t ino = 0;
  ino = (inode_num_t)random();
  return ino;
}

int make_oid(char *oid, size_t len, inode_num_t ino)
{
  if (oid == NULL || len == 0)
    return -1;
  size_t n = snprintf(oid, len, "%llx.inode", ino);
  if (!n)
    return -1;
  return 0;
}

char *ino_c_str(char *ino_str, size_t len, inode_num_t ino)
{
  if (ino_str == NULL || len == 0) {
    return NULL;
  }
  size_t n = snprintf(ino_str, len, "%llx", ino);
  if (!n)
    return NULL;
  return ino_str;
}

int create_object(rados_write_op_t write, const char * oid, const char *parent_ino_str, const char *my_ino_str)
{
  if (oid == NULL || parent_ino_str == NULL || my_ino_str == NULL)
    return -1;

  int r = 0;
  const char *cur = ".";
  const char *parent = "..";
  const char *cur_val = my_ino_str;
  const char *parent_val = parent_ino_str;
  char const *keys[] = {cur, parent};
  char const *vals[] = {cur_val, parent_val};
  const size_t len1 = strlen(cur_val);
  const size_t len2 = strlen(parent_val);
  const size_t lens[] = {len1, len2};
  size_t num = 2;

  rados_write_op_omap_set(write, keys, vals, lens, num);

  r = rados_write_op_operate(write, ioctx, oid, NULL, LIBRADOS_OPERATION_NOFLAG);
  
  if (r < 0) {
    cout << "create object failed (rados_write_op_operate failed , r = " << r  << ")"<< endl;  
    return r;
  }

  return 0;  
}

int remove_object(const char *oid)
{
  assert(oid != NULL);
  return rados_remove(ioctx, oid);
}


int init_lightfs()
{
  //cout << "init_lightfs" << endl;
  const char *oid = "1.inode"; 
  rados_write_op_t write = rados_create_write_op();
  return create_object(write, oid, NULL_STR, LIGHTFS_ROOT_INO_STR);
  rados_release_write_op(write);
}

int mkdir(inode_num_t parent_ino, const char *dir_name)
{
  /*
    1. create child object
    2. set omap in parent object
    3. delete child object if needed
  */
  int r = 0;
  char ino_str[INO_BITS];
  char parent_ino_str[INO_BITS];
  char oid[INO_BITS+INODE_SUFFIX_LEN];
  char parent_oid[INO_BITS + INODE_SUFFIX_LEN];

  inode_num_t ino = get_random_ino();

  ino_c_str(ino_str, INO_BITS, ino);
  ino_c_str(parent_ino_str, INO_BITS, parent_ino);
  make_oid(oid, sizeof(oid), ino);
  make_oid(parent_oid, sizeof(parent_oid), parent_ino);

  rados_write_op_t write = rados_create_write_op();

  //1. create child object
  r = create_object(write, oid, parent_ino_str, ino_str); 
  if (r < 0 ) {
    rados_release_write_op(write);
    return r;
  }

  
  char const *keys[] = {dir_name};
  char const *vals[] = {ino_str};
  size_t len = strlen(ino_str);
  size_t lens[] = {len};
  size_t num = 1;

  //2. set omap in parent object if needed
  //find the entry in parent object
  if (exist_in_parent(parent_ino, dir_name))
  {
    cout << "sub dir already exists" << endl;
    remove_object(oid); 
    rados_release_write_op(write);
    return 0;
  }
  //entry not exists , so add it 
  rados_write_op_omap_set(write, keys, vals, lens, num); 
  r = rados_write_op_operate(write, ioctx, parent_oid, NULL, LIBRADOS_OPERATION_NOFLAG);
  if (r < 0 ) {
    cout << "add subdir entry failed (rados_write_op_operate error = " << r << " )";
    cout << "so remove child object ...)" << endl;
    //3. delete child object
    int ret = rados_remove(ioctx, oid);
    if (ret < 0) {
      cout << "remove subdir object failed , error = " << ret << endl;
      return ret;
    }
    rados_release_write_op(write);
    return r;
  }
  
  rados_release_write_op(write);
  return 0;
}

int readdir(inode_num_t ino, const char *dir_name)
{
  char oid[INO_BITS + INODE_SUFFIX_LEN];
  make_oid(oid, sizeof(oid), ino);
  rados_read_op_t read = rados_create_read_op();
  rados_omap_iter_t itr;
  const char* start_after = "";
  uint64_t max_return = READ_MAX;
  int rval = -1;
  int r = -1;
  rados_read_op_omap_get_keys(read, start_after, max_return, &itr, &rval);
  r = rados_read_op_operate(read, ioctx, oid, LIBRADOS_OPERATION_NOFLAG);
  if (r < 0 || rval < 0) {
    cout << "omap get keys error = " << rval << endl;
    return rval;
  }

  char *key = NULL;
  char ino_str[INO_BITS];
  char *val = ino_str;
  size_t len = INO_BITS;

  int count = 0;
  do {
    rados_omap_get_next(itr, &key, &val, &len);
    if (key == NULL)
      break;
    //cout << "key = " << key << "  val = " << (val == NULL ? "NULL" : val) << "  len = " << len << endl;
    cout << key << endl;
    ++count;
  } while (key != NULL);

  cout << "count = " << count << endl;
  if (key == NULL) { 
    ;//cout << "list is up to end" << endl;
  }

  rados_omap_get_end(itr);
  rados_release_read_op(read); 
  return 0;
}

/*
  find name if exist in parent
  @param parent_ino: ino of parent object
  @param dir_name: name of sub dir
  @return val: 1 on exists, 0 on not exists
*/
int exist_in_parent(inode_num_t parent_ino, const char *dir_name)
{
  if (strcmp(dir_name, ".") == 0 || strcmp(dir_name, "..") == 0) {
    cout << "exist_in_parent: can't operate on \".\" or \"..\"" << endl;
    return -1;
  }
  char parent_oid[INO_BITS + INODE_SUFFIX_LEN];
  make_oid(parent_oid, sizeof(parent_oid), parent_ino);
  
  int r = -1;
  rados_read_op_t read = rados_create_read_op();
  rados_omap_iter_t itr;
  char const *keys[] = {dir_name};
  size_t keys_len = sizeof(keys)/sizeof(char *);
  int rval = -1;
  rados_read_op_omap_get_vals_by_keys(read, keys, keys_len, &itr, &rval);
  r = rados_read_op_operate(read, ioctx, parent_oid, LIBRADOS_OPERATION_NOFLAG);
  if (r < 0) {
    cout << "get_vals_by_keys failed , error = " << r << endl;
    return r;
  }
  
  char *key = NULL;
  char ino_str[INO_BITS];
  char *val = ino_str;
  size_t len = 255;
 
  int count = 0; 
  do {
    rados_omap_get_next(itr, &key, &val, &len);
    if (val == NULL)
      break;
    count++;
  } while (val != NULL);
  rados_omap_get_end(itr);
  rados_release_read_op(read); 
  if (count > 0) 
    return 1;  

  return 0;
}

/*
  do_lookup : find ino of dir_name in parent object
  @param parent_ino: parent ino to make parent oid (<paent_ino>.inode) 
  @param dir_name: this dir name
  @param my_ino: dir inode number in string (eg ."ab88fd")
  @param my_len: length of char * my_ino

  @return : count of dir_name on success , negative code on failure 
*/
int do_lookup(inode_num_t parent_ino, const char *dir_name, char *my_ino, size_t my_len)
{
  if (strcmp(dir_name, ".") == 0 || strcmp(dir_name, "..") == 0) {
    cout << "can't lookup \".\" or \"..\"" << endl;
    return -1;
  }
  char parent_oid[INO_BITS + INODE_SUFFIX_LEN];
  make_oid(parent_oid, sizeof(parent_oid), parent_ino);
  
  int r = -1;
  rados_read_op_t read = rados_create_read_op();
  rados_omap_iter_t itr;
  char const *keys[] = {dir_name};
  size_t keys_len = sizeof(keys)/sizeof(char *);
  int rval = -1;
  rados_read_op_omap_get_vals_by_keys(read, keys, keys_len, &itr, &rval);
  r = rados_read_op_operate(read, ioctx, parent_oid, LIBRADOS_OPERATION_NOFLAG);
  if (r < 0) {
    cout << "get_vals_by_keys failed , error = " << r << endl;
    return r;
  }
  
  char *key = NULL;
  char ino_str[INO_BITS];
  char *val = ino_str;
  size_t len = 255;
 
  int count = 0; 
  do {
    rados_omap_get_next(itr, &key, &val, &len);
    if (val == NULL)
      break;
    //cout << val << endl;
    assert(my_len > len);
    memcpy(my_ino, val, len);
    my_ino[len] = '\0'; 
    count++;
  } while (val != NULL);
  rados_omap_get_end(itr);
  rados_release_read_op(read); 
  if (count < 1) {
    cout << dir_name << " not exists in parent omap" <<endl;
    return -ENOENT;  
  }
  return 0;
}

int lookup(inode_num_t parent_ino, const char *dir_name)
{
  char my_ino[INO_BITS]={'\0'};
  int r = -1;
  r = do_lookup(parent_ino, dir_name, my_ino, INO_BITS); 
  if (strlen(my_ino) > 0)
    cout << my_ino << endl;
  return r;
}

int get_subs(rados_omap_iter_t &itr, const char *oid, size_t count, deque<char *> &sub_inos, deque<char *> &sub_names)
{
  if (oid == NULL)
    return -1;
  
  rados_read_op_t read = rados_create_read_op();
  const char *start_after = "";
  int rval = -1;
  int r = -1;
  //rados_read_op_omap_get_keys(read, start_after, count, &itr, &rval);
  rados_read_op_omap_get_vals(read, start_after, NULL, count, &itr, &rval);
  r = rados_read_op_operate(read, ioctx, oid, LIBRADOS_OPERATION_NOFLAG); 
  if ( r < 0) {
    cout << "read op operate error = " << r << endl;
    return r;
  }
  if ( rval < 0) {
    cout << "omap get keys error = " << rval << endl;
    return rval;
  }

  char *key = NULL;
  char *val = NULL;
  size_t len = 0;
  do {
    r = rados_omap_get_next(itr, &key, &val, &len);
    if (val == NULL || key == NULL || len == 0)
      break;
    //cout << key << << "val = " << (val == NULL ? "NULL" : val) << endl;
    val[len] = '\0';
    if ( (strcmp(key, "..") == 0) || (strcmp(key, ".") == 0)) 
      continue;
    //cout << "key = " << (key == NULL ? "NULL" : key) << " sizeof(key) = " << sizeof(key) << " strlen(key) = " << strlen(key) <<"  val = " << (val == NULL ? "NULL" : val) << "  len = " << len << endl;
    //key[strlen(key)] = '\0';
    sub_names.push_back(key);
    sub_inos.push_back(val);
  } while (val != NULL);

  rados_release_read_op(read); 
  return 0;
}

int do_rmdir(char *ino_str, const char *dir_name)
{
  assert(ino_str != NULL);
  assert(dir_name != NULL);
  cout << "level("<< dir_name <<")start ";

  int r = -1;
  int i = 0;
  int len = strlen(ino_str);
  assert(len < INO_BITS);
  char oid[INO_BITS + INODE_SUFFIX_LEN]={'\0'};
  strncat(oid, ino_str, len);
  strncat(oid, ".inode", INODE_SUFFIX_LEN);
  oid[len+INODE_SUFFIX_LEN] = '\0';
  cout << " oid=" << oid << " " ;

  //get all subs(children)
  rados_omap_iter_t itr;  
  deque<char *> sub_inos;
  deque<char *> sub_names;
  r = get_subs(itr, oid, READ_MAX, sub_inos, sub_names); 
  if (r < 0) {
    cout << "get_subs error = " << r << endl;
    cout << "level("<< dir_name <<")end " << endl;
    rados_omap_get_end(itr);
    return r;
  }
  int q_len = sub_inos.size();
  for (i=0; i<q_len; i++) {
    cout << " <" << sub_inos[i] << ", " << sub_names[i] << "> " ;
  }
  assert(sub_inos.size() == sub_names.size());

  //i am the lowest level(leaf), remove myself and return to my parent dir
  if (q_len == 0) {
    cout << " r = remove_object(" << oid << ") ";
    r = remove_object(oid);    
    cout << "level("<< dir_name <<")end " << endl;
    rados_omap_get_end(itr);
    return 0;
  } 

  //i am non-empty dir
  //traverse to lower level, and remove all children and their children, and so on...
  for (i=0; i<q_len; i++) {
    r = do_rmdir(sub_inos[i], sub_names[i]);
  }

  /*
    after traverse to next level, back to this level
    remove this dir or file
  */
  cout << " r = remove_object(" << oid << ") ";
  r = remove_object(oid);    
  cout << "level("<< dir_name <<")end " << endl;
  //memory link, if not call rados_omap_get_end
  rados_omap_get_end(itr);
  return 0;
}

int rmdir(inode_num_t parent_ino, const char *dir_name)
{
  if (parent_ino < 1 || dir_name == NULL)
    return -1;

  if (strcmp(dir_name, "/") == 0) {
    cout << "can not remove root \"/\"" << endl;
    return -1;
  }
  
  if (strcmp(dir_name, ".") == 0) {
    cout << "can not remove myself " << endl;
    return -1;
  }

  if (strcmp(dir_name, "..") == 0) {
    cout << "can not remove parent " << endl;
    return -1;
  }

  int r = -1;
  char dir_ino[INO_BITS] = {'\0'};

  r = do_lookup(parent_ino, dir_name, dir_ino, sizeof(dir_ino));
  if (r < 0)
    return r;
  
  cout << "dir_ino = " << dir_ino << " dir_name = " << dir_name << endl; 
  r = do_rmdir(dir_ino, dir_name); 
  if (r < 0)
    return r;

  //remove dir in parent's omap
  char parent_oid[INO_BITS + INODE_SUFFIX_LEN];
  make_oid(parent_oid, sizeof(parent_oid), parent_ino);
  rados_write_op_t write = rados_create_write_op();
  const char *keys[] = {dir_name};
  size_t keys_len = 1;
  rados_write_op_omap_rm_keys(write, keys, keys_len);
  r = rados_write_op_operate(write, ioctx, parent_oid, NULL, LIBRADOS_OPERATION_NOFLAG);
  if (r < 0) {
    cout << "rmdir failed in parent omap, write op operate , r = " << r << endl;
    rados_release_write_op(write);
    return r;
  } 
  rados_release_write_op(write);

  return 0;
}

/*
  in the same dir, rename a file
*/
int rename(inode_num_t parent_ino, const char *old_name, const char *new_name)
{
  char parent_oid[INO_BITS + INODE_SUFFIX_LEN];
  char ino_str[INO_BITS];
  make_oid(parent_oid, sizeof(parent_oid), parent_ino);

  int r = -1;

  r = do_lookup(parent_ino, old_name, ino_str, sizeof(ino_str));
  if (r < 0) 
    return r;
  
  if (exist_in_parent(parent_ino, new_name)) {
    cout << "rename: " << new_name << " already exists in parent omap" << endl;
    return -1;
  }
 
  rados_write_op_t write = rados_create_write_op();
  
  const char *keys_old[] = {old_name};
  size_t keys_old_len = 1;
  const char *keys_new[] = {new_name};
  const char *vals_new[] = {ino_str};
  size_t len_new = strlen(ino_str);
  size_t lens_new[] = {len_new};
  size_t count = 1;
  
  rados_write_op_omap_rm_keys(write, keys_old, keys_old_len);
  rados_write_op_omap_set(write, keys_new, vals_new, lens_new, count); 
  
  r = rados_write_op_operate(write, ioctx, parent_oid, NULL, LIBRADOS_OPERATION_NOFLAG);
  if (r < 0) {
    cout << "rename new inode error , r = " << r << endl;
    return r;
  }

  return 0; 
}
