/*
usage:gcc ll.c -o ll -D_FILE_OFFSET_BITS=64 -lfuse
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <assert.h>
#include <sys/time.h>

#include "lightfs_fuse.hpp"

namespace lightfs {

  static void print_stat(struct stat *st)
  {
    cout << "[" << endl;
    cout << "st_dev:" << st->st_dev << endl;
    cout << "st_ino:" << hex << st->st_ino << dec << endl;
    cout << "st_mode:" << oct << st->st_mode << dec << endl;
    cout << "st_nlink:" << st->st_nlink << endl;
    cout << "st_uid:" << st->st_uid << endl;
    cout << "st_gid:" << st->st_gid << endl;
    cout << "st_rdev:" << st->st_rdev << endl;
    cout << "st_size:" << st->st_size << endl;
    cout << "st_blksize:" << st->st_blksize << endl;
    cout << "st_blocks:" << st->st_blocks << endl;
    cout << "st_atime:" << st->st_atime << endl;
    cout << "st_mtime:" << st->st_mtime << endl;
    cout << "st_ctime:" << st->st_ctime << endl;
    cout << "]" << endl;
  }

  static void print_fh(Fh *fh)
  {
    cout << "[" << endl;
    cout << "ino:" << hex << fh->ino << dec << endl;
    cout << "*inode:" << hex << fh->inode << dec << endl;
    cout << "pos:" << fh->pos << endl;
    cout << "mode:" << oct << fh->mode << dec << endl;
    cout << "flags:" << oct << fh->flags << dec << endl;
    cout << "]" << endl;
  }
  
  static void fuse_ll_init(void *userdata, struct fuse_conn_info* conn)
  {
    
  }

  static void fuse_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                       mode_t mode)
  {
    if (!VALID_NAME(name)) {
      fuse_reply_err(req, ENAMETOOLONG);
      return;
    }
    int r = -1;
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));

    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_mkdir(req, parent, name, mode, &e.attr);
    if (r == 0) {
      e.ino = e.attr.st_ino;
      fuse_reply_entry(req, &e);
    } else {
      fuse_reply_err(req, -r);
    }
  }


  static void fuse_ll_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
  {
    int r = -1;
    
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req); 
    void *d_buf = NULL;
    r = lfuse->lfs->ll_opendir(req, ino, (dir_buffer **)&d_buf);
    if (r == 0) {
      fi->fh = (long)d_buf;
      fuse_reply_open(req, fi);     
    } else {
      fuse_reply_err(req, -r);
    } 
  }

  static void fuse_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                         struct fuse_file_info *fi)
  {
    /* off : offset in direntry stream ...*/
    int r = -1;
    char *buf = new char[size];

    off_t fill_size = 0;
    cout << "fuse_ll_readdir: off = " << off << endl;

    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req); 
    dir_buffer *d_buffer = (dir_buffer *)fi->fh;
    
    r = lfuse->lfs->ll_readdir(req, ino, off, size, &fill_size, buf, d_buffer);       
    cout << "fuse_ll_readdir: fill_size = " << fill_size << endl;

    if (r == 0) {
      fuse_reply_buf(req, buf, fill_size);
    } else {
      fuse_reply_buf(req, NULL, 0);
    }

    delete []buf;
  }

  static void fuse_ll_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
  { 
    int r = -1;
    if (!fi) {
      fuse_reply_err(req, 1);
      return;
    } 
    dir_buffer *d_buffer = (dir_buffer *)fi->fh;
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_releasedir(req, d_buffer);
    fuse_reply_err(req, -r);
  }  

  static void fuse_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
  {
    int r = -1;
    
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_rmdir(req, parent, name); 
    fuse_reply_err(req, -r);
  }

  static void fuse_ll_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
		const char *name)
  {
    if (!VALID_NAME(name)) {
      fuse_reply_err(req, ENAMETOOLONG);
      return;
    }
    //parent: pino of name (symlink) 
    cout << "symlink: " << link << ", parent:" << hex << parent << dec << " name:" << name << endl;
    int r = -1;
    fuse_entry_param e;
    memset(&e, 0, sizeof(e));
    
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    cout << "symlink: mountpoint = " << lfuse->mountpoint << endl;
    r = lfuse->lfs->ll_symlink(req, link, parent, name, &e.attr);
    if (r == 0) {
      e.ino = e.attr.st_ino;
      fuse_reply_entry(req, &e);
    } else {
      fuse_reply_err(req, -r);
    }
  }

  static void fuse_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
  {
    int r = -1;
    struct fuse_entry_param e;
    memset(&e, 0, sizeof(e));
 
    cout << "fuse_ll_lookup: parent = " << hex << parent << dec << " name = " << name << endl;
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_lookup(req, parent, name, &e.attr);
    cout << "fuse_ll_lookup , ll_lookup = " << r << endl;
    if (r == 0) {
      e.ino = e.attr.st_ino;
      fuse_reply_entry(req, &e);
    } else {
      fuse_reply_err(req, -r);
    }
  }

  static void fuse_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                        fuse_ino_t newparent, const char *newname)
  {
    if (!VALID_NAME(newname)) {
      fuse_reply_err(req, ENAMETOOLONG);
      return;
    }
    cout << "fuse_ll_rename:" << name << " -> " << newname << " parent:" 
	<< hex << parent << " newparent: " << newparent  << dec << endl;
    int r = -1;
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_rename(req, parent, name, newparent, newname);
    fuse_reply_err(req, -r);
  }

  static void fuse_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
			int to_set, struct fuse_file_info *fi)
  {
    cout << "fuse_ll_setattr: ino = " << hex << ino << dec
         << " fi = " << hex << fi << dec << endl;
    int r = -1;
    Fh *fh = NULL;
    if (fi) 
      fh = (Fh *)fi->fh;

    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_setattr(req, ino, attr, to_set, fh);
    if (r == 0) {
      fuse_reply_attr(req, attr, 0);
    } else {
      fuse_reply_err(req, -r);
    }
  }

  static void fuse_ll_getattr(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
  {
    int r = 0;
    cout << "fuse_ll_getattr, ino = " << hex << ino << dec << endl;
    struct stat attr;
    memset(&attr, 0, sizeof(attr));

    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_getattr(req, ino, &attr);

    if (r == 0) {
      fuse_reply_attr(req, &attr, 0);
    } else {
      fuse_reply_err(req, -r);
    }
  }

  static void fuse_ll_readlink(fuse_req_t req, fuse_ino_t ino)
  {
    int r = -1;
    string linkname;    

    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_readlink(req, ino, linkname);
    if (r == 0) {
      fuse_reply_readlink(req, linkname.c_str());
    } else {
      fuse_reply_err(req, -r);
    }
  }

  static void fuse_ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
			mode_t mode, dev_t rdev)
  {
    if (!VALID_NAME(name)) {
      fuse_reply_err(req, ENAMETOOLONG);
      return;
    }
    int r = -1;
    struct fuse_entry_param e;
    
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_mknod(req, parent, name, mode, rdev, &e.attr); 
    if (r == 0) {
      e.ino = e.attr.st_ino;
      fuse_reply_entry(req, &e);
    } else {
      fuse_reply_err(req, -r);
    }
     
  }

  static void fuse_ll_create(fuse_req_t req, fuse_ino_t parent, const char *name,
			mode_t mode, struct fuse_file_info *fi)
  {
    if (!VALID_NAME(name)) {
      fuse_reply_err(req, ENAMETOOLONG);
      return;
    }
    int r = -1;
    struct fuse_entry_param e;
    void *fh = NULL;

    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_create(req, parent, name, mode, &e.attr, fi->flags, (Fh **)&fh);
    print_stat(&e.attr);
    if (r == 0) {
      e.ino = e.attr.st_ino;
      fi->fh = (long)fh;
      r = fuse_reply_create(req, &e, fi);
    } else {
      fuse_reply_err(req, -r);
    }
  }

  static void fuse_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
  {
    int r = -1;
     
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_unlink(req, parent, name);
    fuse_reply_err(req, -r);
  }

  static void fuse_ll_open(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
  {
    int r = -1;
    void *fh = NULL;
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_open(req, ino, fi->flags, (Fh **)&fh);
    cout << "ll_open: r = " << r << endl;
    if (r == 0) {
      fi->fh = (long)fh;
      fuse_reply_open(req, fi);
    } else {
      fuse_reply_err(req, -r);
    }
  }

  static void fuse_ll_release(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
  {
    int r = -1;
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    Fh *fh = (Fh *)fi->fh;
    r = lfuse->lfs->ll_release(req, fh);
    fuse_reply_err(req, -r);
  }

  static void fuse_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                      struct fuse_file_info *fi)
  {
    int r = -1;
    Fh *fh = NULL;
    bufferlist bl;
    if (fi)
      fh = (Fh *)fi->fh;
 
    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_read(req, size, off, fh, &bl);
    if (r >= 0) {
      fuse_reply_buf(req, bl.c_str(), bl.length());
    } else {
      fuse_reply_err(req, -r);
    }
  }


  static void fuse_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                       size_t size, off_t off, struct fuse_file_info *fi)
  {
    int r = -1;
    Fh *fh = NULL;
    if (fi) 
      fh = (Fh *)fi->fh;

    LightfsFuse *lfuse = (LightfsFuse *)fuse_req_userdata(req);
    r = lfuse->lfs->ll_write(req, off, size, buf, fh);
    if (r >= 0) {
      fuse_reply_write(req, r);
    } else {
      fuse_reply_err(req, -r);
    }
  }

  static struct fuse_lowlevel_ops fuse_ll_oper = {
  init:fuse_ll_init,
  destroy:0, 
  lookup:fuse_ll_lookup,
  forget:0,
  getattr:fuse_ll_getattr,
  setattr:fuse_ll_setattr,
  readlink:fuse_ll_readlink,
  mknod:fuse_ll_mknod,
  mkdir:fuse_ll_mkdir,
  unlink:fuse_ll_unlink,
  rmdir:fuse_ll_rmdir,
  symlink:fuse_ll_symlink,
  rename:fuse_ll_rename,
  link:0,
  open:fuse_ll_open,
  read:fuse_ll_read,
  write:fuse_ll_write,
  flush:0,
  release:fuse_ll_release,
  fsync:0,
  opendir:fuse_ll_opendir,
  readdir:fuse_ll_readdir,
  releasedir:fuse_ll_releasedir,
  fsyncdir:0,
  statfs:0,
  setxattr:0,
  getxattr:0,
  listxattr:0,
  removexattr:0,
  access:0,
  create:fuse_ll_create,
  getlk:0,
  setlk:0,
  bmap:0,
  #if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 8)
  #ifdef FUSE_IOCTL_COMPAT
  ioctl:0,
  #else
  ioctl:0,
  #endif
  poll:0,
  #if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 9)
  write_buf:0,
  retrieve_reply:0,
  flock:0,
  fallocate:0
  #endif
  #endif
  }; 

  int LightfsFuse::init(int argc, char *argv[])
  {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    int err = -1;

    if (fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) != -1 &&
        (ch = fuse_mount(mountpoint, &args)) != NULL) {

      se = fuse_lowlevel_new(&args, &fuse_ll_oper,sizeof(fuse_ll_oper), this);
      if (se != NULL) {
	if (fuse_set_signal_handlers(se) != -1) {
	  fuse_session_add_chan(se, ch);
	  err = fuse_session_loop(se);
	  fuse_remove_signal_handlers(se);
	  fuse_session_remove_chan(ch);
	}
        fuse_session_destroy(se);
      }
      fuse_unmount(mountpoint, ch);
    }
    fuse_opt_free_args(&args);

    return err ? 1 : 0;
  }

  int LightfsFuse::loop()
  {
    return 0;
  }

  void LightfsFuse::finalize()
  {

  }

}
