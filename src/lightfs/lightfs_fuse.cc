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
  
  static void fuse_ll_init(void *userdata, struct fuse_conn_info* conn)
  {
    
  }

  static void fuse_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                       mode_t mode)
  {
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
    void *d_buf;
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
  {}

  static void fuse_ll_getattr(fuse_req_t req, fuse_ino_t ino,
			struct fuse_file_info *fi)
  {
    int r = 0;
    cout << "fuse_ll_getattr, ino = " << ino << endl;
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

  static void fuse_ll_open(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi)
  {}

  static void fuse_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                      struct fuse_file_info *fi)
  {}

  static void fuse_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                       size_t size, off_t off, struct fuse_file_info *fi)
  {}

  static struct fuse_lowlevel_ops fuse_ll_oper = {
  init:fuse_ll_init,
  destroy:0, 
  lookup:fuse_ll_lookup,
  forget:0,
  getattr:fuse_ll_getattr,
  setattr:0,
  readlink:0,
  mknod:0,
  mkdir:fuse_ll_mkdir,
  unlink:0,
  rmdir:fuse_ll_rmdir,
  symlink:0,
  rename:0,
  link:0,
  open:0,
  read:0,
  write:0,
  flush:0,
  release:0,
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
  create:0,
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




/*
#define ERRO 0

FILE *input;

void show_info(const char *str)
{
	input = fopen("show_info.txt","a");
	fprintf(input,"%s\n",str);
	fclose(input);
}

static void fuse_ll_getattr(fuse_req_t req, fuse_ino_t ino,struct fuse_file_info *fi)
	{
		struct stat stbuf;
		memset(&stbuf,0,sizeof(stbuf));
		stbuf.st_ino = ino;

		char buf[65] = {'\0'};
		sprintf(buf, "%lu", ino);
		show_info(buf);

		if(ino == 1)
			stbuf.st_mode = S_IFDIR | 0755;
		else	stbuf.st_mode = S_IFREG | 0644;
		if(ERRO)
//int fuse_reply_err (fuse_req_t req, int err)
			fuse_reply_err(req,-errno);
//int fuse_reply_attr(fuse_req_t req, const struct stat *attr,double attr_timeout);
		else
			fuse_reply_attr(req,&stbuf,1.0);
	}

static void fuse_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                       mode_t mode)
	{
		struct fuse_entry_param e;
		mkdir(name,mode);
		memset(&e,0,sizeof(e));
		e.ino = 3;
		e.attr.st_ino = e.ino;
		e.attr.st_mode = S_IFDIR | 0755;
		e.attr_timeout = 1.0;
		e.entry_timeout = 1.0;
		if(ERRO)
			fuse_rep_err(req,-errno);
		else
//int fuse_reply_entry(fuse_req_t req, const struct fuse_entry_param *e);
			fuse_reply_entry(req,&e);
	}
                       
static void fuse_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
	{
		if(ERRO)
			fuse_reply_err(req,-errno);
		
	}
static void fuse_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                         struct fuse_file_info *fi)
	{
		size_t rd_size;

		if(ERRO)
			fuse_reply_err(req,-errno);
		else
		if(off<100)
		{
			char buf[100];
			const char *name = "name";
			struct stat stbuf;
			memset(&stbuf,0,sizeof(stbuf));
			stbuf.st_ino = ino;
			stbuf.st_mode = S_IFREG | 0644;
//size_t fuse_add_direntry(fuse_req_t req, char *buf, size_t bufsize,const char *name, const struct stat *stbuf,off_t off);
			rd_size = fuse_add_direntry(req,buf,100,name,&stbuf,100);

//int fuse_reply_buf(fuse_req_t req, const char *buf, size_t size);
			fuse_reply_buf(req,buf,100);
		}
		else fuse_reply_buf(req,NULL,0);
	}
static void fuse_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
	{
		struct fuse_entry_param e;
		memset(&e,0,sizeof(e));
		e.ino = 2;
		e.attr.st_ino = e.ino;
		e.attr.st_mode = S_IFREG | 0644;
		e.attr_timeout = 1.0;
		e.entry_timeout = 1.0;
  		show_info("fuse_ll_lookup");
		if(ERRO)
			fuse_reply_err(req,-errno);
		else
			fuse_reply_entry(req,&e);
	}
static void fuse_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                        fuse_ino_t newparent, const char *newname)
	{
		if(ERRO)
			fuse_reply_err(req,-errno);
	}

static void fuse_ll_open(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi)
	{
		if(ERRO)
			fuse_reply_err(req,-errno);
		else
			fuse_reply_open(req,fi);
	}
static void fuse_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                      struct fuse_file_info *fi)
	{
		if(ERRO)
			fuse_reply_err(req,-errno);
		else
		{
			char *buf = NULL;
			fuse_reply_buf(req,buf,0);
		}
	}

static void fuse_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                       size_t size, off_t off, struct fuse_file_info *fi)
	{
		if(ERRO)
			fuse_reply_err(req,-errno);
		else
			fuse_reply_write(req,0);
	}

static void fuse_ll_release(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
	{
		if(ERRO)
			fuse_reply_err(req,-errno);
	}

static struct fuse_lowlevel_ops fuse_ll_oper = {
	.getattr	= fuse_ll_getattr,
	.mkdir		= fuse_ll_mkdir,
	.rmdir		= fuse_ll_rmdir,
	.readdir	= fuse_ll_readdir,
	.lookup		= fuse_ll_lookup,
	.rename		= fuse_ll_rename,
	
//	.open		= NULL,
//	.read		= NULL,
//	.write		= NULL,
//	.release	= NULL,
};

int main(int argc, char *argv[])
{
//define FUSE_ARGS_INIT(argc, argv) { argc, argv, 0 }
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_chan *ch;
	char *mountpoint;
	int err = -1;

	if (fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) != -1 &&
	    (ch = fuse_mount(mountpoint, &args)) != NULL) {
		struct fuse_session *se;

		se = fuse_lowlevel_new(&args, &fuse_ll_oper,
				       sizeof(fuse_ll_oper), NULL);
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
*/
