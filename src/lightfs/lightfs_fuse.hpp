
#ifndef _LIGHTFS_FUSE_HPP_
#define _LIGHTFS_FUSE_HPP_

#define FUSE_USE_VERSION 30

#include "lightfs.hpp"

namespace lightfs {

  class LightfsFuse
  {
  public:
    struct fuse_chan *ch;
    struct fuse_session *se;
    char *mountpoint;
    Lightfs *lfs;
    
    LightfsFuse(Lightfs *fs) : ch(NULL), se(NULL), mountpoint(NULL), lfs(fs) {}
    ~LightfsFuse() {}
 
    int init(int argc, char *argv[]);
    int loop();
    void finalize();    
  };
}
#endif
