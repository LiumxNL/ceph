
#include "lightfs.hpp"
#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"

#include <iostream>
#include <stdio.h>

using namespace librados;

namespace lightfs {

  const char *CONF = "/etc/ceph/ceph.conf";
  const char *LIGHTFS_POOL = "lfs";
  const char *USER_ID = "admin";

  class LightfsCtx {
  public:
    Rados _rados;
    IoCtx _ioctx;
    std::string _conf;
    std::string _pool;
    
    LightfsCtx(const char *conf = CONF, const char *pool = LIGHTFS_POOL) :
       _rados(),
       _ioctx(), 
       _conf(conf),
       _pool(pool)
    {}
    ~LightfsCtx() {}
    
    int init_ctx() 
    {
      int r = -1;
      r = _rados.init(USER_ID);
      if (r < 0) {
        cerr << "cannot init rados" << endl;
        return r;
      }

      r = _rados.conf_read_file(_conf.c_str());
      if (r < 0) {
        cerr << "cannot open conf" << endl;
        return r;
      }

      r = _rados.connect();
      if (r < 0) {
        cerr << "Cannot connect cluster" << endl;
        return r;
      }
  
      cout << "lookup pool = " << _rados.pool_lookup(_pool.c_str()) << endl;

      r = _rados.ioctx_create(_pool.c_str(), _ioctx);
      if (r < 0) {
        cerr << "Cannot open pool " << _pool << endl;
        return r;
      }
      return 0; 
    }

    void destroy_ctx() 
    {
      _ioctx.close();
      _rados.shutdown(); 
    }
  };
}
