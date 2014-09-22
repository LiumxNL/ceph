#include <stdio.h>
#include <iostream>
#include <string>
#include <string.h>

#include "logger.hpp"

#include "common/ceph_context.h"

#include "include/rados/librados.hpp"
#include "lightfs.hpp"

#define dout_subsys ceph_subsys_client

using namespace std;
using namespace librados;

namespace lightfs
{


  void test(IoCtx &ioctx)
  {
 //   CephContext * ctx = ioctx.cct();
  /*  
      writeback_handler = new ObjecterWriteback(objecter);
    ObjectCacher cacher(cct, "objecttest", *writeback_handler, client_lock,
				  client_flush_set_callback,    // all commit callback
				  (void*)this,
				  cct->_conf->client_oc_size,
				  cct->_conf->client_oc_max_objects,
				  cct->_conf->client_oc_max_dirty,
				  cct->_conf->client_oc_target_dirty,
				  cct->_conf->client_oc_max_dirty_age,
				  true);
    ObjectCacher::ObjectSet set;
    */
  }

  class LoggerTest: public Logger
  {
  protected:
    virtual int handle(bufferlist &entry)
    {
      string value;
      int r = 0;
      int count = 0;
      try
      {
        bufferlist::iterator p = entry.begin();
        ::decode(r, p);
        ::decode(count, p);
        ::decode(value, p);
      } catch (const buffer::error &err)
      {
        cout << "decode entry failed" << std::endl;
      }
      entry.clear();
      int newcount = count - 1;
      ::encode(r, entry);
      ::encode(newcount, entry);
      ::encode(value, entry);
      cout << "READ LOG(" << value << "," << r << "," << count << ") by " << this << std::endl;
      if (count == 0)
        return 0;
      else
        return r;
    }
  public:
    LoggerTest(IoCtx &ioctx)
      :Logger("test", ioctx)
    {}
    int log(int r, int count, string data)
    {
      bufferlist entry;
      ::encode(r, entry);
      ::encode(count, entry);
      ::encode(data, entry);
      cout << "WRITE LOG(" << data << "," << r << "," << count << ")" << std::endl;
      return Logger::log(entry);
    }
  };

  void main(int argc, const char *argv[])
  {
    int r;

    Rados rados;
    rados.init(NULL);
    r = rados.conf_read_file("/etc/ceph/ceph.conf");
    if (r < 0) {
      cout << "open ceph.conf failed" << std::endl;
      return;
    }
    r = rados.conf_parse_argv(argc, argv);
    if (r < 0){
      cout << "parse conf faield " << std::endl;
    }
    r = rados.connect();
    if (r < 0) {
      cout << "connect failed " << std::endl;
      return;
    }

    IoCtx ioctx;
    r = rados.ioctx_create("lfs", ioctx);
    if (r < 0) {
      cout << "open pool lfs failed " << std::endl;
      goto err_shutdown;
    }
    cout << "Begin...." << std::endl;

    test(ioctx);
    
  err_close:
    ioctx.close();
  err_shutdown:
    rados.shutdown();
  }
};

int main(int argc, const char *argv[])
{
  lightfs::main(argc, argv);
  return 0;
}
