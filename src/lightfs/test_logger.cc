#include <iostream>
#include <string>
#include <string.h>

#include "logger.hpp"

#include "common/ceph_context.h"

#include "include/rados/librados.hpp"

#define dout_subsys ceph_subsys_client

using namespace std;
using namespace librados;

namespace lightfs
{

  Mutex mutex("LoggerTest");

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
      cout << "LOG(" << value << "," << r << "," << count << ")" << std::endl;
      entry.clear();
      int newcount = count - 1;
      ::encode(r, entry);
      ::encode(newcount, entry);
      ::encode(value, entry);
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
    {
      LoggerTest test(ioctx);
      if (argc == 2 && strcmp(argv[1], "init") == 0) {
        test.init_pool(2);
        cout << "Init done." << std::endl;
        goto err_close;
      }
      r = test.open();
      if (r < 0) {
        cout << "open Logger test failed ret: " << r << std::endl;
        goto err_close;
      }

      cout << "DO LOG...." << std::endl;
      test.log(0, 10, "OK0");
      test.log(0, 10, "OK1");
      test.log(0, 10, "OK2");
      test.log(0, 10, "OK3");
      test.log(0, 10, "OK4");

      cout << "DO FLUSH..." << std::endl;
      test.flush();
      cout << "Wait....." << std::endl;
      sleep(99999999);

    err_close_logger:
      test.close();
    }
    
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
