#include <stdio.h>
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
    {
      LoggerTest test(ioctx);
      LoggerTest test1(ioctx);
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
      r = test1.open();
      if (r < 0) {
        cout << "open Logger test failed ret: " << r << std::endl;
        goto err_close_test;
      }

      cout << "DO LOG...." << std::endl;
      for (int i = 0; i < 100; ++i) {
        int err = rand() & 0x3;
        switch(err)
        {
          case 1:
            err = -EBUSY;
            break;
          case 2:
            err = -EINVAL;
            break;
          case 3:
            err = -EAGAIN;
            break;
        };
        {
          Logger::Transaction trans(test);

          test.log(err, rand() & 0x3, "S0");
          test.log(err, rand() & 0x3, "S1");
          test.log(err, rand() & 0x3, "S2");
          test.log(err, rand() & 0x3, "S3");
          test.log(err, rand() & 0x3, "S4");

          test.flush();
        }
        test.flush();
        
        sleep(5);
      }

      cout << "Wait....." << std::endl;

      getchar();

      cout << "Before ending..." << std::endl;
      test1.close();
  err_close_test:
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
