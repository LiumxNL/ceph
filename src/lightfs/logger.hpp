#ifndef __LOGGER_HEADER__
#define __LOGGER_HEADER__

#include <string>

#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "common/Timer.h"

#include "include/rados/librados.hpp"
#include "include/lightfs_types.hpp"

namespace lightfs
{
  class C_Logger_Flusher;
  class C_Logger_Cleaner;

  class Logger
  {
    friend class C_Logger_Cleaner;
    friend class C_Logger_Flusher;
  private:
    CephContext *_cct;

    const char *_prefix;
    void get_oid_global(string &oid);
    void get_oid_head(string &oid, int queue);
    void get_oid_tail(string &oid, int queue);
    void get_oid_entry(string &oid, int queue, uint64_t pos);

    librados::IoCtx _reader_ioctx;
    Mutex _reader_mutex;
    SafeTimer _reader_timer;
    int do_handle(const std::string &oid);

    librados::IoCtx _writer_ioctx;
    Mutex _writer_mutex;
    int _writer_queue;
    uint64_t _writer_pos;
    int _writer_count;
    Context * _writer_flusher;
    SafeTimer _writer_timer;
    int open_writer();
    void pend_flush(int second);
    void cancel_flush();
    void do_flush();

    int _bits;
    inline int bits_count() { return 1 << _bits; }
    inline int bits_mask() { return bits_count() - 1; }
  protected:
    int log(bufferlist &entry);
    /*
         * ret:
         * 0 on success
         * -EBUSY on retry, and entry can be changed.
         * else will be ignore
       */ 
    virtual int handle(bufferlist &entry) = 0;
  public:
    int init_pool(int bits);

    Logger(const char *prefix, librados::IoCtx &ioctx);
    virtual ~Logger();
    int open();
    void close();// Do not call it when transaction locked.

    Mutex transaction;
    void flush();

    void cleaner();
  };
};

#endif
