#include <stdlib.h>
#include <string.h>
#include <string>
#include <deque>

#include "logger.hpp"

#include "include/err.h"

#include "common/dout.h"
#include "common/Thread.h"

#include "cls/lock/cls_lock_client.h"
#include "cls/lightfs/cls_lightfs_client.h"

#define dout_subsys ceph_subsys_client

using namespace std;
using namespace librados;

namespace lightfs
{
  static CephContext *get_cct(const IoCtx &ioctx)
  {
    return (CephContext *)const_cast<IoCtx &>(ioctx).cct();
  }

  class C_Logger_Flusher : public Context
  {
  protected:
    virtual void finish(int r)
    {
      _logger.do_flush();
    }
  private:
    Logger &_logger;
  public:
    C_Logger_Flusher(Logger &logger)
      :_logger(logger)
    {}
  };

  class C_Logger_Cleaner : public Context
  {
  protected:
    virtual void finish(int r)
    {
      _logger.cleaner();
    }
  private:
    Logger &_logger;
  public:
    C_Logger_Cleaner(Logger &logger)
      :_logger(logger)
    {}
  };

  void Logger::get_oid_global(string &oid)
  {
    oid = _prefix;
  }

  void Logger::get_oid_head(string &oid, int queue)
  {
    char buf[32];
    snprintf(buf, 20, "%s.%04X.head", _prefix, queue);
    oid = buf;
  }

  void Logger::get_oid_tail(string &oid, int queue)
  {
    char buf[32];
    snprintf(buf, 20, "%s.%04X.tail", _prefix, queue);
    oid = buf;
  }

  void Logger::get_oid_entry(string &oid, int queue, uint64_t pos)
  {
    char buf[32];
    snprintf(buf, 20, "%s.%04X.%08lX", _prefix, queue, pos);
    oid = buf;
  }

  Logger::Logger(const char *prefix, IoCtx &ioctx)
    :_cct(get_cct(ioctx))
    ,_prefix(prefix)
    ,_reader_mutex("Logger::Reader")
    ,_reader_timer(_cct, _reader_mutex, false)
    ,_writer_mutex("Logger::Writer")
    ,_writer_queue(-1)
    ,_writer_pos(0)
    ,_writer_count(0)
    ,_writer_flusher(NULL)
    ,_writer_timer(_cct, _writer_mutex)
    ,_bits(0)
    ,transaction("Logger::Transaction")
  {
    _reader_ioctx.dup(ioctx);
    _writer_ioctx.dup(ioctx);
  }

  Logger::~Logger()
  {
    close();
    _reader_ioctx.close();
    _writer_ioctx.close();
  }

  int Logger::init_pool(int bits)
  {
    if (bits < 1 || bits > 4)
      return -EINVAL;

    ldout(_cct, 1) << "initialize logger ( " << _prefix << " ) pool." << dendl;
    Mutex::Locker lock(_reader_mutex);

    int r;
    for (int i = 0; i < (1 << bits); ++i) {
      string oid;

      get_oid_head(oid, i);
      r = cls_client::create_seq(&_reader_ioctx, oid, 0);
      if (r < 0)
        return r;

      get_oid_tail(oid, i);
      r = cls_client::create_seq(&_reader_ioctx, oid, 0);
      if (r < 0)
        return r;
    }

    string oid;
    get_oid_global(oid);

    r = _reader_ioctx.create(oid, false);
    if (r < 0)
      return r;

    bufferlist data;
    ::encode(bits, data);
    r = _reader_ioctx.omap_set_header(oid, data);
    if (r < 0)
      return r;

    return 0;
  }

  int Logger::open()
  {
    if (_bits)
      return 0;

    {
      Mutex::Locker lock(_reader_mutex);

      int r;
      string oid;
      get_oid_global(oid);

      bufferlist data;
      r = _reader_ioctx.omap_get_header(oid, &data);
      if (r < 0)
        return r;

      try {
        bufferlist::iterator p = data.begin();
        ::decode(_bits, p);
      } catch (const buffer::error &err) {
        return -EIO;
      }
      _reader_timer.init();
      _reader_timer.add_event_after(0, new C_Logger_Cleaner(*this));
    }

    {
      Mutex::Locker lock(_writer_mutex);
      _writer_timer.init();
    }
    return 0;
  }

  void Logger::close()
  {
    {
      Mutex::Locker lock(_reader_mutex);
      _reader_timer.shutdown();
    }
    {
      Mutex::Locker lock(_writer_mutex);
      _writer_timer.shutdown();
      _writer_flusher = NULL;
    }
    flush();
    _bits = 0;
  }

  int Logger::open_writer()
  {
    assert(_writer_queue < 0);
    int r;

    int queue = rand() & bits_mask();

    string tail_oid;
    get_oid_tail(tail_oid, queue);

    while (true) {
      uint64_t tail;
      r = cls_client::read_seq(&_writer_ioctx, tail_oid, tail);
      if (r < 0)
        return r;

      string oid;
      get_oid_entry(oid, queue, tail);

      ObjectWriteOperation op;
      op.create(true);
      utime_t duration(3600, 0);
      rados::cls::lock::lock(&op, "Logger", LOCK_EXCLUSIVE, "Writer", "", "", duration, 0);
      r = _writer_ioctx.operate(oid, &op);
      if (r < 0) {
        if (r == -EEXIST) {
          r = cls_client::write_seq(&_writer_ioctx, tail_oid, tail);
          if (r == 0 || r == -ENOEXEC)
            continue;
        }
        return r;
      }

      cls_client::write_seq(&_writer_ioctx, tail_oid, tail);

      _writer_queue = queue;
      _writer_pos = tail;
      _writer_count = 0;

      return 0;
    }
  }

  void Logger::pend_flush(int second)
  {
    cancel_flush();
    _writer_flusher = new C_Logger_Flusher(*this);
    _writer_timer.add_event_after(second, _writer_flusher);
  }

  void Logger::cancel_flush()
  {
    if (_writer_flusher) {
      _writer_timer.cancel_event(_writer_flusher);
      _writer_flusher = NULL;
    }
  }

  void Logger::do_flush()
  {
    assert(_writer_mutex.is_locked());
    if (transaction.is_locked()) {
      pend_flush(60);
      return;
    }

    cancel_flush();

    string oid;
    get_oid_entry(oid, _writer_queue, _writer_pos);

    int r = _writer_ioctx.unlock(oid, "Logger", "Writer");
    if (r < 0) {
      ldout(_cct, 10) << "Failed to unlock logger (" << oid << ") error: " << r << dendl;
    }

    _writer_queue = -1;
  }

  void Logger::flush()
  {
    Mutex::Locker lock(_writer_mutex);
    if (_writer_queue >= 0)
      do_flush();
  }

  int Logger::log(bufferlist &entry)
  {
    if (!_bits)
      return -EINVAL;

    int r;
    Mutex::Locker lock(_writer_mutex);

    while (true) {
      if (_writer_queue < 0) {
        r = open_writer();
        if (r < 0)
          return r;
      }

      string oid;
      get_oid_entry(oid, _writer_queue, _writer_pos);

      ObjectWriteOperation op;

      bufferlist cmp;
      op.cmpxattr("state", CEPH_OSD_CMPXATTR_OP_EQ, cmp);

      utime_t duration(3600, 0);
      rados::cls::lock::lock(&op, "Logger", LOCK_EXCLUSIVE, "Writer", "", "", duration, LOCK_FLAG_RENEW);

      bufferlist data;
      ::encode(entry, data);
      op.append(data);

      r = _writer_ioctx.operate(oid, &op);
      if (r < 0) {
        if (r == -EBUSY || r == -ENOENT || r == -ECANCELED) {
          _writer_queue = -1;
          cancel_flush();
          continue;
        }
        return r;
      }

      pend_flush((++_writer_count >= 1024) ? 0 : 3600);

      return 0;
    }
  }

  int Logger::do_handle(const string &oid)
  {
    bool handled = false;
    int r;
    bufferlist data;

    while (true) {
      bufferlist buf;

      r = _reader_ioctx.read(oid, buf, 65536, data.length());
      if (r < 0)
        return r;

      if (buf.length() == 0)
        break;

      data.claim_append(buf);
    }

    try {
      bufferlist::iterator p = data.begin();
      while (!p.end()) {
        bufferlist entry;
        ::decode(entry, p);

        _reader_mutex.Unlock();

        // entry can be changed by (handle)
        r = handle(entry);
        if (r == -EBUSY)
          log(entry);
        else if (r == 0)
          handled = true;

        _reader_mutex.Lock();
      }
    } catch (const buffer::error &err) {
      return -EIO;
    }

    return handled ? 0 : -EBUSY;
  }

  void Logger::cleaner()
  {
    if (!_bits)
      return;

    bool handled = false;
    Mutex::Locker lock(_reader_mutex);

    for (int queue = 0; queue < bits_count(); ++queue) {
      int r;

      string head_oid;
      uint64_t head;
      get_oid_head(head_oid, queue);
      r = cls_client::read_seq(&_reader_ioctx, head_oid, head);
      if (r < 0)
        continue;

      string tail_oid;
      uint64_t tail;
      get_oid_tail(tail_oid, queue);
      r = cls_client::read_seq(&_reader_ioctx, tail_oid, tail);
      if (r < 0)
        continue;

      if (head == tail)
        continue;

      string oid;
      get_oid_entry(oid, queue, head);
      r = _reader_ioctx.stat(oid, NULL, NULL);
      if (r < 0)
        continue;

      ObjectWriteOperation oplock;

      utime_t duration(3600, 0);
      rados::cls::lock::lock(&oplock, "Logger", LOCK_EXCLUSIVE, "Reader", "", "", duration, 0);

      bufferlist cmp;
      cmp.append("read");
      oplock.setxattr("state", cmp);

      r = _reader_ioctx.operate(oid, &oplock);
      if (r < 0)
        continue;

      r = do_handle(oid);
      if (r == 0)
        handled = true;

      ObjectWriteOperation opunlock;
      rados::cls::lock::unlock(&opunlock, "Logger", "Reader");
      opunlock.remove();
      r = _reader_ioctx.operate(oid, &opunlock);
      if (r < 0)
        continue;

      cls_client::write_seq(&_reader_ioctx, head_oid, head);
    }

    _reader_timer.add_event_after(handled ? 0 : 180, new C_Logger_Cleaner(*this));
  }
};
