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

  class LoggerReader
  {
  private:
    Logger *_logger;
    int _queue;
    uint64_t _head;
    LoggerReader() {}
  public:
    ~LoggerReader()
    {
      int r;
      string oid;

      ObjectWriteOperation op;
      rados::cls::lock::unlock(&op, "LoggerReader", "LoggerCookie");
      op.remove();
      _logger->get_oid_entry(oid, _queue, _head);
      r = _logger->_reader_ioctx.operate(oid, &op);
      if (r < 0) {
        ldout(_logger->_cct, 10) << "Failed to delete logger (" << oid << ") error: " << r << dendl;
        return;
      }

      _logger->get_oid_head(oid, _queue);
      r = cls_client::write_seq(&_logger->_reader_ioctx, oid, _head);
      if (r < 0) {
        ldout(_logger->_cct, 5) << "Failed to update head (" << oid << " , " << _head << ") error: " << r << dendl;        
      }
    }
    static LoggerReader *open(Logger *logger, int queue)
    {
      int r;
      string oid;
      LoggerReader *result;

      uint64_t head;
      logger->get_oid_head(oid, queue);
      r = cls_client::read_seq(&logger->_reader_ioctx, oid, head);
      if (r < 0)
        goto err;

      uint64_t tail;
      logger->get_oid_tail(oid, queue);
      r = cls_client::read_seq(&logger->_reader_ioctx, oid, tail);
      if (r < 0)
        goto err;

      if (head == tail) {
        r = -EAGAIN;
        goto err;
      }

      logger->get_oid_entry(oid, queue, head);
      uint64_t size;
      time_t mtime;
      r = logger->_reader_ioctx.stat(oid, &size, &mtime);
      if (r < 0)
        goto err;

      r = logger->_reader_ioctx.lock_exclusive(oid, "LoggerReader", "LoggerCookie", "LoggerReader", NULL, 0);
      if (r < 0)
        goto err;

      result = new LoggerReader();
      result->_head = head;
      result->_queue = queue;
      result->_logger = logger;
      return result;

    err:
      assert(r < 0);
      return (LoggerReader *)(size_t)r;
    }
    int read(deque<bufferlist> &list)
    {
      list.clear();

      string oid;
      _logger->get_oid_entry(oid, _queue, _head);
      bufferlist data;
    again:
      uint64_t off = 0;
      bufferlist temp;
      int r = _logger->_reader_ioctx.read(oid, temp, 65536, off);
      if (r < 0)
        goto err;
      if (temp.length()) {
        off += temp.length();
        data.claim_append(temp);
        goto again;
      }

      try {
        bufferlist::iterator p = data.begin();
        while (!p.end()) {
          bufferlist entry;
          list.push_back(entry);
          ::decode(list.back(), p);
        }
      } catch (const buffer::error &err) {
        r = -EIO;
        goto err;
      }

    err:
      return r;
    }
  };

  class LoggerWriter
  {
  private:
    Logger *_logger;
    int _queue;
    string _oid;
    int _count;
    LoggerWriter() {}
  public:
    ~LoggerWriter()
    {
      int r = _logger->_writer_ioctx.unlock(_oid, "LoggerWriter", "LoggerCookie");
      if (r < 0) {
        ldout(_logger->_cct, 10) << "Failed to unlock logger (" << _oid << ") error: " << r << dendl;
      }
    }
    static LoggerWriter *open(Logger *logger, int queue)
    {
      int r;
      LoggerWriter *result;

      string oid;
      string tail_oid;
      logger->get_oid_tail(tail_oid, queue);
    again:
      uint64_t tail;
      r = cls_client::read_seq(&logger->_writer_ioctx, tail_oid, tail);
      if (r < 0)
        goto err;

      logger->get_oid_entry(oid, queue, tail);
      r = logger->_writer_ioctx.create(oid, true);
      if (r < 0) {
        if (r == -EEXIST) {
          r = cls_client::write_seq(&logger->_writer_ioctx, tail_oid, tail);
          if (r == 0 || r == -ENOEXEC)
            goto again;
        }
        goto err;
      }

      r = logger->_writer_ioctx.lock_exclusive(oid, "LoggerWriter", "LoggerCookie", "LoggerWriter", NULL, 0);
      if (r < 0)
        goto err;

      cls_client::write_seq(&logger->_writer_ioctx, tail_oid, tail);

      result = new LoggerWriter();
      result->_logger = logger;
      result->_oid = oid;
      result->_queue = queue;
      result->_count = 0;
      return result;

    err:
      assert(r < 0);
      return (LoggerWriter *)(size_t)r;
    }
    int write(bufferlist &entry)
    {
      bufferlist data;
      ::encode(entry, data);

      ++_count;
      return _logger->_writer_ioctx.append(_oid, data, data.length());
    }
    int count() { return _count; }
  };

  class C_Logger_Flusher : public Context
  {
  protected:
    virtual void finish(int r)
    {
      _logger.flush();
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
    ,_reader_last_queue(0)
    ,_reader_timer(_cct, _reader_mutex, false)
    ,_writer_mutex("Logger::Writer")
    ,_writer(NULL)
    ,_writer_flusher(NULL)
    ,_writer_timer(_cct, _writer_mutex, false)
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

  void Logger::flush()
  {
    Mutex::Locker lock_trans(transaction);
    Mutex::Locker lock(_writer_mutex);

    if (_writer_flusher) {
      _writer_timer.cancel_event(_writer_flusher);
      _writer_flusher = NULL;
    }

    if (_writer) {
      delete _writer;
      LoggerWriter *writer = LoggerWriter::open(this, rand() & bits_mask());
      if (!IS_ERR(writer))
        _writer = writer;
      else
        _writer = NULL;
    }
  }

  int Logger::log(bufferlist &entry)
  {
    if (!_bits)
      return -EINVAL;

    Mutex::Locker lock(_writer_mutex);
    if (!_writer) {
      LoggerWriter *writer = LoggerWriter::open(this, rand() & bits_mask());
      if (IS_ERR(writer))
        return PTR_ERR(writer);
      _writer = writer;
    }

    int r = _writer->write(entry);
    if (r < 0)
      return r;

    bool full = _writer->count() >= 1024;

    if (_writer_flusher)
      _writer_timer.cancel_event(_writer_flusher);
    _writer_flusher = new C_Logger_Flusher(*this);
    _writer_timer.add_event_after(full ? 0 : 3600, _writer_flusher);

    return 0;
  }

  void Logger::cleaner()
  {
    if (!_bits)
      return;

    int second = 180;
    Mutex::Locker lock(_reader_mutex);
    LoggerReader *reader = NULL;

    for (int i = _reader_last_queue + 1; i < _reader_last_queue + bits_count() + 1; ++i) {
      int queue = i & bits_mask();

      reader = LoggerReader::open(this, queue);
      if (!IS_ERR(reader)) {
        _reader_last_queue = queue;
        break;
      }
    }

    if (!IS_ERR(reader) && reader) {
      deque<bufferlist> list;
      bool handled = false;

      int r = reader->read(list); // ignore any error;
      if (r < 0)
        ldout(_cct, 5) << "Read logger entry failed (" << _reader_last_queue << ")" << dendl;

      while (!list.empty()) {
        bufferlist &entry = list.front();

        r = handle(entry);
        if (r == -EBUSY)
          log(entry);
        else if (r == 0)
          handled = true;
        else
          ldout(_cct, 5) << " handle logger entry failed." << dendl;

        list.pop_front();
      }

      if (handled)
        second = 0;
    }

    _reader_timer.add_event_after(second, new C_Logger_Cleaner(*this));
  }
};
