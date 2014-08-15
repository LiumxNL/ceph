if WITH_FUSE
liblightfs_la_SOURCES = \
	lightfs/lightfs.cc \
	lightfs/logger.cc

liblightfs_la_LIBADD = $(LIBRADOS) libcls_lightfs_client.la -lfuse
noinst_LTLIBRARIES += liblightfs.la

noinst_HEADERS += \
	lightfs/lightfs.hpp \
	lightfs/logger.hpp \
	lightfs/test.hpp

lightfs_test_cls_SOURCES = lightfs/test_cls.cc
lightfs_test_cls_LDADD = liblightfs.la
bin_DEBUGPROGRAMS += lightfs_test_cls

lightfs_test_logger_SOURCES = lightfs/test_logger.cc
lightfs_test_logger_LDADD = liblightfs.la
bin_DEBUGPROGRAMS += lightfs_test_logger

lightfs_test_fs_SOURCES = lightfs/test_fs.cc
lightfs_test_fs_LDADD = liblightfs.la
bin_PROGRAMS += lightfs_test_fs

lightfs_watch_SOURCES = lightfs/watch.cc
lightfs_watch_LDADD = liblightfs.la
bin_PROGRAMS += lightfs_watch

lightfs_notify_SOURCES = lightfs/notify.cc
lightfs_notify_LDADD = liblightfs.la
bin_PROGRAMS += lightfs_notify
endif
