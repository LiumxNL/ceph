if WITH_FUSE
liblightfs_la_SOURCES = \
	lightfs/lightfs.cc \
	lightfs/logger.cc

liblightfs_la_LIBADD = $(LIBRADOS) libcls_lightfs_client.la -lfuse
noinst_LTLIBRARIES += liblightfs.la

noinst_HEADERS += \
	lightfs/lightfs.hpp \
	lightfs/logger.hpp

lightfs_test_cls_SOURCES = lightfs/test_cls.cc
lightfs_test_cls_LDADD = liblightfs.la
bin_DEBUGPROGRAMS += lightfs_test_cls

endif
