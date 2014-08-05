if WITH_FUSE
liblightfs_la_SOURCES = \
	lightfs/lightfs.cc

liblightfs_la_LIBADD = $(LIBRADOS) libcls_lightfs_client.la -lfuse
noinst_LTLIBRARIES += liblightfs.la

noinst_HEADERS += \
	lightfs/lightfs.hpp

lightfs_test_SOURCES = lightfs/test_lfs.cc
lightfs_test_LDADD = liblightfs.la
bin_DEBUGPROGRAMS += lightfs_test

endif
