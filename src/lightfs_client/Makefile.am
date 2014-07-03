
ceph_lfs_SOURCES = \
	lightfs_client/lightfs.cc \
	lightfs_client/test_lightfs.cc
	
ceph_lfs_LDADD = $(LIBRADOS) $(LIBRBD)
ceph_lfs_CFLAGS = $(AM_CFLAGS)
ceph_lfs_CXXFLAGS = $(AM_CXXFLAGS)
bin_PROGRAMS += ceph-lfs #ceph light filesystem client

test_atomic_SOURCES = \
	lightfs_client/lightfs.h \
	lightfs_client/lightfs.cc \
	lightfs_client/test_atomic.cc
  	
test_atomic_LDADD = $(LIBRADOS) 
test_atomic_CFLAGS = $(AM_CFLAGS)
test_atomic_CXXFLAGS = $(AM_CXXFLAGS)
bin_PROGRAMS += test_atomic

watch_SOURCES = \
	lightfs_client/lightfs.h \
	lightfs_client/lightfs.cc \
	lightfs_client/watch.cc
  	
watch_LDADD = $(LIBRADOS) 
watch_CFLAGS = $(AM_CFLAGS)
watch_CXXFLAGS = $(AM_CXXFLAGS)
bin_PROGRAMS += watch

notify_SOURCES = \
	lightfs_client/lightfs.h \
	lightfs_client/lightfs.cc \
	lightfs_client/notify.cc
  	
notify_LDADD = $(LIBRADOS) 
notify_CFLAGS = $(AM_CFLAGS)
notify_CXXFLAGS = $(AM_CXXFLAGS)
bin_PROGRAMS += notify

buf_encode_SOURCES = \
	lightfs_client/buf_encode.cc 
  	
buf_encode_LDADD = $(LIBRADOS) 
buf_encode_CFLAGS = $(AM_CFLAGS)
buf_encode_CXXFLAGS = $(AM_CXXFLAGS)
bin_PROGRAMS += buf_encode

include_HEADERS = \
	lightfs_client/lightfs.h
