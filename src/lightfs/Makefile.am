
lfs_SOURCES = \
	lightfs/lightfs.cc \
	lightfs/test_lfs.cc
	
lfs_LDADD = $(LIBRADOS) 
lfs_CFLAGS = $(AM_CFLAGS)
lfs_CXXFLAGS = $(AM_CXXFLAGS)
bin_PROGRAMS += lfs #ceph light filesystem client

include_HEADERS = \
	lightfs/lightfs.hpp
