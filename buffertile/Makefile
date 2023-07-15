HOME = $(shell pwd)

TILESTORE = $(shell pwd)/../tilestore
# TILEDB_HOME=/media/samsung/mxm/TileDB-2.4.2/dist

# Makefile for BF layer
LIB	= bf

# dirs
INCDIR	= ${HOME}/include
TINCDIR = ${HOME}/test
SRCDIR	= ${HOME}/src
BINDIR	= ${HOME}/bin
TESTDIR	= ${HOME}/test

OBJDIR  = ${HOME}/obj
LIBDIR  = ${HOME}/lib

# source files
SRCS	= bf.c list.c hash.c tileio.c arraykey.c utils.c bf_malloc.c \
	bf_internal.c
OBJS	= ${SRCS:.c=.o}

# test files
TESTS	= bf_test_main.c bf_test_bf.c bf_test_dense.c  \
	bf_test_util.c bf_test_mp.c bf_test_sparse.c # bf_test_mp_child.c bf_test_perf.c 
TOBJS	= ${TESTS:.c=.o}		# test objects

# 3rd party libraries
OUT_INCS 	= -I$(TILESTORE)/include
OUT_LIBS 	= -L${TILESTORE}/lib -ltilestore -lm -lrt -lpthread # -lprofiler

#############################################################################
# This macro definition can be overwritten by command-line definitions.
CC	= gcc
CFLAGS	= -std=gnu11 -pedantic -fPIC -Wno-unused-result -g3 -O2
#############################################################################

all: lib${LIB}.a ${LIB}test

# create a test executable
${LIB}test: $(TOBJS) lib${LIB}.a 
	mkdir -p ${BINDIR}
	cd ${OBJDIR}; \
	$(CC) $(CFLAGS) -o $@ ${TOBJS} ${LIBDIR}/lib${LIB}.a ${OUT_LIBS} -I$(INCDIR) -I$(TINCDIR) ${OUT_INCS}; \
	$(CC) $(CFLAGS) -o bf_test_mp_child ${TESTDIR}/bf_test_mp_child.c ${LIBDIR}/lib${LIB}.a ${OUT_LIBS} -I$(INCDIR) ${OUT_INCS}; \
	mv $@ bf_test_mp_child ${HOME}/bin; 

# make a lib
lib${LIB}.a:$(OBJS)
	cd ${OBJDIR}; \
	ar cr lib${LIB}.a $(OBJS); \
	ranlib lib${LIB}.a
	mkdir -p ${LIBDIR}
	mv ${OBJDIR}/lib${LIB}.a ${LIBDIR}

# build src/*.c to objs/*.o
$(OBJS): ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ -I$(INCDIR) ${OUT_INCS}

# build test/*.c to objs/*.o
$(TOBJS): ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${TESTDIR}/${@:.o=.c} -o ${OBJDIR}/$@ -I$(INCDIR) ${OUT_INCS}

# cleanup
clean:
	rm -rf ${OBJDIR} ${LIBDIR} ${BINDIR}
