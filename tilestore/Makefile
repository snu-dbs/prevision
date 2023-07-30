HOME = $(shell pwd)

# Makefile for TileStore
LIB	= tilestore

# dirs
INCDIR	= ${HOME}/include
TINCDIR = ${HOME}/test
SRCDIR	= ${HOME}/src
BINDIR	= ${HOME}/bin
TESTDIR	= ${HOME}/test

OBJDIR  = ${HOME}/obj
LIBDIR  = ${HOME}/lib

# source files
SRCS	= tilestore.c hashtable.c tilestore_v3.c tilestore_v4.c
OBJS	= ${SRCS:.c=.o}

# test files
TESTS	= test.c 
TOBJS	= ${TESTS:.c=.o}		# test objects

# bin files
BSRCS	= tool.c 
BOBJS	= ${BSRCS:.c=.o}		

# 3rd party libraries
OUT_INCS 	= 
OUT_LIBS 	= -lm -lrt -lpthread # -lprofiler

#############################################################################
# This macro definition can be overwritten by command-line definitions.
CC	= gcc
CFLAGS	= -std=gnu11 -pedantic -fPIC -Wno-unused-result -g3 -O2
#############################################################################

all: lib${LIB}.a ${LIB}test ${LIB}_tool

# create tilestore inspection tool
${LIB}_tool: $(BOBJS) lib${LIB}.a 
	mkdir -p ${BINDIR}
	cd ${OBJDIR}; \
	$(CC) $(CFLAGS) -o ${BINDIR}/$@ ${BOBJS} ${LIBDIR}/lib${LIB}.a ${OUT_LIBS} -I$(INCDIR) ${OUT_INCS};

# create a test executable
${LIB}test: $(TOBJS) lib${LIB}.a 
	mkdir -p ${BINDIR}
	cd ${OBJDIR}; \
	$(CC) $(CFLAGS) -o ${BINDIR}/$@ ${TOBJS} ${LIBDIR}/lib${LIB}.a ${OUT_LIBS} -I$(INCDIR) -I$(TINCDIR) ${OUT_INCS};

# make a lib
lib${LIB}.a:$(OBJS)
	cd ${OBJDIR}; \
	ar cr lib${LIB}.a $(OBJS); \
	ranlib lib${LIB}.a
	mkdir -p ${LIBDIR}
	mv ${OBJDIR}/lib${LIB}.a ${LIBDIR}

# build src/*.c to objs/*.o
$(BOBJS): ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ -I$(INCDIR) ${OUT_INCS}

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
