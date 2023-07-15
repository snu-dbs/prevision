HOME = $(shell pwd)

TILESTORE = ../tilestore
BF = ../buffertile

# Makefile for LAM layer
LIB	= chunk
INCDIR	= -I${HOME}/include -I${BF}/include -I${TILESTORE}/include
SRCDIR	= ${HOME}/src
OBJDIR  = ${HOME}/obj
LIBDIR  = ${HOME}/lib
SRCS	= chunk_interface.c chunk_iter.c
TESTS	= chunk_test.c
OBJS	= ${SRCS:.c=.o}

# 3rd party libraries
OUT_INCS 	= -I$(TILESTORE)/include
OUT_LIBS 	= -L${TILESTORE}/lib -L${BF}/lib -lbf -ltilestore -lrt -lpthread -lopenblas -lm
#############################################################################
# This macro definition can be overwritten by command-line definitions.
CC	= gcc
CFLAGS	= -std=c11 -pedantic -g3 -O2
#############################################################################

all: lib${LIB}.a ${LIB}test

# create a test executable
${LIB}test: lib${LIB}.a
	$(CC) $(CFLAGS) -o $@ ${LIB}test.c ${LIBDIR}/lib${LIB}.a ${OUT_LIBS} $(INCDIR) ${OUT_INCS}

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
	$(CC) $(CFLAGS) -c ${SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ $(INCDIR) ${OUT_INCS}

# cleanup
clean:
	rm -rf ${OBJDIR} ${LIBDIR} ${LIB}test __*
