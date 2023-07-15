HOME = $(shell pwd)

TILESTORE = ../tilestore
BF = ../buffertile
CHK = ../tilechunk
LAM = ../linear_algebra_module

# Makefile for Executor layer
LIB	= exec
INCDIR	= -I${HOME}/include -I${TILESTORE}/include -I${BF}/include -I${CHK}/include -I${LAM}/include
SRCDIR	= ${HOME}/src
OBJDIR  = ${HOME}/obj
LIBDIR  = ${HOME}/lib
SRCS	= exec_interface.c planner.c node_interface.c array_struct.c simulate.c helper_ops.c
TESTS	= exec_pipeline_test.c exec_planner_test.c
OBJS	= ${SRCS:.c=.o}

# 3rd party libraries
OUT_INCS 	=
OUT_LIBS 	= -L${TILESTORE}/lib -L${BF}/lib -L${CHK}/lib -L${LAM}/lib -llam -lchunk -lbf -ltilestore -llapack -lopenblas -lm -lrt -lpthread 
#############################################################################
# This macro definition can be overwritten by command-line definitions.
CC	= gcc
CFLAGS	= -std=c11 -pedantic -g3 -O2
#############################################################################

all: lib${LIB}.a ${LIB}_pipeline_test ${LIB}_planner_test ${LIB}_op_func_test

# create test executable(s0)
${LIB}_pipeline_test: lib${LIB}.a
	$(CC) $(CFLAGS) -o $@ ${LIB}_pipeline_test.c ${LIBDIR}/lib${LIB}.a ${OUT_LIBS} $(INCDIR) ${OUT_INCS}

${LIB}_planner_test: lib${LIB}.a
	$(CC) $(CFLAGS) -o $@ ${LIB}_planner_test.c ${LIBDIR}/lib${LIB}.a ${OUT_LIBS} $(INCDIR) ${OUT_INCS}

${LIB}_op_func_test: lib${LIB}.a
	$(CC) $(CFLAGS) -o $@ op_func_test.c ${LIBDIR}/lib${LIB}.a ${OUT_LIBS} $(INCDIR) ${OUT_INCS}

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
	rm -rf ${OBJDIR} ${LIBDIR} *_test  __temparr_*.tilestore /dev/shm/buffertile
