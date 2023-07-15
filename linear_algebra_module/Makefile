HOME = $(shell pwd)

TILESTORE = ../tilestore
BF = ../buffertile
CHK = ../tilechunk
EXEC = ../lam_executor

# Makefile for LAM layer
LIB							= lam
INCDIR						= -I${HOME}/include -I${BF}/include -I${CHK}/include -I${EXEC}/include -I${TILESTORE}/include
INTERFACE_SRCDIR			= ${HOME}/src/interface
DINTERNAL_SRCDIR 			= ${HOME}/src/internals/dense
DINTERNAL_AGGR_SRCDIR 		= ${HOME}/src/internals/dense/dense_aggr
SINTERNAL_SRCDIR			= ${HOME}/src/internals/sparse
SINTERNAL_AGGR_SRCDIR 		= ${HOME}/src/internals/sparse/sparse_aggr
MINTERNAL_SRCDIR			= ${HOME}/src/internals/mixed
UINTERNAL_SRCDIR			= ${HOME}/src/internals
FUNC_TESTDIR 				= ${HOME}/test/func_test
PERF_TESTDIR				= ${HOME}/test/perf_test
BINDIR						= ${HOME}/bin
OBJDIR  					= ${HOME}/obj
LIBDIR  					= ${HOME}/lib

INTERFACE_SRCS	= lam_elemwise.c lam_matmul.c lam_transpose.c lam_aggr.c \
	lam_window.c lam_retile.c lam_newarr.c
INTERFACE_OBJS	= ${INTERFACE_SRCS:.c=.o}

DINTERNAL_SRCS = window_operations.c dense_elemwise_ops.c dense_matmul.c
DINTERNAL_OBJS = ${DINTERNAL_SRCS:.c=.o} 

DINTERNAL_AGGRS_SRCS = count.c sum.c avg.c max.c min.c var.c stdev.c norm.c
DINTERNAL_AGGRS_OBJS = ${DINTERNAL_AGGRS_SRCS:.c=.o}

SINTERNAL_SRCS = sparse_elemwise_ops.c sparse_matmul.c
SINTERNAL_OBJS = ${SINTERNAL_SRCS:.c=.o}

SINTERNAL_AGGRS_SRCS = sp_norm.c sp_sum.c
SINTERNAL_AGGRS_OBJS = ${SINTERNAL_AGGRS_SRCS:.c=.o}

MINTERNAL_SRCS = mixed_elemwise_ops.c mixed_matmul.c
MINTERNAL_OBJS = ${MINTERNAL_SRCS:.c=.o} 

UINTERNAL_SRCS = lam_util.c
UINTERNAL_OBJS = ${UINTERNAL_SRCS:.c=.o}

# 3rd party libraries
OUT_INCS 	=
OUT_LIBS 	= -L${TILESTORE}/lib -L${BF}/lib -L${CHK}/lib -lchunk -lbf -ltilestore -lm -lrt -lpthread -lopenblas
#############################################################################
# This macro definition can be overwritten by command-line definitions.
CC	= gcc
CFLAGS	= -std=c11 -pedantic -Wno-pointer-arith -g3 -O2
#############################################################################


all: lib${LIB}.a 

# make a lib
lib${LIB}.a: $(INTERFACE_OBJS) ${DINTERNAL_OBJS} ${DINTERNAL_AGGRS_OBJS} ${SINTERNAL_OBJS} ${SINTERNAL_AGGRS_OBJS} ${MINTERNAL_OBJS} ${UINTERNAL_OBJS}
	cd ${OBJDIR}; \
	ar cr lib${LIB}.a $^; \
	ranlib lib${LIB}.a
	mkdir -p ${LIBDIR}
	mv ${OBJDIR}/lib${LIB}.a ${LIBDIR}

# build src/*.c to objs/*.o
$(INTERFACE_OBJS): ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${INTERFACE_SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ $(INCDIR) ${OUT_INCS}
${DINTERNAL_OBJS}: ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${DINTERNAL_SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ $(INCDIR) ${OUT_INCS}
${DINTERNAL_AGGRS_OBJS}: ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${DINTERNAL_AGGR_SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ $(INCDIR) ${OUT_INCS}
${SINTERNAL_OBJS}: ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${SINTERNAL_SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ $(INCDIR) ${OUT_INCS}
${SINTERNAL_AGGRS_OBJS}: ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${SINTERNAL_AGGR_SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ $(INCDIR) ${OUT_INCS}
${MINTERNAL_OBJS}: ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${MINTERNAL_SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ $(INCDIR) ${OUT_INCS}
${UINTERNAL_OBJS}: ${INCS}
	mkdir -p ${OBJDIR}
	$(CC) $(CFLAGS) -c ${UINTERNAL_SRCDIR}/${@:.o=.c} -o ${OBJDIR}/$@ $(INCDIR) ${OUT_INCS}

# cleanup
clean:
	rm -rf ${OBJDIR} ${LIBDIR} ${BINDIR}  __* 
