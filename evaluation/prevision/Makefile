HOME = $(shell pwd)

TILESTORE = ../../tilestore
BF = ../../buffertile
CHK = ../../tilechunk
LAM = ../../linear_algebra_module
LAE = ../../lam_executor

# Makefile for Executor layer
LIB	= exec
INCDIR	= -I${HOME}/include -I${TILESTORE}/include -I${BF}/include -I${CHK}/include -I${LAM}/include -I${LAE}/include
# SRCDIR	= ${HOME}/src
# OBJDIR  = ${HOME}/obj
# LIBDIR  = ${HOME}/lib
# SRCS	= exec_interface.c planner.c node_interface.c array_struct.c
# TESTS	= exec_pipeline_test.c exec_planner_test.c exec_eval.c
OBJS	= ${SRCS:.c=.o}

# 3rd party libraries
OUT_INCS 	=
OUT_LIBS 	= -L${TILESTORE}/lib -L${BF}/lib -L${CHK}/lib -L${LAM}/lib -L${LAE}/lib -lexec -llam -lchunk -lbf -ltilestore -lm -lrt -lpthread -lopenblas 
#############################################################################
# This macro definition can be overwritten by command-line definitions.
CC	= gcc
CFLAGS	= -std=c11 -pedantic -g3 -O2
#############################################################################

all: ${LIB}_eval

# create test executable(s0)
${LIB}_eval: 
	$(CC) $(CFLAGS) -o $@ ${LIB}_eval.c ${OUT_LIBS} $(INCDIR) ${OUT_INCS}

# cleanup
clean:
	rm -rf ${OBJDIR} ${LIBDIR} *_test __[a-zA-Z]* exec_eval
