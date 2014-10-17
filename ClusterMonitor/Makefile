CC=gcc
HOME=/home/chenxueyou
CFLAGS=-g 
ZOOKEEPER_INC=-I${HOME}/zookeeper/include/zookeeper
ZOOKEEPER_LIB= -L${HOME}/zookeeper/lib -lzookeeper_mt

APP=monitor
all:
	${CC} monitor.c -DTHREAD ${CFLAGS} ${ZOOKEEPER_INC} ${ZOOKEEPER_LIB} -o ${APP} 
clean:
	rm -f ${APP}
