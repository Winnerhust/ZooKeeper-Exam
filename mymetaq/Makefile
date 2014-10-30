CC=gcc
HOME=/home/chenxueyou
CFLAGS=-g 
ZOOKEEPER_INC=-I${HOME}/zookeeper/include/zookeeper
ZOOKEEPER_LIB= -L${HOME}/zookeeper/lib -lzookeeper_mt

APP=producer consumer
all:
	${CC} producer.c -DTHREAD ${CFLAGS} ${ZOOKEEPER_INC} ${ZOOKEEPER_LIB} -o producer
	${CC} consumer.c -DTHREAD ${CFLAGS} ${ZOOKEEPER_INC} ${ZOOKEEPER_LIB} -o consumer
clean:
	rm -f ${APP}
