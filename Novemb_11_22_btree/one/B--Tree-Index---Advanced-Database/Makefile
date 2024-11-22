CC = gcc
CFLAGS  = -g -Wall -Wno-unused-value
# -w <-original and  -g opt should be removed 

test1: main.o bpt.o
	$(CC) $(CFLAGS) -o test1 main.o bpt.o


	
main.o: main.c bpt.h
	$(CC) $(CFLAGS) -c main.c -lm


bpt.o: bpt.c bpt.h
	$(CC) $(CFLAGS) -c bpt.c
	

clean: 
	$(RM) test1 *.o *~

run:
	./test1
