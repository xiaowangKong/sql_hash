sql_hash : main.o MyHash.o  GroupingBuffer.o
	g++ -o sql_hash main.o MyHash.o GroupingBuffer.o

main.o : MyHash.h timer.h 
	g++ -c main.cpp
MyHash.o : MyHash.h murmurhash.h
	g++ -c MyHash.cpp
GroupingBuffer.o : GroupingBuffer.h
	g++ -c GroupingBuffer.cpp
clean :
	rm sql_hash main.o MyHash.o  GroupingBuffer.o
