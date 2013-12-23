YACC_DEBUG=--verbose --debug
LEX_DEBUG=--verbose --debug
all: parser

clean:
	rm -f parser.cpp pparser tokens.cpp *.o *.hpp

parser.cpp: parser.y
	bison $(YACC_DEBUG) -d -o $@ $^
	
parser.hpp: parser.cpp

tokens.cpp: tokens.l parser.hpp
	flex $(LEX_DEBUG) -o $@ $^ 

parser.o : parser.cpp parser.hpp
	g++ -c parser.cpp
	
tokens.o : tokens.cpp parser.hpp
	g++ -c tokens.cpp
	
node.o : node.cpp node.h
	g++ -c node.cpp
	
parser: main.cpp parser.o tokens.o node.o
	g++ -o $@ $^ -DYYDEBUG=1
