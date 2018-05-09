LIB_DIR=..
INCLUDE_DIR=../../include

all:
	g++ -o benchmark -std=c++11 -Wno-deprecated-declarations benchmark.cpp -I$(INCLUDE_DIR) -L$(LIB_DIR) -luv -ldse
